# Copyright (c) 2020 Huawei Technologies Co.,Ltd.
#
# openGauss is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#
#          http://license.coscl.org.cn/MulanPSL2
#
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

import array
import ctypes
import ctypes.util
import errno
import logging
import os
import queue
import select
import struct
import sys
import threading
from abc import abstractmethod

if os.sys.platform == 'win32':
    pass
else:
    import fcntl
    import termios

# Events
IN_ACCESS = 0x00000001
IN_MODIFY = 0x00000002
IN_ATTRIB = 0x00000004
IN_CLOSE_WRITE = 0x00000008
IN_CLOSE_NOWRITE = 0x00000010
IN_OPEN = 0x00000020
IN_MOVED_FROM = 0x00000040
IN_MOVED_TO = 0x00000080
IN_CREATE = 0x00000100
IN_DELETE = 0x00000200
IN_DELETE_SELF = 0x00000400
IN_MOVE_SELF = 0x00000800

WATCH_EVENTS = (IN_CREATE | IN_MODIFY | IN_OPEN)

IN_ISDIR = 0x40000000

MAX_NOTIFICATION = 1024


def get_mask_name(mask):
    event_flags = {
        0x00000001: 'IN_ACCESS',
        0x00000002: 'IN_MODIFY',
        0x00000004: 'IN_ATTRIB',
        0x00000008: 'IN_CLOSE_WRITE',
        0x00000010: 'IN_CLOSE_NOWRITE',
        0x00000020: 'IN_OPEN',
        0x00000040: 'IN_MOVED_FROM',
        0x00000080: 'IN_MOVED_TO',
        0x00000100: 'IN_CREATE',
        0x00000200: 'IN_DELETE',
        0x00000400: 'IN_DELETE_SELF',
        0x00000800: 'IN_MOVE_SELF',
    }

    stripped_mask = mask - (mask & IN_ISDIR)
    return event_flags.get(stripped_mask)


class NotImplException(Exception):
    pass


class INotify:
    def __init__(self):
        if sys.platform.startswith('freebsd'):
            libc_name = ctypes.util.find_library('inotify')
        else:
            libc_name = ctypes.util.find_library('c')

        self._libc = ctypes.CDLL(libc_name, use_errno=True)

        self._libc.inotify_init.argtypes = []
        self._libc.inotify_init.restype = ctypes.c_int
        self._libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
        self._libc.inotify_add_watch.restype = ctypes.c_int
        self._libc.inotify_rm_watch.argtypes = [ctypes.c_int, ctypes.c_int]
        self._libc.inotify_rm_watch.restype = ctypes.c_int

    @staticmethod
    def get_err():
        code = ctypes.get_errno()
        return code, os.strerror(code)

    def inotify_init(self):
        return self._libc.inotify_init()

    def inotify_add_watch(self, fd, pathname, mask):
        pathname = pathname.encode('utf8')
        pathname = ctypes.create_string_buffer(pathname)
        return self._libc.inotify_add_watch(fd, pathname, mask)

    def inotify_rm_watch(self, fd, wd):
        return self._libc.inotify_rm_watch(fd, wd)


class ProcessEvent:
    def __init__(self):
        pass

    def __call__(self, event):
        return self.process_default(event)

    @abstractmethod
    def process_default(self, event):
        raise NotImplException('Need to override this method.')


class Event:
    def __init__(self, **kwargs):
        self.wd = kwargs.get('wd')
        self.mask = kwargs.get('mask')
        self.name = kwargs.get('name')
        self.path = kwargs.get('path')
        self.maskname = get_mask_name(self.mask)

    @property
    def pathname(self):
        if self.name:
            return os.path.abspath(os.path.join(self.path, self.name))
        else:
            return os.path.abspath(self.path)

    @property
    def dir(self):
        return os.path.isdir(self.pathname)

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return self.__str__()


class Watcher:
    def __init__(self, wd, path, mask, proc_fun, auto_add):
        self.wd = wd
        self.path = path
        self.mask = mask
        self.proc_fun = proc_fun
        self.auto_add = auto_add
        self.dir = os.path.isdir(self.path)

    def __str__(self):
        return "{path: %s, mask: %s, wd: %s, proc_func: %s}" % (self.path, hex(self.mask),
                                                                self.wd, self.proc_fun)


def walk_dir(top, recursive, chroot_prefix):
    """To walk the directory recursively"""
    if os.path.islink(top):
        link = os.readlink(top)
        if os.path.isdir(chroot_prefix + link):
            top = chroot_prefix + link
        elif os.path.isdir(link):
            top = link
        else:
            return

    if not os.path.isdir(top):
        return

    yield top

    if not recursive:
        return

    for root, dirs, files in os.walk(top):
        for directory in dirs:
            path = os.path.join(root, directory)
            for new_dir in walk_dir(path, recursive, chroot_prefix):
                yield new_dir

        for item in files:
            path = os.path.join(root, item)
            if not os.path.islink(path):
                continue

            for new_dir in walk_dir(path, recursive, chroot_prefix):
                yield new_dir

        break


class WatchManager:
    def __init__(self):
        self.watchers = {}
        self.queues = {}
        self._inotify = INotify()
        self._fd = self._inotify.inotify_init()
        if self._fd < 0:
            err = 'Cannot initialize new instance of inotify, [%d]:%s.'
            raise OSError(err % self._inotify.get_err())

    def __del__(self):
        self.close()

    def close(self):
        self.remove_watch()
        os.close(self._fd)

    def get_fd(self):
        return self._fd

    def get_watcher(self, wd):
        return self.watchers.get(wd)

    def get_queue(self, wd):
        return self.queues.get(wd)

    def get_wds(self):
        return list(self.queues.keys())

    @staticmethod
    def __iterate_param(param):
        if isinstance(param, list) or isinstance(param, tuple):
            for p_ in param:
                yield p_
        else:
            yield param

    def add_watch(self, path, mask, proc_fun=None, recursive=False, auto_add=False,
                  blacklist=None, whitelist=None, chroot_prefix=""):
        for p in self.__iterate_param(path):
            if not isinstance(p, str):
                continue

            for subdir in walk_dir(p, recursive, chroot_prefix):
                norm_path = os.path.normpath(subdir)
                if (
                    (blacklist and any(s in norm_path for s in blacklist)) or
                    (whitelist and not any(s in norm_path for s in whitelist))
                ):
                    continue

                if auto_add and not (mask & IN_CREATE):
                    mask |= IN_CREATE

                wd = self._inotify.inotify_add_watch(self._fd, norm_path, mask)
                if wd < 0:
                    code, msg = self._inotify.get_err()
                    logging.error('[INOTIFY] add_watch: cannot watch %s WD=%d, [%d]%s.',
                                  subdir, wd, code, msg)
                else:
                    self.watchers[wd] = Watcher(wd=wd, path=norm_path, mask=mask,
                                                proc_fun=proc_fun, auto_add=auto_add)
                    self.queues[wd] = queue.Queue()
                    logging.info("[INOTIFY] successfully add %s to watches.", path)

    def __str__(self):
        ret = []
        for k, v in list(self.watchers.items()):
            ret.append(str(v))

        return "fd: %d, watchers: %s." % (self._fd, str(ret))

    def remove_watch(self):
        """inotify has a default limit 128 for watch dirs, so we have to recover
        the watch descriptor every time we stop the watcher manager.
        """
        for watch_descriptor in self.watchers:
            self._inotify.inotify_rm_watch(self._fd, watch_descriptor)


def find_mask(mask, chosen_masks):
    if chosen_masks is None:
        return mask

    for chosen_mask in chosen_masks:
        if mask & chosen_mask:
            return chosen_mask

    return 0


class Notifier:
    def __init__(self, watch_manager, default_proc_fun, chosen_masks=None):
        self._watch_manager = watch_manager
        self._fd = self._watch_manager.get_fd()
        self._epoll = select.epoll()
        self._epoll.register(self._fd, select.POLLIN)
        self._default_proc_fun = default_proc_fun
        self.chosen_masks = chosen_masks

    def check_events(self):
        try:
            events = self._epoll.poll(timeout=5)
        except IOError as err:
            if err.errno != errno.EINTR:
                raise
        else:
            if not events:
                return False

        return True

    def read_events(self):
        buff = array.array('i', [0])
        if fcntl.ioctl(self._fd, termios.FIONREAD, buff, True) == -1:
            return

        q_len = buff[0]
        if q_len < 0:
            logging.warning('[INOTIFY] event queue size is less than zero.')
            return

        r = os.read(self._fd, q_len)
        counter = 0
        events = set()
        while counter < q_len:
            s_size = 16
            wd, mask, _, c_len = struct.unpack('iIII', r[counter:counter + s_size])
            bname, = struct.unpack('%ds' % c_len, r[counter + s_size:counter + s_size + c_len])
            name = bname.rstrip(b'\0').decode("utf-8")
            events.add((wd, mask, name))
            counter += s_size + c_len

        for wd, mask, name in events:
            q = self._watch_manager.get_queue(wd)
            if q.qsize() <= MAX_NOTIFICATION:
                chosen_mask = find_mask(mask, self.chosen_masks)
                if chosen_mask:
                    event = Event(wd=wd, mask=chosen_mask, name=name)
                    q.put(event)

            else:
                logging.warning('[INOTIFY] too many events to handle. '
                                'Thus forbid to add new events. File: %s', name)

    def process_events(self):
        for wd in self._watch_manager.get_wds():
            watcher = self._watch_manager.get_watcher(wd)
            q = self._watch_manager.get_queue(wd)
            if not watcher:
                continue

            while q.qsize() > 0:
                event = q.get()
                event.path = watcher.path
                new_path = os.path.join(event.path, event.name)
                if os.path.isdir(new_path) and (event.mask & IN_CREATE) and watcher.auto_add:
                    self._watch_manager.add_watch(new_path, watcher.mask,
                                                  proc_fun=watcher.proc_fun,
                                                  recursive=False,
                                                  auto_add=True)

                self._default_proc_fun(event)

    def stop(self):
        if self._fd:
            self._epoll.unregister(self._fd)
            self._watch_manager.close()
            self._fd = None


class ThreadedNotifier(threading.Thread, Notifier):
    def __init__(self, watch_manager, default_proc_fun=None, chosen_masks=None):
        threading.Thread.__init__(self)
        self._stop_event = threading.Event()
        Notifier.__init__(self, watch_manager, default_proc_fun,
                          chosen_masks=chosen_masks)

    def stop(self):
        self._stop_event.set()
        threading.Thread.join(self)
        Notifier.stop(self)
        logging.info("[INOTIFY] stop threaded notifier")

    def loop(self):
        while not self._stop_event.isSet():
            self.process_events()
            if self.check_events():
                self.read_events()

    def run(self):
        logging.info("[INOTIFY] start threaded notifier")
        self.loop()
