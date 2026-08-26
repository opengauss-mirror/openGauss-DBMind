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

import logging
import os
import queue
import re
import shlex
import shutil
import subprocess
import threading
import time

from . import inotify

SCRIPT_PATH = os.path.split(os.path.realpath(__file__))[0]
LOG_AGENT_TMPDIR = os.path.join(SCRIPT_PATH, "tmpdir")
ILDE_TIMEOUT = 600
CHOSEN_MASKS = [inotify.IN_ACCESS, inotify.IN_MODIFY, inotify.IN_OPEN, inotify.IN_CREATE]


class Event(object):
    def __init__(self, body=None, header=None):
        self.body = body
        self.header = header

    def __str__(self):
        return 'path: %s, size: %s' % (self.header.get("path", ""), self.header.get("size", ""))


def get_file_stat(filepath):
    _stat = None
    try:
        if os.path.isfile(filepath):
            _stat = os.stat(filepath)
    except Exception as e:
        logging.warning("[TAILDIR] get_file_stat get path: %s Exception: %s", filepath, str(e))
        _stat = None
    finally:
        return _stat


def get_file_size(filepath):
    file_stat = get_file_stat(filepath)
    if file_stat:
        return file_stat.st_size
    return None


def get_last_updated(filepath):
    file_stat = get_file_stat(filepath)
    if file_stat:
        return file_stat.st_mtime
    return None


def get_inode(filepath):
    file_stat = get_file_stat(filepath)
    if file_stat:
        return file_stat.st_ino
    return None


def get_last_line(filepath, file_size):
    with open(filepath, "r", errors="ignore") as f:
        offset = 8
        while offset < file_size:
            f.seek(file_size - offset)
            content = f.read(offset)
            if not content:
                return '\n'

            lines = content.split('\n')
            if not lines:
                return '\n'

            if lines[-1] == '':
                lines.pop()

            if len(lines) >= 2:
                return lines[-1] + '\n'
            else:
                offset *= 2

        f.seek(0)
        lines = f.readlines()
        if not lines:
            return '\n'

        return lines[-1]


class TailFile(object):
    def __init__(self, filepath, pos=0, mask=inotify.IN_ACCESS, tmp_gz_file_flg=False):
        self.path = filepath
        self.pos = pos
        self.mask = mask
        self.tmp_gz_file_flg = tmp_gz_file_flg

        self.fd = self.open_fd(self.path)
        self.update_pos(self.pos)
        self.inode = get_inode(filepath)
        self.create_time = int(time.monotonic())
        self.consume_event_complete = False

    def read(self):
        body = self.fd.read()
        self.update_pos(self.fd.tell())
        return body

    def update_pos(self, pos):
        self.pos = pos
        if self.fd is not None:
            self.fd.seek(pos)

    def to_dict(self):
        return {'file': self.path, 'pos': self.pos, 'mask': self.mask}

    @staticmethod
    def open_fd(filepath):
        try:
            fd = open(filepath, errors="ignore")
        except Exception as e:
            logging.warning("[TAILDIR] tailfile_init open path: %s error: %s", filepath, str(e))
            fd = None
        return fd

    def __del__(self):
        if self.fd:
            self.fd.close()
        if self.tmp_gz_file_flg and os.path.isfile(self.path):
            os.remove(self.path)


class FileMatcher(object):
    def __init__(self, match_paths=(), exclude_file=None):
        self.filename_pattern = re.compile(r'^.*?log$')  # regex for log file name
        self.paths = match_paths
        self.exclude_file = list() if exclude_file is None else exclude_file

    def match(self, path, filename):
        if not self.filename_pattern.match(filename):
            return False

        if any(m in filename for m in self.exclude_file):
            return False

        if self.paths and path not in self.paths:
            return False

        return True


class EventHandler(inotify.ProcessEvent):
    def __init__(self, matcher, q):
        super().__init__()
        self.matcher = matcher
        self.q = q

    def process_default(self, event):
        if not isinstance(event, inotify.Event):
            return
        try:
            if event.dir:
                return

            path = event.path
            name = event.name
            pathname = event.pathname
            mask = event.mask

            if self.matcher.match(path, name):
                if os.path.isfile(pathname):
                    header = dict(path=pathname,
                                  ts=int(time.time() * 1000),
                                  type=mask)
                    self.q.put(header)
        except Exception as e:
            logging.warning("[TAILDIR] process_default process inotify event error: %s", str(e))


class TaildirSource:
    def __init__(self, matcher="", watch_paths=None, blacklist=None, whitelist=None,
                 chroot_prefix=""):
        self.matcher = matcher
        self.watch_paths = list() if watch_paths is None else watch_paths
        self.blacklist = blacklist
        self.whitelist = whitelist
        self.chroot_prefix = chroot_prefix

        self.tmpdir = LOG_AGENT_TMPDIR
        self.clear_tmpdir(self.tmpdir)
        self.create_tmpdir(self.tmpdir)
        self.events = queue.Queue()
        self.channel = queue.Queue()
        self.tail_files = dict()

        self.running = False
        self.threads = []
        self.init_threads()

        # inotify
        self.notifier = None
        self.watch_manager = None
        self.register_monitor()

    def init_threads(self):
        idle_thread = threading.Thread(name='idle checker',
                                       target=self.idle_file_checker)
        self.threads.append(idle_thread)
        custom_thread = threading.Thread(name='consumer_modified',
                                         target=self.consumer_event)
        self.threads.append(custom_thread)

    def start(self):
        self.running = True
        for t in self.threads:
            t.setDaemon(True)
            t.start()
        self.notifier.start()

    def stop(self):
        self.running = False
        for t in self.threads:
            t.join(timeout=1)
        self.notifier.stop()

    def clear(self):
        while self.events.qsize():
            self.events.get()

    def idle_file_checker(self):
        need_pop_files = set()
        while self.running:
            need_pop_files.clear()
            for filepath in list(self.tail_files.keys()):
                try:
                    if not os.path.isfile(filepath):
                        need_pop_files.add(filepath)
                        continue

                    tf = self.tail_files.get(filepath)
                    mtime = get_last_updated(filepath)
                    last_size = get_file_size(filepath)
                    inode = get_inode(filepath)
                    if not mtime or not last_size or not inode:
                        need_pop_files.add(filepath)
                        continue

                    if tf.inode != inode:  # the tail file was replaced
                        need_pop_files.add(filepath)
                        continue

                    interval_modify = int(time.time()) - mtime
                    interval_create = int(time.monotonic()) - tf.create_time

                    # This tail_file has had no modifications since creation, so we need to pop it to release space.
                    if interval_modify > ILDE_TIMEOUT and interval_create > ILDE_TIMEOUT:
                        logging.info("[TAILDIR] [NEED POP]: file: %s,mask: %d, interval_now_and_mtime: %d, "
                                     "interval_now_and_create: %d, tf.pos: %d, last_size: %d",
                                     filepath, self.tail_files[filepath].mask, interval_modify,
                                     interval_create, tf.pos, last_size)
                        need_pop_files.add(filepath)

                except Exception as e:
                    logging.warning("[TAILDIR] search_need_pop_files exception: [filepath]: %s, [Exception]: %s",
                                    filepath, str(e))
                    need_pop_files.add(filepath)

            for k in need_pop_files:
                try:
                    logging.info("[TAILDIR] [POP] file: %s,mask: %d", k, self.tail_files[k].mask)
                    self.tail_files.pop(k)
                except Exception as e:
                    logging.warning("[TAILDIR] idle_file_checker exception: %s", str(e))

            time.sleep(3)

        logging.info("[TAILDIR] idle file checker stopped.")

    def consumer_event(self):
        while self.running:
            try:
                time.sleep(0.01)
                try:
                    header = self.events.get_nowait()
                except queue.Empty:
                    time.sleep(1)
                    continue

                if not header:
                    time.sleep(1)
                    continue

                filepath = header.get('path')
                mask = header.get('type')
                if not filepath:
                    continue

                filepath, tmp_gz_file_flg, run_flg = self.handle_file_to_tmpdir(filepath)
                if not run_flg:
                    continue

                if self.blacklist and any(s in filepath for s in self.blacklist):
                    continue

                if filepath not in self.tail_files:
                    last_size = get_file_size(filepath)
                    last_line = get_last_line(filepath, last_size)
                    tail_file = TailFile(filepath=filepath, pos=last_size, mask=mask,
                                         tmp_gz_file_flg=tmp_gz_file_flg)
                    if tail_file.fd is not None:
                        self.tail_files[filepath] = tail_file
                        logging.info(
                            "[TAILDIR] create tailfile: filepath: %s, last_size: %d, mask: %s, %s",
                            filepath, last_size, hex(mask), "tmp_gz_file" if tmp_gz_file_flg else "raw_file"
                        )

                        if (not tmp_gz_file_flg) and last_size > 0:
                            self.channel.put(Event(header=header, body=last_line))
                            continue

                if mask not in [inotify.IN_ACCESS, inotify.IN_OPEN]:
                    self.tail_files[filepath].mask = mask
                    self.channel.put(Event(header=header))

            except Exception as e:
                logging.warning('[TAILDIR] consumer_event error Exception: %s', str(e))
                time.sleep(0.5)

        logging.info("[TAILDIR] consumer event stopped.")

    def register_monitor(self):
        self.watch_manager = inotify.WatchManager()
        self.watch_manager.add_watch(self.watch_paths, inotify.WATCH_EVENTS,
                                     recursive=True, auto_add=True,
                                     blacklist=self.blacklist, whitelist=self.whitelist,
                                     chroot_prefix=self.chroot_prefix)

        handler = EventHandler(self.matcher, self.events)
        self.notifier = inotify.ThreadedNotifier(self.watch_manager, handler, chosen_masks=CHOSEN_MASKS)
        logging.info("[TAILDIR] register_monitor init notifier success.")

    def handle_file_to_tmpdir(self, filepath):
        run_flg = True
        tmp_gz_file_flg = False
        gz_file = "%s.gz" % filepath
        cur_gz_file = filepath.replace("-current.log", ".log.gz")
        if not os.path.isfile(filepath):
            if os.path.isfile(gz_file) or os.path.isfile(cur_gz_file):
                gz_file = gz_file if os.path.isfile(gz_file) else cur_gz_file
                filepath, tmp_gz_file_flg = self.gunzip_file_to_tmpdir(filepath,
                                                                       gz_file, self.tmpdir)
                run_flg = True
            else:
                run_flg = False
                logging.warning("[TAILDIR] handle_file_to_tmpdir: the filepath: %s doesn't exist, "
                                "and its gz file %s doesn't exist either.",
                                filepath, gz_file if not os.path.isfile(gz_file) else cur_gz_file)
        return filepath, tmp_gz_file_flg, run_flg

    def gunzip_file_to_tmpdir(self, filepath, gz_filepath, tmpdir):
        tmpfile = ""
        tmp_gz_file_flg = False
        try:
            self.create_tmpdir(tmpdir)
            tmpfilename = "tmp_%s" % os.path.basename(filepath)
            tmpfile = os.path.join(tmpdir, tmpfilename)
            if (
                not os.path.isfile(filepath) and
                os.path.isfile(gz_filepath) and
                not os.path.isfile(tmpfile)
            ):
                cmd = "gunzip -c %s > %s" % (shlex.quote(gz_filepath), shlex.quote(tmpfile))
                (status, output) = subprocess.getstatusoutput(cmd)
                if status != 0:
                    logging.warning('[TAILDIR] gunzip_file_to_tmpdir status is not 0, output is: %s', str(output))

            if os.path.isfile(tmpfile):
                tmp_gz_file_flg = True
        except Exception as e:
            logging.warning('[TAILDIR] gunzip_file_to_tmpdir, Exception: %s', str(e))
        finally:
            return tmpfile, tmp_gz_file_flg

    @staticmethod
    def create_tmpdir(tmpdir):
        try:
            if not os.path.exists(tmpdir):
                os.mkdir(tmpdir, 0o700)
                logging.info('[TAILDIR] create_tmpdir success: %s', tmpdir)
        except Exception as e:
            logging.warning('[TAILDIR] create_tmpdir, Exception: %s', str(e))

    @staticmethod
    def clear_tmpdir(tmpdir):
        try:
            if os.path.isdir(tmpdir):
                shutil.rmtree(tmpdir)
                logging.info('[TAILDIR] clear_tmpdir success: %s', tmpdir)
        except Exception as e:
            logging.warning('[TAILDIR] clear_tmpdir:%s, Exception: %s', tmpdir, str(e))

    def restart_watcher(self):
        """To periodically restart the watch manager."""
        self.watch_manager.remove_watch()
        self.watch_manager.add_watch(self.watch_paths, inotify.WATCH_EVENTS, recursive=True,
                                     blacklist=self.blacklist, whitelist=self.whitelist,
                                     chroot_prefix=self.chroot_prefix)
        logging.info("[TAILDIR] restart_watcher restart watcher success.")
