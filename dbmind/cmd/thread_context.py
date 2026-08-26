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

import contextvars


class ContextProxy:
    def __init__(self, name, default=None):
        self._name = name
        self._var = contextvars.ContextVar(name, default=default)

    def get(self):
        return self._var.get()

    def set(self, value):
        return self._var.set(value)

    def reset(self, token):
        self._var.reset(token)

    def __getattr__(self, name):
        return getattr(self.get(), name)

    def __setattr__(self, name, value):
        if name in ("_var", "_name"):
            super().__setattr__(name, value)
        else:
            raise AttributeError(f"Cannot set attribute '{name}' directly.")

    def __repr__(self):
        return f"{self._var.__repr__()}: {self._name}"

    def __iter__(self):
        return iter(self._var.get().agents().items())
