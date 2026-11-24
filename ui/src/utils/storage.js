/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-DBMind is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

const ls = localStorage;
const ss = sessionStorage;
const db = {
  ls: {
    get (key) {
      try {
        return JSON.parse(ls.getItem(key));
      } catch (err) {
        return ls.getItem(key);
      }
    },
    set (key, value) {
      ls.setItem(key, JSON.stringify(value));
    },
    remove (key) {
      ls.removeItem(key);
    },
    clear () {
      ls.clear();
    }
  },
  ss: {
    get (key) {
      try {
        return JSON.parse(ss.getItem(key));
      } catch (err) {
        return ss.getItem(key);
      }
    },
    set (key, value) {
      ss.setItem(key, JSON.stringify(value));
    },
    remove (key) {
      ss.removeItem(key);
    },
    clear () {
      ss.clear();
    }
  }
};
export default db;