# Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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

"""The error codes and corresponding error messages.
"""

WRONG_USER_INFO = "Wrong username or password."
MISSING_VALUE = "Username or password cannot be empty."
WRONG_USER_ROLE = "Illegal user role."
PASSWORD_NOT_CHANGED = "Initial password is forbidden."
CONNECTION_ERROR = "Unable to connect to DBMind."
METHOD_NOT_ALLOWED = "V1 API is not allowed in distribute mode."
NOT_AUTHENTICATED = "Not authenticated or token is expired."
LOCK_UP = "The account has been locked."

ERROR_CODE = {
    WRONG_USER_INFO: 1,
    MISSING_VALUE: 2,
    WRONG_USER_ROLE: 3,
    PASSWORD_NOT_CHANGED: 4,
    CONNECTION_ERROR: 5,
    METHOD_NOT_ALLOWED: 6,
    NOT_AUTHENTICATED: 7,
    LOCK_UP: 8
}
