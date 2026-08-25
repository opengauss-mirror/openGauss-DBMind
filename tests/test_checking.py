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

import os

from dbmind.common.utils import checking

from .conftest import assert_raise


def test_check_path_valid():
    assert checking.check_path_valid("/home/user1")
    assert not checking.check_path_valid("/home/(2")


def test_check_ip_valid():
    assert checking.check_ip_valid("127.0.0.1")
    assert not checking.check_ip_valid("256.255.0.0")
    assert checking.check_ip_valid("0.0.0.0")
    assert checking.check_ip_valid("2001:db8:3c4d:15::")
    assert checking.check_ip_valid("ff00::")
    assert not checking.check_ip_valid("2001:db8:3c4d")


def test_check_port_valid():
    assert checking.check_port_valid("65535")
    assert not checking.check_port_valid("65536")
    assert not checking.check_port_valid("abc")


def test_check_instance_valid():
    assert checking.check_instance_valid("127.0.0.1")
    assert checking.check_instance_valid("127.0.0.1:8080")
    assert not checking.check_instance_valid("127.0.0.1:65536")


def test_existing_special_char():
    assert not checking.existing_special_char("12345")
    assert checking.existing_special_char("qwe!@#")


def test_path_type():
    path = os.path.realpath(__file__)
    assert checking.path_type(path) == os.path.realpath(__file__)


def test_http_scheme_type():
    assert checking.http_scheme_type('https') == 'https'
    assert checking.http_scheme_type('http') == 'http'
    assert_raise(Exception, checking.http_scheme_type, 'other')


def test_positive_int_type():
    assert checking.positive_int_type('100') == 100
    assert_raise(Exception, checking.positive_int_type, '-1')
    assert_raise(Exception, checking.positive_int_type, '100.0')
    assert_raise(Exception, checking.positive_int_type, 'abc')


def test_not_negative_int_type():
    assert checking.not_negative_int_type('100') == 100
    assert_raise(Exception, checking.not_negative_int_type, 'abc')
    assert_raise(Exception, checking.not_negative_int_type, '001')


def test_date_type():
    assert checking.date_type('1692000600000') == 1692000600000
    assert checking.date_type('2023-08-08 08:08:08') == 1691453288000


def test_check_datetime_legality():
    assert checking.check_datetime_legality('2023-08-08 08:08:08')
    assert not checking.check_datetime_legality('2023-13-08 01:01:01')


def test_check_timestamp_legality():
    assert checking.check_timestamp_legality('1692000600000').group() == '1692000600000'
    assert not checking.check_timestamp_legality('16920006000000')
    assert not checking.check_timestamp_legality(None)


def test_parameterchecker():
    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.UINT2, "optional": True}
    )
    def test_uint2(value):
        return True

    assert test_uint2(value=100)
    assert test_uint2(value=None)
    assert_raise(ValueError, test_uint2, value=65536)
    assert_raise(ValueError, test_uint2, value='abc')

    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.INT2, "optional": False}
    )
    def test_int2(value):
        return True

    assert test_int2(value=100)
    assert_raise(ValueError, test_int2, value=None)
    assert_raise(ValueError, test_int2, value=32768)
    assert_raise(ValueError, test_int2, value='abc')

    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.INT2, "optional": True}
    )
    def test_int2_optional(value):
        return True

    assert test_int2_optional(value=100)
    assert test_int2_optional(value=None)
    assert_raise(ValueError, test_int2_optional, value=32768)
    assert_raise(ValueError, test_int2_optional, value='abc')

    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.PINT32, "optional": False}
    )
    def test_pint32(value):
        return True

    assert test_pint32(value=100)
    assert_raise(ValueError, test_pint32, value=None)
    assert_raise(ValueError, test_pint32, value=65536)
    assert_raise(ValueError, test_pint32, value='abc')

    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.PINT32, "optional": True}
    )
    def test_pint32_optional(value):
        return True

    assert test_pint32_optional(value=100)
    assert test_pint32_optional(value=None)
    assert_raise(ValueError, test_pint32_optional, value=65536)
    assert_raise(ValueError, test_pint32_optional, value='abc')

    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.PERCENTAGE, "optional": False}
    )
    def test_percentage(value):
        return True

    assert test_percentage(value=0)
    assert test_percentage(value=0.5)
    assert test_percentage(value=1)
    assert_raise(ValueError, test_percentage, value=None)
    assert_raise(ValueError, test_percentage, value=1.1)
    assert_raise(ValueError, test_percentage, value=-0.1)
    assert_raise(ValueError, test_percentage, value='abc')

    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.BOOL, "optional": False}
    )
    def test_bool(value):
        return True

    assert test_bool(value=True)
    assert test_bool(value=False)
    assert_raise(ValueError, test_bool, value=None)
    assert_raise(ValueError, test_bool, value=1)
    assert_raise(ValueError, test_bool, value='abc')

    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.IP_WITHOUT_PORT, "optional": False}
    )
    def test_ip_without_port(value):
        return True

    assert test_ip_without_port(value='127.0.0.1')
    assert test_ip_without_port(value='0.0.0.0')
    assert_raise(ValueError, test_ip_without_port, value=None)
    assert_raise(ValueError, test_ip_without_port, value='127.0.0.1:8080')
    assert_raise(ValueError, test_ip_without_port, value='256.0.0.0')

    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.IP_WITH_PORT, "optional": False}
    )
    def test_ip_with_port(value):
        return True

    assert test_ip_with_port(value='127.0.0.1:8000')
    assert test_ip_with_port(value='0.0.0.0:8000')
    assert_raise(ValueError, test_ip_with_port, value=None)
    assert_raise(ValueError, test_ip_with_port, value='127.0.0.1')
    assert_raise(ValueError, test_ip_with_port, value='256.0.0.0:8000')

    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.INSTANCE, "optional": True}
    )
    def test_instance(value):
        return True

    assert test_instance(value='127.0.0.1:8000')
    assert test_instance(value='127.0.0.1')
    assert test_instance(value='0.0.0.0:8000')
    assert test_instance(value='0.0.0.0')
    assert test_instance(value=None)
    assert_raise(ValueError, test_instance, value='127.0.0.1:90000')
    assert_raise(ValueError, test_instance, value='256.0.0.0:8000')

    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.NAME, "optional": True}
    )
    def test_name(value):
        return True

    assert test_name(value='name1')
    assert test_name(value=None)
    assert_raise(ValueError, test_name, value='a')
    assert_raise(ValueError, test_name, value='a' * 121)

    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.DIGIT, "optional": True}
    )
    def test_digit(value):
        return True

    assert test_digit(value='1')
    assert test_digit(value='0')
    assert test_digit(value=None)
    assert_raise(ValueError, test_digit, value='-1')
    assert_raise(ValueError, test_digit, value='1.0')

    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.TIMESTAMP, "optional": True}
    )
    def test_timestamp(value):
        return True

    assert test_timestamp(value='1234567890111')
    assert test_timestamp(value=None)
    assert_raise(ValueError, test_timestamp, value='111')
    assert_raise(ValueError, test_timestamp, value='1234567890')
    assert_raise(ValueError, test_timestamp, value='a')

    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.STRING, "optional": True}
    )
    def test_string(value):
        return True

    assert test_string(value='name1')
    assert test_string(value='!@!#$#@$#@!@')
    assert test_string(value='  ')
    assert test_string(value=None)
    assert_raise(ValueError, test_string, value=123)
    assert_raise(ValueError, test_string, value=True)

    @checking.ParameterChecker.define_rules(
        value={"type": checking.ParameterChecker.TIMEZONE, "optional": True}
    )
    def test_timezone(value):
        return True

    assert test_timezone(value='UTC+8:35')
    assert test_timezone(value='UTC-8:00')
    assert test_timezone(value=None)
    assert_raise(ValueError, test_timezone, value='UTC+8:350')
    assert_raise(ValueError, test_timezone, value='UTC')
    assert_raise(ValueError, test_timezone, value='abc')
