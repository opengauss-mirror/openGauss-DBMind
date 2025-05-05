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
from sqlalchemy import Column, String
from sqlalchemy.orm import declarative_base

# To storage at remote database server, mainly saving large scale data business, such as
# the result of time-series forecasting and slow query analysis.
ResultDbBase = declarative_base()


# To record dynamic config not likes static text-based file.
# The dynamic config can be modified by user frequently and fresh immediately.
class DynamicConfigCommon:
    category = Column(String, primary_key=True)
    name = Column(String, primary_key=True)
    value = Column(String, nullable=True)
    tag = Column(String, nullable=False)
    annotation = Column(String, nullable=True)

    @classmethod
    def default_values(cls):
        default = getattr(cls, '__default__', {})
        rows = []
        for category, params in default.items():
            for name, value, tag, annotation in params:
                obj = cls()
                obj.category = category
                obj.name = name
                obj.value = value
                obj.tag = tag
                obj.annotation = annotation
                rows.append(obj)
        return rows


DynamicConfigDbBase = declarative_base(
    name='DynamicConfig', cls=DynamicConfigCommon
)
