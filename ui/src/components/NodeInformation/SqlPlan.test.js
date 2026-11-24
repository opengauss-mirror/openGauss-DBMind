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

import { formatPlanTooltip } from './SqlPlan';

test('escapes execution plan properties in HTML tooltips', () => {
  const tooltip = formatPlanTooltip({
    data: {
      '<img src=x onerror=alert(1)>': '"><svg/onload=alert(2)>',
      detail: 'a'.repeat(31),
      x: 10,
      y: 20,
      id: 'node-id',
      key: 1,
    },
  });

  expect(tooltip).toContain('&lt;img src=x onerror=alert(1)&gt;');
  expect(tooltip).toContain('&quot;&gt;&lt;svg/onload=alert(2)&gt;');
  expect(tooltip).not.toContain('<img');
  expect(tooltip).not.toContain('<svg');
  expect(tooltip).not.toContain('node-id');
  expect(tooltip).toContain(`${'a'.repeat(30)}<br>a`);
});
