import { formatPlanTooltip } from './SqlPlan';

test('escapes execution plan properties in HTML tooltips', () => {
  const tooltip = formatPlanTooltip({
    data: {
      '<img src=x onerror=alert(1)>': '"><svg/onload=alert(2)>',
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
});
