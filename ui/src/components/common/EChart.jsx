import React from 'react';
import ReactEcharts from 'echarts-for-react';

// 轻量包装：统一默认参数与样式；保持与原用法兼容（透传其余 props）
const EChart = React.forwardRef(function EChart(
  {
    option,
    style,
    notMerge = true,
    lazyUpdate = true,
    // 默认启用 canvas 与 useDirtyRect 以减少重绘
    opts = { renderer: 'canvas', useDirtyRect: true },
    ...rest
  },
  ref
) {
  // 若调用方未显式提供 `height`，但提供了 `minHeight`，则不强制设置默认高度，交由外层容器控制
  const base = { width: '100%' };
  const mergedStyle =
    style && typeof style.height === 'undefined' && typeof style.minHeight !== 'undefined'
      ? { ...base, ...style }
      : { ...base, height: 90, ...style };
  return (
    <ReactEcharts
      ref={ref}
      option={option}
      notMerge={notMerge}
      lazyUpdate={lazyUpdate}
      // 用户传入的 opts 覆盖默认值（保留向后兼容）
      opts={{ renderer: 'canvas', useDirtyRect: true, ...opts }}
      style={mergedStyle}
      {...rest}
    />
  );
});

export default EChart;
