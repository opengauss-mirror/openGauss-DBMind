/**
 * 指标名称映射工具
 * 处理不同操作系统（Linux vs macOS）的 node_exporter 指标名称差异
 */

// 检测是否为 macOS 环境（通过 user agent 或其他方式）
const isMacOS = () => {
  // 可以通过 API 返回的数据特征来判断，或者通过配置
  // 这里暂时使用简单的判断逻辑
  return window.navigator.platform.toLowerCase().includes('mac');
};

// Memory 指标映射表
const memoryMetricMap = {
  'node_memory_MemTotal_bytes': {
    linux: 'node_memory_MemTotal_bytes',
    macos: 'node_memory_total_bytes'
  },
  'node_memory_MemAvailable_bytes': {
    linux: 'node_memory_MemAvailable_bytes',
    macos: 'node_memory_free_bytes'
  },
  'node_memory_SwapTotal_bytes': {
    linux: 'node_memory_SwapTotal_bytes',
    macos: 'node_memory_swap_total_bytes'
  },
  'node_memory_SwapFree_bytes': {
    linux: 'node_memory_SwapFree_bytes',
    macos: 'node_memory_swap_total_bytes'
  },
  'node_memory_Buffers_bytes': {
    linux: 'node_memory_Buffers_bytes',
    macos: 'node_memory_internal_bytes'
  },
  'node_memory_Cached_bytes': {
    linux: 'node_memory_Cached_bytes',
    macos: 'node_memory_inactive_bytes'
  }
};

/**
 * 获取适合当前系统的指标名称
 * @param {string} metricName - 标准指标名称（Linux格式）
 * @returns {string} - 适合当前系统的指标名称
 */
export const getMetricName = (metricName) => {
  // 如果在映射表中找到了这个指标
  if (memoryMetricMap[metricName]) {
    return isMacOS() 
      ? memoryMetricMap[metricName].macos 
      : memoryMetricMap[metricName].linux;
  }
  
  // 如果不在映射表中，直接返回原始名称
  return metricName;
};

/**
 * 批量转换指标名称数组
 * @param {Array<string>} metrics - 指标名称数组
 * @returns {Array<string>} - 转换后的指标名称数组
 */
export const mapMetricNames = (metrics) => {
  return metrics.map(metric => getMetricName(metric));
};

// 导出映射表供其他模块使用
export const METRIC_MAPPINGS = memoryMetricMap;
export const IS_MACOS = isMacOS();