// react-app-rewired 配置，用于修复 react-scripts 5 (webpack 5) 构建报错：
// "Can't import the named export 'Children' from non EcmaScript module
//  (only default export is available)"
//
// 原因：webpack 5 将 node_modules 中的 .mjs 文件按严格 ESM 解析，
// 依赖包（如 framer-motion、@emotion 等）从 React (CommonJS) 做命名导入时，
// webpack 无法静态分析压缩版 React 的导出，导致编译失败。
// 将 .mjs 按 javascript/auto 解析可恢复 CommonJS 互操作能力。
module.exports = {
  webpack: function (config) {
    config.module.rules.push({
      test: /\.mjs$/,
      include: /node_modules/,
      type: 'javascript/auto',
    });
    return config;
  },
};
