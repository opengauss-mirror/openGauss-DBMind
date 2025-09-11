import React from 'react';
import ReactDOM from 'react-dom';
// 全局样式仅入口导入一次（减少重复注入与闪烁）
import 'antd/dist/antd.css';
import './assets/css/main/index.css';
import './setupAntd';
import App from './App.js';

ReactDOM.render(
  <App />,
  document.getElementById('root')
);
