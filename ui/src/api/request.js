import axios from 'axios'
import Qs from 'qs'
import history from '../utils/history';
import db from '../utils/storage'
import { message } from 'antd';

// 配置参数
const CONFIG = {
  timeout: process.env.REACT_APP_API_TIMEOUT || 15000, // 15秒超时
  retryCount: 3,
  retryDelay: 1000
};

const targetUrl = window.location.origin
if (process.env.NODE_ENV === 'development') {
  axios.defaults.baseURL = targetUrl + '/transpond'
} else {
  axios.defaults.baseURL = targetUrl + '/v1/api'
}

axios.defaults.timeout = CONFIG.timeout;
axios.defaults.headers.post['Content-Type'] = 'application/x-www-form-urlencoded;charset=UTF-8';
axios.defaults.headers.common['X-Requested-With'] = 'XMLHttpRequest';

// 全局控制：避免会话过期时重复弹错、重复跳转
let isRedirectingToLogin = false;
let lastAuthErrorTs = 0;

// 请求拦截器
axios.interceptors.request.use(config => {
  // 添加认证token
  const token = db.ss.get('access_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  
  // 开发环境下打印请求信息
  if (process.env.NODE_ENV === 'development') {
    // eslint-disable-next-line no-console
    console.log(`[API] ${config.method?.toUpperCase()} ${config.url}`, config.data || config.params);
  }
  
  // 添加请求时间戳
  config.metadata = { startTime: new Date() };
  
  return config;
}, error => {
  // eslint-disable-next-line no-console
  console.error('[API] 请求配置错误:', error);
  message.error('请求配置错误');
  return Promise.reject(error);
});

// 响应拦截器
axios.interceptors.response.use(
  response => {
    // 计算请求耗时
    const endTime = new Date();
    const duration = endTime.getTime() - response.config.metadata.startTime.getTime();
    
    if (process.env.NODE_ENV === 'development') {
      // eslint-disable-next-line no-console
      console.log(`[API] 响应成功 ${response.config.method?.toUpperCase()} ${response.config.url} (${duration}ms)`);
    }
    
    if (response.status === 200) {
      return Promise.resolve(response);
    } else {
      return Promise.reject(response);
    }
  },
  error => {
    // 检查是否应该跳过错误消息（用于登录等特殊请求）
    const skipErrorMessage = error.config?.skipErrorMessage;
    
    // 网络错误处理
    if (!error.response) {
      if (!skipErrorMessage) {
        message.error('网络连接失败，请检查网络状态');
      }
      return Promise.reject(error);
    }
    
    const { status, data } = error.response;
    
    // 如果标记了跳过错误消息，直接返回
    if (skipErrorMessage) {
      return Promise.reject(error.response);
    }
    
    // 根据状态码处理不同错误
    switch (status) {
      case 401:
      case 403: {
        // 登录页不提示；其余页面只提示一次并跳转一次
        if (window.location.pathname !== '/login') {
          const now = Date.now();
          if (!isRedirectingToLogin && now - lastAuthErrorTs > 2000) {
            message.error('登录已过期，请重新登录');
            lastAuthErrorTs = now;
          }
          if (!isRedirectingToLogin) {
            isRedirectingToLogin = true;
            clearAuthData();
            // 避免多次跳转
            setTimeout(() => { isRedirectingToLogin = false; }, 3000);
            history.push('/login');
          }
        }
        break;
      }
      case 404:
        message.error('请求的资源不存在');
        break;
      case 500:
        message.error('服务器内部错误');
        break;
      case 502:
      case 503:
      case 504:
        message.error('服务暂时不可用，请稍后重试');
        break;
      default:
        message.error(data?.msg || `请求失败 (${status})`);
    }
    
    return Promise.reject(error.response);
  }
);

// 清除认证数据
function clearAuthData() {
  ['access_token', 'token_type', 'user_name', 'expires_in', 'Instance_value'].forEach(key => {
    db.ss.remove(key);
  });
}

// 请求重试函数
async function requestWithRetry(requestFn, retryCount = CONFIG.retryCount, options = {}) {
  try {
    return await requestFn();
  } catch (error) {
    if (retryCount > 0 && shouldRetry(error, options)) {
      // eslint-disable-next-line no-console
      console.log(`[API] 请求失败，${CONFIG.retryDelay}ms后重试 (剩余${retryCount}次)`);
      await new Promise(resolve => setTimeout(resolve, CONFIG.retryDelay));
      return requestWithRetry(requestFn, retryCount - 1, options);
    }
    throw error;
  }
}

// 判断是否应该重试
function shouldRetry(error, options = {}) {
  // 如果明确指定不重试，直接返回 false
  if (options.noRetry) return false;
  
  const status = error?.response?.status;
  return !status || status >= 500 || status === 429; // 服务器错误或限流时重试
}

/**
 * GET请求
 * @param {String} url 
 * @param {Object} params 
 */
export function get(url, params) {
  return requestWithRetry(() => 
    new Promise((resolve, reject) => {
      axios.get(url, { params }).then(res => {
        resolve(res.data)
      }).catch(err => {
        reject(err.data)
      })
    })
  );
}

/**
 * POST请求
 * @param {String} url
 * @param {Object} params 
 */
export function post(url, params) {
  return requestWithRetry(() => 
    new Promise((resolve, reject) => {
      axios.post(url, params).then(res => {
        resolve(res.data);
      }).catch(err => {
        reject(err.data)
      })
    })
  );
}

/**
 * DELETE请求
 * @param {String} url
 * @param {Object} params
 */
export function Delete(url, params) {
  return requestWithRetry(() => 
    new Promise((resolve, reject) => {
      axios.delete(url, params).then(res => {
        resolve(res.data);
      }).catch(err => {
        reject(err.data)
      })
    })
  );
}

/**
 * POST请求（表单序列化）
 * @param {String} url 
 * @param {Object} params 
 */
export function qspost(url, params) {
  return requestWithRetry(() => 
    new Promise((resolve, reject) => {
      axios.post(url, Qs.stringify(params)).then(res => {
        resolve(res.data);
      }).catch(err => {
        reject(err.data)
      })
    })
  );
}

/**
 * POST请求（无重试，用于登录等操作）
 * @param {String} url
 * @param {Object} params 
 */
export function postNoRetry(url, params) {
  return new Promise((resolve, reject) => {
    axios.post(url, params, { 
      skipErrorMessage: true,  // 添加标记，跳过拦截器的错误提示
      headers: { 'Content-Type': 'application/json' } // 登录接口使用 JSON，避免422
    }).then(res => {
      resolve(res.data);
    }).catch(err => {
      reject(err.data || err.response?.data || { msg: '请求失败' })
    })
  });
}
