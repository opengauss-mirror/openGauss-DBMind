import axios from 'axios'
import Qs from 'qs'
import history from '../utils/history';
import db from '../utils/storage'
import { message } from 'antd';

const targetUrl = window.location.origin
if (process.env.NODE_ENV === 'development') {
  axios.defaults.baseURL = targetUrl + '/transpond'
} else {
  axios.defaults.baseURL = targetUrl + '/api'
}
axios.defaults.timeout = 30000//30s
axios.defaults.headers.post['Content-Type'] = 'application/x-www-form-urlencoded;charset=UTF-8';

axios.interceptors.request.use(config => {
  const token = 'Bearer ' + db.ss.get('access_token')
  token && (config.headers.Authorization = token);
  return config;
}, error => {
  message.error('Request Timeout!');
  return Promise.error(error);
});

axios.interceptors.response.use(
  response => {
    if (response.status === 200) {
      return Promise.resolve(response);
    } else {
      return Promise.reject(response);
    }
  },
  error => {
    if (error.response.status) {
      switch (error.response.status) {
        // 401: 未登录                
        case 401:
          history.push('/login')
          break;
        // 403 token过期               
        case 403:
          message.error('Login expired. Please log in again!')
          db.ss.remove('access_token')
          db.ss.remove('token_type')
          db.ss.remove('user_name')
          db.ss.remove('expires_in')
          setTimeout(() => {
            history.push('/login')
          }, 1000);
          break;
        // 404请求不存在
        case 404:
          message.error('The network request does not exist.');
          break;
        case 504:
          message.error('Login failed.');
          db.ss.remove('access_token')
          db.ss.remove('token_type')
          db.ss.remove('user_name')
          history.push('/login')
          break;
        default:
          message.error(error.response.data.msg);
      }
      return Promise.reject(error.response);
    }
  })

/**
 * get
 * @param {String} url 
 * @param {Object} params 
 */
export function get(url, params) {
  return new Promise((resolve, reject) => {
    axios.get(url, {
      params: params
    }).then(res => {
      resolve(res.data)
    }).catch(err => {
      reject(err.data)
    })
  });
}

/**
 * post
 * @param {String} url
 * @param {Object} params 
 */
export function post(url, params) {
  return new Promise((resolve, reject) => {
    axios.post(url, params)
      .then(res => {
        resolve(res.data);
      })
      .catch(err => {
        reject(err.data)
      })
  })
}

/**
 * post参数序列化
 * @param {String} url 
 * @param {Object} params 
 */
export function qspost(url, params) {
  return new Promise((resolve, reject) => {
    axios.post(url, params)
      .then(res => {
        resolve(res.data);
      })
      .catch(err => {
        reject(err.data)
      })
  });
}
