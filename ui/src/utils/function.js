// 1.时间戳转为年月日时分秒  2018-05-06 10:18:11
const formatTimestamp = function (data, fmt) {
  if (!data) return '';
  let timeStr = new Date(parseInt(data));
  fmt = fmt || 'yyyy-MM-dd hh:mm:ss';
  let o = {
    'M+': timeStr.getMonth() + 1,
    'd+': timeStr.getDate(),
    'h+': timeStr.getHours(),
    'm+': timeStr.getMinutes(),
    's+': timeStr.getSeconds(),
    'q+': Math.floor((timeStr.getMonth() + 3) / 3), //季度
    'S': timeStr.getMilliseconds() //毫秒
  };
  if (/(y+)/.test(fmt)) fmt = fmt.replace(RegExp.$1, (timeStr.getFullYear() + '').substr(4 - RegExp.$1.length));
  for (let k in o)
    if (new RegExp('(' + k + ')').test(fmt)) fmt = fmt.replace(RegExp.$1, (RegExp.$1.length === 1) ? (o[k]) : (('00' + o[k]).substr(('' + o[k]).length)));
  return fmt;
}
// 2018-05-06 10:18:11
// 2.表格标题下划线改为空格
const formatTableTitle = function (title) {
  let titleName = title.replace(/_/g, ' ')
  if (title.indexOf('sql') !== -1) {
    titleName = titleName.replace('sql', 'SQL')
  } else if (title.indexOf('db') !== -1) {
    titleName = titleName.replace('db', 'DB')
  }
  return titleName
}
//3.将秒转几天几小时
const formatSecond = function (second) {
  const days = Math.floor(second / 86400);
  const hours = Math.floor((second % 86400) / 3600);
  return days + 'd' + hours + 'h';
}
// 4.表格时间戳转换
const formatTableTime = function (data) {
  data.forEach(item => {
    Object.keys(item).forEach(function (key) {
      if (key === 'query_start' && item['query_start']) {
        item[key] = item[key].substring(0, item[key].indexOf('.'));
      } else if (key === 'last_updated' && item['last_updated']) {
        item[key] = item[key].substring(0, item[key].indexOf('.'));
      } else if (!item[key]) {
        item[key] = ''
      } else if (key.indexOf('time') !== -1 || key.indexOf('start') !== -1 || key.indexOf('end') !== -1) {
        if (key !== 'node_time' && key !== 'db_time' && key !== 'node_timestamp') {
          item[key] = formatSecond(item[key])
        }
      }
    })
  })
  return data
}
export { formatTimestamp, formatTableTitle, formatSecond, formatTableTime }