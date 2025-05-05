import React, { Component } from 'react';
import { Card, Empty, Spin } from 'antd';
import PropTypes from 'prop-types';
import ReactEcharts from 'echarts-for-react';
import { formatTimestamp } from '../../../utils/function';

export default class DynamicMemory extends Component {
  static propTypes={
    dynamicMemory:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      showFlag: 0,
      allDataRegular:[],
    }
  }
  async getQps (data) {
        let arrayData = [], colors = ['#5470c6', '#91cc75', '#fac858', '#007acc' ]
        Object.keys(data).forEach(function (key, i, v) {
          // 处理X轴
          let formatTimeData = [];
          data[key].timestamps.forEach(ele => {
            formatTimeData.push(formatTimestamp(ele));
          });
          // 处理Y轴
          let seriesItem = {
            data: data[key].data,
            type: 'line',
            smooth: true,
            name: key,
            symbol: 'none',
            color: colors[0],
          }
          let arr = []
          Object.keys(data[key].statistic).forEach(okey => {
            arr.push([okey, data[key].statistic[okey]].join(':'))
          })
          let param = {
            xdata:formatTimeData,
            seriesData:seriesItem,
            yname:key + " ( "+ arr.join(',') +" )"
          }
          arrayData.push(param);
        })
        this.setState(() => ({
          showFlag: 0,
          allDataRegular: [...arrayData],
        }), () => {
          this.echartsElement.resize();
        })
  }
  getOption = (item) => {
    return {
      title: {
        text: item.yname,
        left: 'center',
        textStyle:{
          color: '#314b71',
          fontSize: '12'
        }
      },
      grid: {
        containLabel: true,
        width: '100%',
        left: '0%',
        top: '15%',
        right: '0%'
      },
      xAxis: {
        axisLine: {
          lineStyle: {
            width: 0,
          }
        },
        axisLabel: {
          padding: [0, 0, 0, 80],
          textStyle: {
            color: '#314b71',
            fontSize: '10'
          }
        },
        type: 'category',
        data: item.xdata
      },
      yAxis: {
        type: 'value',
        nameLocation: 'end',
        nameTextStyle: {
          padding: [0, 0, 8, 120],
          color: '#314b71',
        },
        nameGap: 15,
        axisLabel: {
          textStyle: {
            color: '#314b71',
            fontSize: '12'
          }
        }
      },
      tooltip: {
        trigger: 'axis',
        textStyle: {
        align: 'left'
        }
      },
      dataZoom: {
        start: 0,
        end: 100,
        show: true,
        type: 'slider',
        handleSize: '100%',
        left: '0%',
        right: '0.3%',
        height: 15
      },
      series: item.seriesData
    }
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    if (JSON.stringify(nextProps.dynamicMemory) !== '{}') {
       this.getQps(nextProps.dynamicMemory)
    } else {
      this.setState({showFlag: 1})
    }
  }
  render () {
    return (
      <div style={{ textAlign: 'center' }}>
        <Card title='Dynamic Memory' style={{ height: '100%' }} >
          {this.state.showFlag === 0 ? this.state.allDataRegular.map((item) => {
                return (
                    <ReactEcharts
                    ref={(e) => {
                      this.echartsElement = e
                    }}
                    option={this.getOption(item)}
                    style={{ width: '100%', height: 240 }}
                    lazyUpdate={true}
                  >
                  </ReactEcharts>
                )
              })
            : this.state.showFlag === 1 ? <Empty description={false} style={{ paddingTop: 50 }} /> : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>}
        </Card>
      </div>
    )
  }
}
