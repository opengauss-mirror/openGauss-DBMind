import React, { Component } from 'react';
import { Empty } from 'antd';
import PropTypes from 'prop-types';
import ReactEcharts from 'echarts-for-react';
import { formatTimestamp } from '../../../utils/function';

export default class TotalConnectionsChart extends Component {
  static propTypes={
    totalLineChart:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      seriesData: [],
      xdata: [],
      ifShow: true,
      legendData:[],
    }
  }
  getOption = () => {
    return {
      title: {
        text: 'Total Connections',
        left: 'left',
        textStyle:{
          fontSize: "14",
          fontFamily: "Arial",
          fontWeight: "Bold"
        }
      },
      legend: {
        data:this.state.legendData,
        right:22
      },
      grid: {
        top: "15%",
        left: "3%",
        right: "4%",
        bottom: "2%",
        containLabel: true,
      },
      xAxis: {
        type: 'category',
        boundaryGap: false,
        splitLine: {
          //网格线
          show: true, //是否显示
          lineStyle: {
            //网格线样式
            color: '#F2F2F2', //网格线颜色
            width: 1, //网格线的加粗程度
            type: 'dashed' //网格线类型
          }
        },
        axisLine: {
          show: true,
          lineStyle: {
            color: '#939393',
            width: 1,
            type: 'solid'
          }
        },
        axisLabel: {
          show: true,
          margin: 10,
          textStyle: {
            color: '#4D5964',
            fontSize: 11,
            fontFamily: 'Arial',
            fontWeight: 'normal'
          }
        },
        data: this.state.xdata.map(function (str) {
          return str.replace(' ', '\n');
        })
      },
      yAxis: {
        min: 0,
        splitLine: {
          //网格线
          show: true, //是否显示
          lineStyle: {
            //网格线样式
            color: '#F2F2F2', //网格线颜色
            width: 1, //网格线的加粗程度
            type: 'dashed' //网格线类型
          }
        },
        ayisLine: {
          show: true,
          lineStyle: {
            color: '#939393',
            width: 1,
            type: 'solid'
          }
        },
        axisLabel: {
          margin: 10,
          
          show: true,
          textStyle: {
            color: '#4D5964',
            fontSize: 11,
            fontFamily: 'Arial',
            fontWeight: 'normal',
            align: 'right'
          }
        },
        type:'value'
      },
      tooltip: {
          trigger: 'axis',
          textStyle: {
          align: 'left'
        }
      },
    
      series: this.state.seriesData
    };
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    if (JSON.stringify(nextProps.totalLineChart.data) !== '{}') {
      let colors = [ '#2CA768','#EC6F1A', '#EEBA18','#5890FD'], legendData = []
          // 处理X轴
          let formatTimeData = [];
          nextProps.totalLineChart.timestamps.forEach(ele => {
            formatTimeData.push(formatTimestamp(ele));
          });
            let allData = [],seriesItem = {}
            Object.keys(nextProps.totalLineChart.data.total_connection).forEach(function (data, i, v) {
              legendData.push(data);
              seriesItem = {
                data: nextProps.totalLineChart.data.total_connection[data],
                type: 'line',
                smooth: true,
                name: data,
                symbol: 'circle',
               
                itemStyle: {
                  normal: {
                    color: colors[i],
                    lineStyle: {
                      width: 1,
                    },
                  },
                },
               
              }
              allData.push(seriesItem)
            })
      this.setState({
        ifShow: true,
        seriesData: allData,
        legendData: legendData,
        xdata: formatTimeData,
      })
    } else {
      this.setState({
        ifShow: false
      })
    }
  }
  render () {
    return (
      <div >
        {this.state.ifShow ?
          <ReactEcharts
          className="systemBorder"
            ref={(e) => {
              this.echartsElement = e
            }}
            option={this.getOption()}
            style={{ width: '100%', height: 240 }}
            lazyUpdate={true}
          >
          </ReactEcharts>
          : <Empty description={false} style={{ paddingTop: 50 }} />}
      </div>
    )
  }
}
