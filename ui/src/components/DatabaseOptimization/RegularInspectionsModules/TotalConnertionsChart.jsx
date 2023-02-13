import React, { Component } from 'react';
import { Empty } from 'antd';
import ReactEcharts from 'echarts-for-react';
import { formatTimestamp } from '../../../utils/function';

export default class TotalConnertionsChart extends Component {
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
        text: 'Total Connertions',
        left: 'center',
        textStyle:{
          color: '#314b71',
          fontSize: '12'
        }
      },
      legend: {
        data:this.state.legendData,
        x: 'right' 
      },
      grid: {
        containLabel: true,
        width: '100%',
        left: '0%',
        top: '15%',
        right: '0%',
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
            color: '#5470c6',
            fontSize: '10'
          }
        },
        type: 'category',
        data: this.state.xdata
      },
      yAxis: {
        type: 'value',
        nameTextStyle: {
          color: '#5470c6',
          padding: [0, 0, 10, 50],
        },
        nameGap: 10,
        axisLabel: {
          textStyle: {
            color: '#5470c6',
            fontSize: '10'
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
        right: '0.8%',
        height: 15
      },
      series: this.state.seriesData
    };
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    if (JSON.stringify(nextProps.totalLineChart.data.total_connection) !== '{}') {
      let colors = ['#5470c6', '#91cc75', '#fac858', '#007acc' ], legendData = []
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
                symbol: 'none',
                color: colors[i],
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