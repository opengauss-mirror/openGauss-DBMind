import React, { Component } from 'react';
import { Card, Empty } from 'antd';
import ReactEcharts from 'echarts-for-react';
import PropTypes from 'prop-types';
import { formatTimestamp } from '../../../utils/function';

export default class InvalidIndexesChange extends Component {
  static propTypes={
    invalidIndexes:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      seriesData: [],
      xdata: [],
      yname: '',
      ifShow: true,
    }
  }
  getOption = () => {
    return {
      grid: {
        containLabel: true,
        width: '100%',
        left: '0%',
        top: '15%',
        right: '0%',
        bottom: 30
      },
      xAxis: {
        axisLine: {
          lineStyle: {
            width: 0,
          }
        },
        axisLabel: {
          color: '#314b71',
          fontSize: 8,
          padding: [0, 0, 0, 80],
        },
        type: 'category',
        data: this.state.xdata
      },
      yAxis: {
        type: 'value',
        name: this.state.yname,
        offset: -8,
        nameTextStyle: {
          color: '#314b71',
          fontSize: '10',
          padding: [0, 0, 0, 100]
        },
        axisLabel: {
          textStyle: {
            color: '#314b71',
            fontSize: '8'
          }
        }
      },
      tooltip: {
        trigger: 'axis',
      },
      legend: {
        data: ['SQL Collection'],
        right: 0,
        itemWidth: 15,
        itemHeight: 10,
        textStyle: {
          fontSize: 8
        }
      },
      dataZoom: {
        start: 0,
        end: 100,
        show: true,
        type: 'slider',
        handleSize: '100%',
        left: '0%',
        right: '1%',
        height: 10,
        bottom: 8,
        textStyle: {
          fontSize: 8
        }
      },
      series: this.state.seriesData
    }
  }
  getChartData (data) {
    if (data.timestamps.length > 0) {
      // 处理X轴
      let formatTimeData = []
      data.timestamps.forEach(ele => {
        formatTimeData.push(formatTimestamp(ele))
      });
      // 处理Y轴数据
      let ydata = []
      let seriesItem = {
        data: data.values,
        type: 'line',
        smooth: true,
        name: '',
        symbol: 'none',
      }
      ydata.push(seriesItem)
      this.setState(() => ({
        xdata: formatTimeData,
        seriesData: [...ydata],
        yname: 'invalid_indexes'
      }), () => {
        this.getOption()
      })
    } else {
      this.setState({ifShow: false})
    }
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    this.getChartData(nextProps.invalidIndexes)
  }
  render () {
    return (
      <div>
        <Card title="Invalid Indexes" >
          {this.state.ifShow ? <ReactEcharts
            ref={(e) => {
              this.echartsElement = e
            }}
            option={this.getOption()}
            style={{ width: '100%', height: 200 }}
            lazyUpdate={true}
          >
          </ReactEcharts> : <Empty description={this.state.ifShow} style={{ height: 200, paddingTop: 50 }} />}
        </Card>
      </div>
    )
  }
}
