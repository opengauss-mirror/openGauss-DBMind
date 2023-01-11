import React, { Component } from 'react';
import { Card, Empty } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import ReactEcharts from 'echarts-for-react';
import PropTypes from 'prop-types';
import { formatTimestamp } from '../../../utils/function';

export default class SuggestionsChangeChart extends Component {
  static propTypes={
    suggestions:PropTypes.object.isRequired
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
        right: '0%'
      },
      xAxis: {
        axisLine: {
          lineStyle: {
            width: 0,
          }
        },
        axisLabel: {
          textStyle: {
            color: '#314b71',
            fontSize: '10'
          }
        },
        type: 'category',
        data: this.state.xdata
      },
      yAxis: {
        type: 'value',
        name: this.state.yname,
        nameTextStyle: {
          padding: [0, 0, 0, 120],
          color: '#314b71',
        },
        nameGap: 10,
        axisLabel: {
          textStyle: {
            color: '#314b71',
            fontSize: '10'
          }
        }
      },
      tooltip: {
        trigger: 'axis',
      },
      legend: {
        data: ['Index Statistics'],
        x: 'left',
        padding: [10, 0, 0, 200]
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
        ifShow: true,
        xdata: formatTimeData,
        seriesData: [...ydata],
        yname: 'suggestions'
      }), () => {
        this.getOption()
      })
    } else {
      this.setState({ifShow: false})
    }
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    this.getChartData(nextProps.suggestions)
  }
  render () {
    return (
      <div className="mb-20" >
        <Card title="Suggestions">
          {this.state.ifShow ? <ReactEcharts
            ref={(e) => {
              this.echartsElement = e
            }}
            option={this.getOption()}
            style={{ width: '100%', height: 250 }}
            lazyUpdate={true}
          >
          </ReactEcharts> : <Empty description={this.state.ifShow} style={{ height: 250, paddingTop: 80 }} />}
        </Card>
      </div>
    )
  }
}
