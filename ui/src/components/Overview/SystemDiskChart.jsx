import React, { Component } from 'react';
import { Empty, message } from 'antd';
import ReactEcharts from 'echarts-for-react';
import { getQpsInterface } from '../../api/overview';
import { formatTimestamp } from '../../utils/function';

export default class SystemDiskChart extends Component {
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
      },
      xAxis: {
        axisLine: {
          lineStyle: {
            width: 0,
          }
        },
        axisLabel: {
          padding: [0, 0, 0, 78],
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
        name: this.state.yname,
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
    }
  }
  async getQps () {
    let params = {
      name: 'os_disk_iops',
      time: {
        start: this.props.startTime,
        end: this.props.endTime
      }
    }
    const { success, data, msg } = await getQpsInterface(params)
    if (success) {
      if (data.length > 0) {
        // 处理X轴
        let formatTimeData = []
        data[0].timestamps.forEach(ele => {
          formatTimeData.push(formatTimestamp(ele))
        });
        // 处理Y轴数据
        let allData = data, ydata = [], colors = ['#5470c6', '#91cc75', '#fac858']
        allData.forEach((item, index) => {
          let seriesItem = {
            data: item.values,
            type: 'line',
            smooth: true,
            name: item.labels.from_instance,
            symbol: 'none',
            color: colors[index],
          }
          ydata.push(seriesItem)
        })
        this.setState(() => ({
          ifShow: true,
          xdata: formatTimeData,
          seriesData: [...ydata],
          yname: data[0].name,
        }), () => {
          this.getOption()
        })
      } else {
        this.setState({ifShow: false})
      }
    } else {
      this.setState({ifShow: false})
      message.error(msg)
    }
  }
  componentDidMount () {
    this.getQps()
    this.echartsElement.resize()
  }
  componentWillUnmount () {
      this.setState = () => {return}
  }
  render () {
    return (
      <div>
        {this.state.ifShow ? <ReactEcharts
          ref={(e) => {
            this.echartsElement = e
          }}
          option={this.getOption()}
          style={{ width: '100%', height: 240 }}
          lazyUpdate={true}
        >
        </ReactEcharts> : <Empty description={false} style={{ paddingTop: 50 }} />}
      </div>
    )
  }
}
