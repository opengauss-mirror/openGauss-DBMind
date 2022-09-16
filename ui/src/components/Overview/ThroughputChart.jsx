import React, { Component } from 'react';
import { Card, message, Empty, Spin } from 'antd';
import { ReloadOutlined } from '@ant-design/icons'; 
import ReactEcharts from 'echarts-for-react';
import { getQpsInterface } from '../../api/overview';
import { formatTimestamp } from '../../utils/function';

export default class ThroughputChart extends Component {
  constructor(props) {
    super(props)
    this.state = {
      seriesData: [],
      xdata: [],
      yname: '',
      showFlag: 0,
    }
  }
  async getQps () {
    let params = {
      name: 'gaussdb_qps_by_instance',
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
        // 处理Y轴
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
          showFlag: 0,
          xdata: formatTimeData,
          seriesData: [...ydata],
          yname: data[0].name
        }), () => {
          this.getOption()
        })
      } else {
        this.setState({showFlag: 1})
      }
    } else {
      this.setState({showFlag: 1})
      message.error(msg)
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
          padding: [0, 0, 0, 80],
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
      tooltip: {trigger: 'axis'},
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
  handleRefresh () {
    this.setState({showFlag: 2}, () => {
      this.getQps()
    })
  }
  componentDidMount () {
    this.getQps()
    this.echartsElement.resize()
  }
  render () {
    return (
      <div className="mb-20">
        <Card title="Throughput" style={{ height: 320 }} extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />}>
          {this.state.showFlag === 0 ? <ReactEcharts
            ref={(e) => {
              this.echartsElement = e
            }}
            option={this.getOption()}
            style={{ width: '100%', height: 240 }}
            lazyUpdate={true}
          >
          </ReactEcharts> : this.state.showFlag === 1 ? <Empty description={false} style={{ paddingTop: 50 }} /> : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>}
        </Card>
      </div>
    )
  }
}
