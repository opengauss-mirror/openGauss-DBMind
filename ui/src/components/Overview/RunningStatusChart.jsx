import React, { Component } from 'react';
import { Card, message, Empty, Spin } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';import ReactEcharts from 'echarts-for-react';
import { getRunningStatusInterface } from '../../api/overview';

export default class RunningStatusChart extends Component {
  constructor() {
    super()
    this.state = {
      chartData: [],
      indicatorData: [],
      legendData: [],
      showFlag: 0
    }
  }
  getOption = () => {
    return {
      tooltip: {
        trigger: 'item',
      },
      legend: {
        data: this.state.legendData,
        bottom: '2%',
        left: '0%'
      },
      grid: {
        left: '0%',
        right: '4%',
        bottom: '0%',
      },
      radar: {
        center: ['55%', '45%'],
        radius: '68%',
        nameGap: 7,
        name: {
          fontSize: 12,
          color: '#6e7079',
        },
        indicator: this.state.indicatorData
      },
      series: [{
        type: 'radar',
        left: '60%',
        symbolSize: 5,
        data: this.state.chartData
      }]
    }
  }
  async getRunningStatus () {
    let _this = this
    const { success, data, msg } = await getRunningStatusInterface()
    if (success) {
      if (JSON.stringify(data) !== '{}') {
        let legengArr = []//legend
        let indicatorData = []//indicator
        let dataArrFlag = []
        for (let item in data) {
          legengArr.push(item)
          let itsArr = []
          for (let it in data[item]) {
            let itObj = {
              key: it,
              value: data[item][it]
            }
            itsArr.push(itObj)
          }
          let itemObj = {
            name: item,
            values: itsArr
          }
          dataArrFlag.push(itemObj)
        }
        // 找出最长的作为indicatorData
        let max = dataArrFlag[0].values.length
        let b = 0
        for (let i = 1; i < dataArrFlag.length; i++) {
          if (max < dataArrFlag[i].values.length) {
            max = dataArrFlag[i].values.length
            b = i
          }
        }
        dataArrFlag[b].values.forEach((item) => {
          let obj = {
            name: item.key,
            max: 1
          }
          indicatorData.push(obj)
        })
        //  渲染的数据
        let fatasFlagArrs = []
        indicatorData.forEach((item) => {
          fatasFlagArrs.push(item.name)
        })
        let finaData = []
        dataArrFlag.forEach((items) => {
          let arr = []
          fatasFlagArrs.forEach((item, index) => {
            if (items.values[index] && items.values[index].key === item) {
              arr.push(items.values[index].value)
            } else {
              arr.push(0)
            }
          })
          let objs = {
            name: items.name,
            value: arr
          }
          finaData.push(objs)
        })
        indicatorData.forEach(item => {
          item.name = (item.name).replace(/_/g, ' ')
        })
        this.setState(() => ({
          showFlag: 0,
          chartData: finaData,
          indicatorData: indicatorData,
          legendData: legengArr
        }), () => {
          _this.getOption()
        })
      } else {
        this.setState({showFlag: 1})
      }
    } else {
      this.setState({showFlag: 1})
      message.error(msg)
    }
  }
  handleRefresh () {
    this.setState({showFlag: 2}, () => {
      this.getRunningStatus()
    })
  }
  componentDidMount () {
    this.getRunningStatus()
  }
  render () {
    return (
      <div>
        <Card title="Running Status" style={{ height: 350 }} extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />}>
          {this.state.showFlag === 0 ? <ReactEcharts
            ref={(e) => {
              this.echartsElement = e
            }}
            option={this.getOption()}
            style={{ width: '100%', height: 260 }}
            lazyUpdate={true}
          >
          </ReactEcharts> : this.state.showFlag === 1 ? <Empty description={false} style={{ paddingTop: 50 }} /> : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>}
        </Card>
      </div>
    )
  }
}
