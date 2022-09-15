import React, { Component } from 'react';
import { Card, message, Select, Empty, Spin } from 'antd';
import { ReloadOutlined } from '@ant-design/icons'; 
import * as echarts from 'echarts';
import { getTransactionStateInterface } from '../../api/overview';

const { Option } = Select;
const style = 'position:absolute;z-index:100;color:#fff;font-size:12px;padding:5px;display:inline;border-radius:4px;background-color:#303133;box-shadow:rgba(0,0,0,0.3) 2px 2px 8px'
export default class TransactionStateChart extends Component {
  constructor() {
    super()
    this.state = {
      ydata: [],
      chartData: [],
      searchOptionList: [],
      chartArrFlag: [],
      defaultValue: '',
      showFlag: 0
    }
  }
  initChart = () => {
    let myChart = echarts.init(document.getElementById('transactionchart'))
    let option = {
      tooltip: {
        trigger: 'axis',
      },
      legend: {
        left: -5,
        top: -5,
        itemWidth:20,
        itemHeight:13
      },
      grid: {
        left: '0%',
        right: '4%',
        top: '10%',
        bottom: 10,
        width: '100%',
        containLabel: true
      },
      xAxis: {
        type: 'value',
        axisLabel: {
          show: true,
          rotate: 10
        },
      },
      yAxis: {
        type: 'category',
        axisLabel: {
          show: true,
          interval: 0,
          color: '#6e7079',
          formatter: function (value) {
            if (value.length > 3) {
              return value.substring(0, 3) + '...'
            } else {
              return value
            }
          }
        },
        triggerEvent: true,
        data: this.state.ydata
      },
      dataZoom: {
        type: 'slider',
        start: 0,
        end: 100,
        disabled: false,
        yAxisIndex: [0],
        width: 15,
        textStyle: {
          width: 20,
          overflow: 'truncate',
          ellipsis: '...'
        },
      },
      series: this.state.chartData,
    }
    myChart.setOption(option)
    this.extension(myChart)
  }
  // Y轴文字太长鼠标悬浮显示
  extension (chart) {
    let elementDiv = document.getElementById('transactionchart')
    if (elementDiv) {
      let div = document.createElement('div')
      div.setAttribute('id', 'chartdiv')
      div.style.display = 'block'
      document.querySelector('html').appendChild(div)
    }
    chart.on('mouseover', function (params) {
      if (params.componentType === 'yAxis') {
        let elementDiv = document.querySelector('#chartdiv')
        let elementStyle = style
        elementDiv.style.cssText = elementStyle
        elementDiv.innerHTML = params.value
        document.querySelector('html').onmousemove = function (event) {
          let elementDiv = document.querySelector('#chartdiv')
          let xx = event.pageX - 10
          let yy = event.pageY + 15
          elementDiv.style.top = yy + 'px'
          elementDiv.style.left = xx + 'px'
        }
      }
    })
    chart.on('mouseout', function (params) {
      if (params.componentType === 'yAxis') {
        let elementDiv = document.querySelector('#chartdiv')
        elementDiv.style.cssText = 'display:none'
      }
    })
  }
  async getTransactionState () {
    const { success, data, msg } = await getTransactionStateInterface()
    if (success) {
      let vdata = []
      if (JSON.stringify(data) !== '{}') {
        let arr = []
        Object.keys(data).forEach(function (key, i, v) {
          vdata = v
          let arrt = []
          Object.keys(data[key]).forEach(function (keyt, it, vt) {
            let objt = {
              name: vt[it],
              value: data[key][keyt]
            }
            arrt.push(objt)
          })
          let obj = {
            name: key,
            value: arrt
          }
          arr.push(obj)
        })
        this.setState({
          showFlag: 0,
          searchOptionList: vdata,
          defaultValue: vdata[0],
          chartArrFlag: arr
        }, () => {
          this.handleChangeChart(this.state.searchOptionList[0])
        })
      } else {
        this.setState({showFlag: 1})
      }
    } else {
      this.setState({showFlag: 1})
      message.error(msg)
    }
  }
  handleChangeChart (selval) {
    let seriesType = ['commit', 'abort']
    let yArr = []
    let commitSeriesData = []
    let abortSeriesData = []
    this.state.chartArrFlag.forEach((item) => {
      if (selval === item.name) {
        yArr = []
        commitSeriesData = []
        abortSeriesData = []
        item.value.forEach((it) => {
          yArr.push(it.name)
          commitSeriesData.push(it.value.commit)
          abortSeriesData.push(it.value.abort)
        })
      }
    })
    let arrflag = []
    let finaData = []
    seriesType.forEach((item) => {
      if (item === 'commit') {
        arrflag = commitSeriesData
      } else {
        arrflag = abortSeriesData
      }
      let objt = {
        name: item,
        type: 'bar',
        data: arrflag
      }
      finaData.push(objt)
    })
    this.setState({
      chartData: finaData,
      ydata: yArr
    }, () => {
      this.initChart()
    })
  }
  handleChange (value) {
    this.setState({defaultValue: value}, () => {
      this.handleChangeChart(this.state.defaultValue)
    })
  }
  handleRefresh () {
    this.setState({showFlag: 2}, () => {
      this.getTransactionState()
    })
  }
  componentDidMount () {
    this.getTransactionState()
  }
  render () {
    return (
      <div>
        <Card title="Transaction State" extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />} style={{ height: 350, position: 'relative' }}>
          {this.state.showFlag === 0 ? <>
            <Select size="small" value={this.state.defaultValue} onChange={(val) => this.handleChange(val)} style={{ position: 'absolute', width: 140, right: 24, top: 74 }}>
              {this.state.searchOptionList.map((item, index) => {
                return (
                  <Option value={item} key={index}>{item}</Option>
                )
              })}
            </Select>
            <div id="transactionchart" style={{ width: '100%', minHeight: 270, overflowY: 'auto' }}></div>
          </> : this.state.showFlag === 1 ? <Empty description={false} style={{ paddingTop: 50 }} /> : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>}
        </Card >
      </div >
    )
  }
}
