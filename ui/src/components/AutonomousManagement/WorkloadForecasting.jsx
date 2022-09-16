import React, { Component } from 'react';
import { Button, Card, Col, DatePicker, Empty, InputNumber, message, Row, Select, Space, Spin } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import moment from 'moment';
import ReactEcharts from 'echarts-for-react';
import {
  getForecastChartInterface,
  getSearchMetricInterface,
  getWorkloadForcastInterface
} from '../../api/autonormousMangemant'
import { formatTimestamp } from '../../utils/function'

const { RangePicker } = DatePicker;
const { Option } = Select;

export default class WorkloadForecasting extends Component {
  constructor() {
    super()
    this.state = {
      inputValue: 1,
      metricName: '',
      minsInputValue: 10,
      maxsInputValue: 10,
      btnType: false,
      chartData: [],
      xdata: [],
      yName: '',
      seriesData: [],
      options: [],
      startTime: '',
      endTime: '',
      loadingflag: false,
      ifShow: true,
      stepVal: '',
      legendData: [],
      showFlag: false,
      xFlag: [],
      yFlag: [],
      colorFlag: [],
      showType: 0,
      newSelectValue: ''
    }
  }
  // 时间框
  onChangeTimes = (dates, dateStrings) => {
    this.setState(() => ({
      startTime: new Date(dateStrings[0]).getTime(),
      endTime: new Date(dateStrings[1]).getTime()
    }))
  }
  // 下拉框数据
  async getSearchMetric () {
    const { success, data, msg } = await getSearchMetricInterface()
    if (success) {
      this.setState(() => ({
        options: data,
        metricName: data[0]
      }), () => {
        this.getWorkloadForcast()
      })
    } else {
      message.error(msg)
    }
  }
  // 查询图表
  async getWorkloadForcast () {
    let params = {
      name: this.state.metricName,
      time: {
        start: !this.state.startTime ? null : this.state.startTime,
        end: !this.state.endTime ? null : this.state.endTime
      }
    }
    this.setState({
      loadingflag: true,
      showFlag: false,
      showType: 0
    })
    const { success, data, msg } = await getWorkloadForcastInterface(params)
    if (success) {
      if (data.length > 0) {
        // 处理X轴
        let formatTimeData = []
        data[0].timestamps.forEach(ele => {
          formatTimeData.push(formatTimestamp(ele))
        });
        // 处理Y轴数据
        let allData = data
        let ydata = []
        let colorDataFlag = []
        let legendDataFlag = []
        let colors = ['#5c7bd9', '#91cc75', '#fac858', '#007acc', '#fb542f', '#c586c0', '#1890ff', '#d69439', '#b03a5b', '#eb8f53', '#c5c63e', '#1e1e1e', '#5470c6', '#91cc75', '#fac858', '#007acc', '#fb542f', '#34a853', '#d69439', '#b03a5b', '#eb8f53', '#c5c63e', '#1e1e1e', '#5470c6', '#91cc75', '#fac858', '#007acc', '#fb542f', '34a853', '#d69439', '#b03a5b', '#eb8f53', '#c5c63e']
        allData.forEach((item, index) => {
          let nametooltip = ''
          Object.keys(item.labels).forEach(function (key,) {
            nametooltip += `${key}:${item.labels[key] ? item.labels[key] : '-'}  `
          })
          let seriesItem = {
            data: item.values,
            type: 'line',
            smooth: true,
            name: nametooltip,
            symbol: 'none',
            color: colors[index],
          }
          legendDataFlag.push(nametooltip)
          ydata.push(seriesItem)
          colorDataFlag.push(item.labels)
        })
        this.setState(() => ({
          showFlag: true,
          loadingflag: false,
          ifShow: true,
          legendData: legendDataFlag,
          xdata: formatTimeData,
          xFlag: formatTimeData,
          seriesData: [...ydata],
          yFlag: [...ydata],
          colorFlag: [...colorDataFlag],
          yName: this.state.metricName,
          showType: 1
        }), () => {
          this.getOption()
        })
      } else {
        this.setState({
          ifShow: false,
          showType: 2
        })
        message.warning('No Data')
      }
    } else {
      this.setState({
        ifShow: false,
        showType: 2
      })
      message.error(msg)
    }
  }
  showChart = () => {
    if (!this.state.metricName) {
      message.warning('Please choose metric name')
    } else {
      this.getWorkloadForcast()
    }
  }
  filter = (inputValue, path) => {
    return path.some(option => option.label.toLowerCase().indexOf(inputValue.toLowerCase()) > -1);
  }
  getOption = () => {
    return {
      grid: {
        containLabel: true,
        top: 25,
        right: 20,
        left: 5,
        height: 260,
        width: '100%'
      },
      legend: {
        type: 'scroll',
        orient: 'vertical',
        data: this.state.legendData,
        width: '100%',
        left: 10,
        height: 390,
        top: 324,
        pageButtonPosition: 'start',
        selectedMode: 'multiple',
        formatter: function (params) {
          let tip1 = '';
          let tip = '';
          let le = params.length
          if (le > 240) {
            let l = Math.ceil(le / 240)
            for (let i = 1; i <= l; i++) {
              if (i < l) {
                tip1 += params.slice(i * 240 - 240, i * 240) + '\n'
              } else if (i === l) {
                tip = tip1 + params.slice((l - 1) * 240, le)
              }
            }
            return tip
          } else {
            tip = params
            return tip
          }
        }
      },
      xAxis: {
        axisLine: {
          lineStyle: {
            width: 0,
          }
        },
        axisLabel: {
          padding: [0, 0, 0, 46],
          textStyle: {
            color: '#314b71',
            fontSize: '10'
          },
        },
        type: 'category',
        data: this.state.xdata
      },
      yAxis: {
        type: 'value',
        name: this.state.yName,
        nameLocation: 'end',
        nameTextStyle: {
          padding: [0, 0, -4, 160],
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
          align: 'left',
        },
        confine: true,
        extraCssText: 'position:fixed',
        formatter: function (params) {
          let res = `${params[0].name} <br/>`
          for (const item of params) {
            if (item.value !== '-' && item.value != null) {
              res += `<span style="background: ${item.color}; height:10px; width: 10px; border-radius: 50%;display: inline-block;margin-right:10px;"></span> ${item.seriesName} ：${item.value}<br/>`
            } else if (item.value == null) {
              res += `<span style="background: ${item.color}; height:10px; width: 10px; border-radius: 50%;display: inline-block;margin-right:10px;"></span> ${item.seriesName} ：${''}<br/>`
            }
          }
          return res
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
        height: 15,
        top: 294
      },
      series: this.state.seriesData
    }
  }
  displayRender = (label) => {
    return label[label.length - 1];
  }
  forecast = () => {
    this.setState({ btnType: !this.state.btnType,}, () => {
      this.getForecastChart()
    }
    )
  }
  // forecast
  async getForecastChart () {
    let params = {
      name: this.state.metricName,
      para: {
        start: !this.state.startTime ? null : this.state.startTime,
        end: !this.state.endTime ? null : this.state.endTime
      }
    }
    this.setState({
      showType: 0
    })
    const { success, msg, data } = await getForecastChartInterface(params)
    if (success) {
      this.setState({loadingflag: false})
      if (data.length > 0) {
        // 处理X轴
        let formatTimeData = []
        data[0].timestamps.forEach(ele => {
          formatTimeData.push(formatTimestamp(ele))
        });
        // 处理Y轴数据
        let allData = data
        let ydata = []
        let legendForecastDataFlag = []
        let colors = ['#5c7bd9', '#91cc75', '#fac858', '#007acc', '#fb542f', '#c586c0', '#1890ff', '#d69439', '#b03a5b', '#eb8f53', '#c5c63e', '#1e1e1e', '#5470c6', '#91cc75', '#fac858', '#007acc', '#fb542f', '#34a853', '#d69439', '#b03a5b', '#eb8f53', '#c5c63e', '#1e1e1e', '#5470c6', '#91cc75', '#fac858', '#007acc', '#fb542f', '34a853', '#d69439', '#b03a5b', '#eb8f53', '#c5c63e']
        allData.forEach((item, index) => {
          let seriesItem = {}
          let nametooltip = ''
          Object.keys(item.labels).forEach(function (key) {
            nametooltip += `${key}:${item.labels[key] ? item.labels[key] : '-'}   `
          })
          seriesItem = {
            data: item.values,
            type: 'line',
            smooth: true,
            name: nametooltip,
            symbol: 'none',
            color: colors[index],
            lineStyle: {
              color: colors[index],
              type: 'dotted'
            }
          }
          legendForecastDataFlag.push(nametooltip)
          ydata.push(seriesItem)
        })
        let arrs = []//X轴
        let xLinshi = [...this.state.xFlag]
        arrs = xLinshi.concat(formatTimeData)
        let arry = []
        arry.length = JSON.stringify(JSON.parse(this.state.xFlag.length))
        arry.fill('-')
        ydata.forEach(item => {
          item.data = arry.concat(item.data)
        })
        ydata.forEach((item, index) => {
          item.data.forEach((it, idx) => {
            if (idx === arry.length - 1) {
              item.data[idx] = this.state.yFlag[index].data[idx]
            }
          })
        })
        this.setState(() => ({
          showFlag: true,
          loadingflag: false,
          ifShow: true,
          xdata: arrs,
          legendData: [...this.state.legendData, ...legendForecastDataFlag],
          seriesData: [...this.state.yFlag, ...ydata],
          yName: this.state.metricName,
          showType: 1
        }), () => {
          this.getOption()
        })
      } else {
        this.setState({
          showType: 2
        })
        message.warning('No Data')
      }
    } else {
      this.setState({showType: 2})
      message.error(msg)
    }
  }
  onSearch = (value) => {
    if (value) {
      this.setState({
        metricName: value,
        newSelectValue: value
      })
    }
  };
  onBlurSelect = () => {
    const value = this.state.newSelectValue
    if (value) {
      this.handleChange(value)
      this.setState({ newSelectValue: ''})
    }
  }
  handleChange = (e) => {
    this.setState({
      newSelectValue: e,
      metricName: e
    })
  }
  handleInputChange = (e) => {
    this.setState({stepVal: e})
  }
  isOnlyClickedOneIsUnSelected = (name, selected) => {
    let unSelectedCount = 0
    for (let item in selected) {
      if (!Object.prototype.hasOwnProperty.call(selected,item)) {
        continue
      }
      if (selected[item] === false) {
        ++unSelectedCount
      }
    }
    return unSelectedCount === 1 && selected[name] === false
  }
  onlyEnableCurrentSelectedLegend = (name, selected) => {
    let legend = []
    for (let item in selected) {
      if (!Object.prototype.hasOwnProperty.call(selected,item)) {
        continue
      }
      legend.push({ 'name': item })
      this.echartsElement.dispatchAction({
        type: 'legendToggleSelect',
        batch: legend
      })
    }
  }
  handleRefresh(){
    this.setState({
      startTime:'',
      endTime:'',
      timeKey:new Date(),
      stepVal:''
    },()=>{
      this.showChart()
    })
  }
  componentDidMount () {
    this.getSearchMetric()
  }
  render () {
    return (
      <div>
        <Card title="Metric Plots" extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />}>
          <Row style={{ marginBottom: 20,paddingRight:20 }} justify="space-between">
            <Col span={14}>
              <Space size={12}>
                <RangePicker
                  ranges={{
                    Today: [moment(), moment()],
                    'This Month': [moment().startOf('month'), moment().endOf('month')],
                  }}
                  key={this.state.timeKey}
                  showTime
                  style={{ width: 400 }}
                  format="YYYY/MM/DD HH:mm:ss"
                  onChange={this.onChangeTimes}
                />
                <Select value={this.state.metricName} onChange={(e) => this.handleChange(e)} placeholder="Search metric" showSearch
                  onSearch={(e) => { this.onSearch(e) }} onBlur={() => this.onBlurSelect()} optionFilterProp="children" filterOption={(input, option) =>
                    option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0
                  } style={{ width: 300 }}>
                  {
                    this.state.options.map(item => {
                      return (
                        <Option value={item} key={item}>{item}</Option>
                      )
                    })
                  }
                </Select>
                <Button onClick={() => this.showChart()}>Search</Button>
                <span>step:</span><InputNumber min={15000} step={1000} placeholder="auto" value={this.state.stepVal} name="stepVal" onChange={(e) => { this.handleInputChange(e) }}/>
              </Space>
            </Col>
            <Col span={1}>
              <Button onClick={() => this.forecast()}>Forecast</Button>
            </Col>
          </Row>
          <div style={{ width: '100%', height: 730, textAlign: 'center' }}>
            {
              this.state.showType === 0 ? <Spin style={{ margin: '250px auto' }} /> : this.state.showType === 1 ? <ReactEcharts
                ref={(e) => {
                  this.echartsElement = e
                }}
                option={this.getOption()}
                style={{ minHeight: '100%' }}
                lazyUpdate={true}
              >
              </ReactEcharts> : this.state.showType === 2 ? <Empty style={{ margin: '250px auto' }} description={''} /> : ''
            }
          </div>
        </Card>
      </div>
    )
  }
}
