import React, { Component } from 'react';
import { Button, Card, Col, Empty, InputNumber, message, Row, Select, Spin, Input} from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import ReactEcharts from 'echarts-for-react';
import { getForecastInterface, getSearchMetricInterface } from '../../api/autonomousManagement'
import { getAgentListInterface } from '../../api/common';
import { formatTimestamp } from '../../utils/function';
import db from '../../utils/storage';
import '../../assets/css/common.css'
import '../../assets/css/main/aiToolkit.css';

const { Option } = Select;
const { TextArea } = Input;

export default class RiskAnalysis extends Component {
  constructor() {
    super()
    this.state = {
      metricName: '',
      labels: '',
      xdata: [],
      optionsSel: [],
      options: [],
      showType: 2,
      newSelectValue: '',
      newSelValue: '',
      instanceName: '',
      isPressCtrl:false,
      allDataRegular:[],
      warningHours:1,
      upper:'',
      lower:'',
    }
  }
  // 下拉框数据
  async getSearchMetric () {
    const { success, data, msg } = await getSearchMetricInterface()
    if (success) {
      this.setState(() => ({
        options: data,
        metricName: data[0]
      }), () => {
        this.getItemList()
      })
    } else {
      message.error(msg)
    }
  }
  async getItemList () {
    const { success, data, msg } = await getAgentListInterface()
    let optionArr = ''
    Object.keys(data).forEach(function (key) {
      if(key === db.ss.get('Instance_value')){
        data[key].forEach(item => {
          data[key].push(item.split(":")[0])
        })
        optionArr = ([...new Set(data[key])])
      }
    })
    if (success) {
      this.setState(() => ({
        optionsSel: optionArr, instanceName: optionArr[0]
      }))
    } else {
      message.error(msg)
    }
  }
  // 查询图表
  async getWorkloadForecast () {
    let params = {
      instance_name: this.state.instanceName ? this.state.instanceName : null,
      metric_name: this.state.metricName ? this.state.metricName : null,
      labels: this.state.labels ? this.state.labels : null,
      warning_hours: this.state.warningHours ? this.state.warningHours : null,
      upper: this.state.upper ? this.state.upper : null,
      lower: this.state.lower ? this.state.lower : null,
    }
    this.setState({
      showType: 0
    })
    const { success, data, msg } = await getForecastInterface(params)
    if (success) {
      if (Object.keys(data).length > 0) {
        let instanceAllData = []
        Object.keys(data).forEach(function (key, i, v) {
          // 处理X轴
          let formatTimeData = [],forecastFormatTimeData = []
          data[key][i].timestamps.forEach(ele => {
            formatTimeData.push(formatTimestamp(ele))
          });
          if(data[key][i].forecast_timestmaps){
            data[key][i].forecast_timestmaps.forEach(ele => {
              forecastFormatTimeData.push(formatTimestamp(ele))
            });
            formatTimeData = formatTimeData.concat(forecastFormatTimeData)
          }
          // 处理Y轴数据
          let colors = ['#5c7bd9', '#91cc75', '#fac858', '#007acc', '#fb542f', '#c586c0', '#1890ff', '#d69439', '#b03a5b', '#eb8f53', '#c5c63e', '#1e1e1e', '#5470c6', '#91cc75', '#fac858', '#007acc', '#fb542f', '#34a853', '#d69439', '#b03a5b', '#eb8f53', '#c5c63e', '#1e1e1e', '#5470c6', '#91cc75', '#fac858', '#007acc', '#fb542f', '34a853', '#d69439', '#b03a5b', '#eb8f53', '#c5c63e']
          data[key].forEach((item, index) => {
            let  ydata = [], nametooltip = '', legendDataFlag = [], timeTotal = '', solidLine = [], dataArray = [], dashedLine = [], forecastSeriesItem = {}
            Object.keys(item.labels).forEach(function (key) {
              nametooltip += `${key}:${item.labels[key] ? item.labels[key] : '-'}  `
            })
            if(item.forecast_values){
              timeTotal = item.values.length + item.forecast_values.length;
              solidLine = item.values.concat(Array(timeTotal - item.values.length).fill(''));
              dataArray = [...item.values].fill('',0,item.values.length-1)
              dashedLine = dataArray.concat(item.forecast_values)
              forecastSeriesItem = {
                data: dashedLine,
                type: 'line',
                smooth: true,
                name: nametooltip,
                symbol: 'none',
                color: colors[index],
                lineStyle: {
                  type: 'dashed'
                }
              }
            }
            let seriesItem = {
              data: item.forecast_values ? solidLine : item.values,
              type: 'line',
              smooth: true,
              name: nametooltip,
              symbol: 'none',
              color: colors[index],
            }
            legendDataFlag.push(nametooltip)
            ydata.push(seriesItem)
            if(item.forecast_values){
              ydata.push(forecastSeriesItem)
            }
            let param = {
              xdata:formatTimeData,
              seriesData:ydata,
              legendData:legendDataFlag,
              warning:item.abnormal_detail,
            }
            instanceAllData.push(param);
          })
        })
        this.setState(() => ({
          allDataRegular: [...instanceAllData],
          showType: 1
        }))
      } else {
        this.setState({
          showType: 2
        })
        message.warning('No Data')
      }
    } else {
      this.setState({
        showType: 2
      })
      message.error(msg)
    }
  }
  showChart = () => {
    if (!this.state.metricName) {
      message.warning('Please choose metric name')
    } else {
      this.getWorkloadForecast()
    }
  }
  getOption = (item) => {
    return {
      grid: {
        containLabel: true,
        top: 25,
        right: 20,
        left: 5,
        bottom: 40,
        width: '100%'
      },
      legend: {
        type: 'scroll',
        orient: 'vertical',
        data: item.legendData,
        width: '100%',
        left: 10,
        height: 40,
        top: 270,
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
        data: item.xdata
      },
      yAxis: {
        scale:true,
        type: 'value',
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
          let res = ''
          if (params[0].value || params[0].value === 0) {
            res = `${params[0].name} <br/><span style="background: ${params[0].color}; height:10px; width: 10px; border-radius: 50%;display: inline-block;margin-right:10px;"></span> ${params[0].seriesName} ：${params[0].value}<br/>`
          } else {
            res = `${params[1].name} <br/><span style="background: ${params[1].color}; height:10px; width: 10px; border-radius: 50%;display: inline-block;margin-right:10px;"></span> ${params[1].seriesName} ：${params[1].value}<br/>`
          }
          return res
        }
      },
      series: item.seriesData
    }
  }
  displayRender = (label) => {
    return label[label.length - 1];
  }

  onSearch = (value) => {
    if (value) {
      this.setState({
        metricName: value,
        newSelectValue: value
      })
    }
  };
  onSearchSel = (value) => {
    if (value) {
      this.setState({
        newSelValue: value,
        instanceName: value
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
  onBlurSelectSel = () => {
    const value = this.state.newSelValue
    if (value) {
      this.changeSelVal(value)
      this.setState({ newSelValue: ''})
    }
  }
  handleChange = (e) => {
    this.setState({
      newSelectValue: e,
      metricName: e
    })
  }
  changeSelVal = (e) => {
    this.setState({
      newSelValue: e,
      instanceName: e
    })
  }
  handleInputChange = (e) => {
    this.setState({labels: e.target.value})
  }
  handleNumChange = (e) => {
    this.setState({warningHours: e})
  }
  handleUpperChange = (e) => {
    this.setState({upper: e})
  }
  handleLowerChange = (e) => {
    this.setState({lower: e})
  }
  handleRefresh(){
    this.setState({
      labels: '',
      warningHours: 1,
      upper: '',
      lower: '',
    },()=>{
      this.showChart()
    })
  }
  componentDidMount () {
    this.getSearchMetric()
    window.addEventListener('keydown', (e) => {
      if (e.key === 'Control') {
      this.setState({
        isPressCtrl: true
      })
      }
    });
    window.addEventListener('keyup', (e) => {
      if (e.key === 'Control') {
      this.setState({
        isPressCtrl: false
      })
      }
    });
  }
  render () {
    return (
      <div className="contentWrap riskanalysis">
        <Card title="Risk Analysis" extra={<ReloadOutlined className="more_link " onClick={() => { this.handleRefresh() }}  style={{height: "100%"}} />}>
          <Row className="analysis" style={{ width: '90%'}} justify="space-between">
            <Col>
            <span>Instance: </span>
            <Select value={this.state.instanceName} placeholder="Instance List" onChange={(val) => { this.changeSelVal(val) }} showSearch
                     onSearch={(e) => { this.onSearchSel(e) }} onBlur={() => this.onBlurSelectSel()} optionFilterProp="children" filterOption={(input, option) =>
                      option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 200 }}>
                    {
                      this.state.optionsSel.map(item => {
                        return (
                          <Option value={item} key={item}>{item}</Option>
                        )
                      })
                    }
                  </Select>
            </Col>
            <Col>
            <span>Metric: </span>
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
            </Col>
            <Col>
            <label>Labels: </label><Input placeholder="Please separate multiple labels with ','" onChange={(e) => this.handleInputChange(e)} value={this.state.labels} style={{ width:300 }}/>
            </Col>
            <Col>
            <label>Hours: </label><InputNumber min={1} max={720} defaultValue={1} onChange={(e) => this.handleNumChange(e)} value={this.state.warningHours} style={{ width:60 }}/>
            </Col>
            <Col>
            <label>Upper: </label><InputNumber min={0} onChange={(e) => this.handleUpperChange(e)}  value={this.state.upper} style={{ width:60 }}/>
            </Col>
            <Col>
            <label>Lower: </label><InputNumber min={0} onChange={(e) => this.handleLowerChange(e)}  value={this.state.lower} style={{ width:60 }}/>
            </Col>
            <Col>
              <Button type="primary" onClick={() => this.showChart()}>Analysis</Button>
            </Col>
          </Row>
          <div style={{ width: '100%',minHeight: 730, height: 'auto', textAlign: 'center' }}>
            {
              this.state.showType === 0 ? <Spin style={{ margin: '250px auto' }} /> : this.state.showType === 1 ?
              this.state.allDataRegular.map((item,index) => {
                return (
                  <>
                    <ReactEcharts
                      ref={(e) => {
                        this.echartsElement = e
                      }}
                      option={this.getOption(item)}
                      style={{ minHeight: '100%' }}
                      lazyUpdate={true}
                    >
                    </ReactEcharts>
                    <div style={{color:'#272727',textAlign:'left',fontWeight:500}}><span>Conclusion Description: </span><span>{item.warning}</span></div>
                  </>
                )
              }) : this.state.showType === 2 ? <Empty style={{ margin: '250px auto' }} description={''} /> : ''
            }
          </div>
        </Card>
      </div>
    )
  }
}