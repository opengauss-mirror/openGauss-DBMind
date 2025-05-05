import React, { Component } from 'react';
import { Card, Tag, Row, Col, Spin } from 'antd';
import PropTypes from 'prop-types';
import ReactEcharts from 'echarts-for-react';
import { formatTimestamp } from '../../../utils/function';

export default class SystemResourceChart extends Component {
  static propTypes={
    systemResourceChart:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      seriesData: [],
      yname: '',
      xdata: [],
      ifShow: true,
      allDataRegular:[],
      legendData:[]
    }
  }
  getOption = (item) => {
    return {
      title: {
        text: item.yname,
        left: 'center',
        textStyle:{
          color: '#314b71',
          fontSize: '12'
        }
      },
      grid: {
        containLabel: true,
        width: '100%',
        left: '0%',
        top: '15%',
        right: '0%'
      },
      legend: {
        data:this.state.legendData,
        x: 'right' 
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
        data: item.xdata
      },
      yAxis: {
        type: 'value',
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
        right: '0.3%',
        height: 15
      },
      series: item.seriesData
    }
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    if (JSON.stringify(nextProps.systemResourceChart) !== '{}') {
      let arrayData = [], colors = ['#5470c6', '#91cc75', '#fac858', '#007acc' ], instanceAllData = [], legendData = []
      Object.keys(nextProps.systemResourceChart.data).forEach(function (key, i, v) {
          // 处理X轴
          let formatTimeData = [];
          nextProps.systemResourceChart.timestamps.forEach(ele => {
            formatTimeData.push(formatTimestamp(ele));
          });
          Object.keys(nextProps.systemResourceChart.data[key]).forEach(function (name, i, v) {
            let allData = [],seriesItem = {}
            Object.keys(nextProps.systemResourceChart.data[key][name]).forEach(function (data, i, v) {
              legendData.push(data);
              seriesItem = {
                data: nextProps.systemResourceChart.data[key][name][data],
                type: 'line',
                smooth: true,
                name: data,
                symbol: 'none',
                color: colors[i],
              }
              allData.push(seriesItem)
            })
            let param = {
              xdata:formatTimeData,
              seriesData:allData,
              yname:v[i]
            }
            arrayData.push(param);
          })
          arrayData.push(key);
          instanceAllData.push(arrayData);
      })
      this.setState({
        ifShow: true,
        allDataRegular: [...instanceAllData],
        legendData:legendData
      })
    } else {
      this.setState({
        ifShow: false
      })
    }
  }
  render () {
    return (
      <div>
          <Card title="System Resource" style={{ height: '100%'}} className="mb-20">
          {this.state.ifShow ?  this.state.allDataRegular.map((item,index) => {
                return (
                  <>
                  <p style={{ textAlign: 'left' }}><Tag style={{ lineHeight: 2.5,color: '#000000', backgroundColor: '#faad14' }}>{item[4]}</Tag></p>
                  <Row gutter={16}>
                    <Col className="gutter-row" span={12}>
                      <ReactEcharts
                        ref={(e) => {
                          this.echartsElement = e
                        }}
                        option={this.getOption(item[0])}
                        style={{ height: 240 }}
                        lazyUpdate={true}
                      >
                      </ReactEcharts>
                    </Col>
                    <Col className="gutter-row" span={12}>
                      <ReactEcharts
                        ref={(e) => {
                          this.echartsElement = e
                        }}
                        option={this.getOption(item[1])}
                        style={{ height: 240 }}
                        lazyUpdate={true}
                      >
                      </ReactEcharts>
                    </Col>
                  </Row>
                  <Row gutter={16}>
                    <Col className="gutter-row" span={12}>
                      <ReactEcharts
                        ref={(e) => {
                          this.echartsElement = e
                        }}
                        option={this.getOption(item[2])}
                        style={{ width: '100%', height: 240 }}
                        lazyUpdate={true}
                      >
                      </ReactEcharts>
                    </Col>
                    <Col className="gutter-row" span={12}>
                      <ReactEcharts
                        ref={(e) => {
                          this.echartsElement = e
                        }}
                        option={this.getOption(item[3])}
                        style={{ width: '100%', height: 240 }}
                        lazyUpdate={true}
                      >
                      </ReactEcharts>
                    </Col>
                  </Row>
                  </>
                )
              })
           : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>}
        </Card>
      </div>
    )
  }
}
