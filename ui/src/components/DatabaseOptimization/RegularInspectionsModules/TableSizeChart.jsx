import React, { Component } from 'react';
import { Card, Tag, Row, Col, Spin } from 'antd';
import PropTypes from 'prop-types';
import ReactEcharts from 'echarts-for-react';
import { formatTimestamp } from '../../../utils/function';

export default class TableSizeChart extends Component {
  static propTypes={
    tableSizeChart:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      seriesData: [],
      yname: '',
      xdata: [],
      ifShow: true,
      allDataRegular:[],
      onlyData:[],
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
        data: item.legendData,
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
    if (JSON.stringify(nextProps.tableSizeChart) !== '{}') {
      let arrayData = [], colors = ['#5470c6', '#91cc75', '#fac858', '#007acc' ], instanceAllData = [], everyData = []
      Object.keys(nextProps.tableSizeChart.data).forEach(function (key, i, v) {
          // 处理X轴
          let formatTimeData = []
          nextProps.tableSizeChart.timestamp.forEach(ele => {
            formatTimeData.push(formatTimestamp(ele));
          });
          Object.keys(nextProps.tableSizeChart.data[key]).forEach(function (name, i, v) {
            Object.keys(nextProps.tableSizeChart.data[key][name]).forEach(function (tData, i, v) {
              let allData = [],seriesItem = {}, legendData = []
              Object.keys(nextProps.tableSizeChart.data[key][name][tData]).forEach(function (data, i, v) {
                legendData.push(data);
                seriesItem = {
                  data: nextProps.tableSizeChart.data[key][name][tData][data],
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
                yname:name+'-'+v[i],
                legendData:legendData,
              }
              arrayData.push(param);
            })
          })
          if(arrayData.length%2 !== 0){
            everyData.push(arrayData[arrayData.length-1])
            arrayData.pop()
          } else {
            everyData.push('')
          }
          arrayData.push(key);
          instanceAllData.push(arrayData);
      })
      this.setState({
        ifShow: true,
        onlyData:everyData,
        allDataRegular: [...instanceAllData]
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
          <Card title="Instance Table Size" style={{ height: '100%'}} className="mb-20">
          <Row gutter={16}>
          {this.state.ifShow ?  this.state.allDataRegular.map((item,index) => {
                return (
                  <>
                  <p style={{ textAlign: 'left',width: '100%'}}><Tag style={{ lineHeight: 2.5,color: '#000000', backgroundColor: '#faad14' }}>{item[item.length-1]}</Tag></p>
                  {item.length ? item.map((oitem,oindex) => {
                    return (
                      (oitem?.constructor === Object) ? <Col className="gutter-row" span={12} >
                      <ReactEcharts
                        ref={(e) => {
                          this.echartsElement = e
                        }}
                        option={this.getOption(oitem)}
                        style={{ height: 240 }}
                        lazyUpdate={true}
                      >
                      </ReactEcharts>
                    </Col> :  <></>
                    )
                  }) : <></>}
                  { this.state.onlyData[index]? 
                  <Col className="gutter-row" span={24}>
                    <ReactEcharts
                      ref={(e) => {
                        this.echartsElement = e
                      }}
                      option={this.getOption(this.state.onlyData[index])}
                      style={{ width: '100%', height: 240 }}
                      lazyUpdate={true}
                    >
                    </ReactEcharts>
                  </Col> : <></>} 
                  </>
                )
              })
           : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>}
          </Row>
        </Card>
      </div>
    )
  }
}
