import React, { Component } from 'react';
import { Card, Row, Col, Spin } from 'antd';
import PropTypes from 'prop-types';
import ReactEcharts from 'echarts-for-react';
import { formatTimestamp } from '../../../../utils/function';

export default class DatabaseSizeChart extends Component {
  static propTypes={
    databaseSizeChart:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      seriesData: [],
      yname: '',
      xdata: [],
      ifShow: true,
      allDataRegular:[],
      legendData:[],
      onlyData:[],
    }
  }
  getOption = (item) => {
    return {
      title: {
        text: item.yname,
        left: 'left',
        textStyle:{
          fontSize: "14",
          fontFamily: "Arial",
          fontWeight: "Bold"
        }
      },
      grid: {
        top: "15%",
        left: "3%",
        right: "4%",
        bottom: "2%",
        containLabel: true,
      },
      legend: {
        data: item.legendData,
        right:22
      },
      xAxis: {
        type: 'category',
        boundaryGap: false,
        splitLine: {
          //网格线
          show: true, //是否显示
          lineStyle: {
            //网格线样式
            color: '#F2F2F2', //网格线颜色
            width: 1, //网格线的加粗程度
            type: 'dashed' //网格线类型
          }
        },
        axisLine: {
          show: true,
          lineStyle: {
            color: '#939393',
            width: 1,
            type: 'solid'
          }
        },
        axisLabel: {
          show: true,
          margin: 10,
          textStyle: {
            color: '#4D5964',
            fontSize: 11,
            fontFamily: 'Arial',
            fontWeight: 'normal'
          }
        },
        data: item.xdata.map(function (str) {
          return str.replace(' ', '\n');
        })
      },
      yAxis: {
        min: 0,
        splitLine: {
          //网格线
          show: true, //是否显示
          lineStyle: {
            //网格线样式
            color: '#F2F2F2', //网格线颜色
            width: 1, //网格线的加粗程度
            type: 'dashed' //网格线类型
          }
        },
        ayisLine: {
          show: true,
          lineStyle: {
            color: '#939393',
            width: 1,
            type: 'solid'
          }
        },
        axisLabel: {
          margin: 10,
          
          show: true,
          textStyle: {
            color: '#4D5964',
            fontSize: 11,
            fontFamily: 'Arial',
            fontWeight: 'normal',
            align: 'right'
          }
        },
        type:'value'
      },
      tooltip: {
        trigger: 'axis',
        textStyle: {
        align: 'left'
      }
    },
      
      series: item.seriesData
    }
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    if (JSON.stringify(nextProps.databaseSizeChart) !== '{}') {
      let arrayData = [], colors = ['#EC6F1A', '#EEBA18'], everyData = []
          // 处理X轴
          let formatTimeData = [];
          nextProps.databaseSizeChart.timestamps.forEach(ele => {
            formatTimeData.push(formatTimestamp(ele));
          });
          Object.keys(nextProps.databaseSizeChart.data).forEach(function (name, i, v) {
            let allData = [],seriesItem = {},legendData = []
            Object.keys(nextProps.databaseSizeChart.data[name]).forEach(function (data, i, v) {
              legendData.push(data);
              seriesItem = {
                data: nextProps.databaseSizeChart.data[name][data],
                type: 'line',
                smooth: true,
                name: data,
                symbol: 'circle',
                
                itemStyle: {
                  normal: {
                    color: colors[i],
                    lineStyle: {
                      width: 1,
                    },
                  },
                },
                
              }
              allData.push(seriesItem)
            })
            let param = {
              xdata:formatTimeData,
              seriesData:allData,
              yname:v[i],
              legendData:legendData,
            }
            arrayData.push(param);
            
          })
          if(arrayData.length%2 !== 0){
            everyData.push(arrayData[arrayData.length-1])
            arrayData.pop()
          }

      this.setState({
        ifShow: true,
        onlyData:everyData,
        allDataRegular: [...arrayData],
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
          <Card title="System Resource" style={{ height: '100%'}} className="mb-10">
          <Row gutter={10}>
          {this.state.ifShow ?  this.state.allDataRegular.map((item,index) => {
                return (
                  <>
                    <Col className="gutter-row mb-10" span={12}>
                      <ReactEcharts
                        className="systemBorder"
                        ref={(e) => {
                          this.echartsElement = e
                        }}
                        option={this.getOption(item)}
                        style={{ height: 240 }}
                        lazyUpdate={true}
                      >
                      </ReactEcharts>
                    </Col>
                  </>
                )
              })
           : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>}
          </Row>
          {this.state.onlyData.length ? 
              <Row gutter={10} className="mb-10">
                <Col className="gutter-row mb-10" span={24}>
                  <ReactEcharts
                    className="systemBorder"
                    ref={(e) => {
                      this.echartsElement = e
                    }}
                    option={this.getOption(this.state.onlyData[0])}
                    style={{ width: '100%', height: 240 }}
                    lazyUpdate={true}
                  >
                  </ReactEcharts>
                </Col>
              </Row> : <></>} 
        </Card>
      </div>
    )
  }
}
