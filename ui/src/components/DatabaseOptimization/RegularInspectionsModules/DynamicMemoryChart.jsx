import React, { Component } from 'react';
import { Empty, Card, Row, Col } from 'antd';
import PropTypes from 'prop-types';
import ReactEcharts from 'echarts-for-react';
import { formatTimestamp } from '../../../utils/function';

export default class DynamicMemoryChart extends Component {
  static propTypes={
    dynamicMemoryChart:PropTypes.object.isRequired
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
    }
  }
  getOption = (item) => {
    return {
      title: {
        text: item.yname,
        left: 'center',
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
        data:this.state.legendData,
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
    if (JSON.stringify(nextProps.dynamicMemoryChart) !== '{}') {
      let arrayData = [],  colors = ["#2DA769", "#5990FD", "#9185F0", "#EC6F1A", "#F43146"], legendData = []
          // 处理X轴
          let formatTimeData = [];
          nextProps.dynamicMemoryChart.timestamps.forEach(ele => {
            formatTimeData.push(formatTimestamp(ele));
          });
          Object.keys(nextProps.dynamicMemoryChart.data).forEach(function (name, i, v) {
            let allData = [],seriesItem = {}
            Object.keys(nextProps.dynamicMemoryChart.data[name]).forEach(function (data, i, v) {
              legendData.push(data);
              seriesItem = {
                data: nextProps.dynamicMemoryChart.data[name][data],
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
              yname:v[i]
            }
            arrayData.push(param);
            
          })
      this.setState({
        ifShow: true,
        allDataRegular: [...arrayData],
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
          <Card title="Dynamic Memory" style={{ height: '100%'}} className="mb-10">
          <Row gutter={10}>
          {this.state.ifShow ?  this.state.allDataRegular.map((item,index) => {
                return (
                  <>
                    <Col className="gutter-row" span={24}>
                      <ReactEcharts
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
           : <Empty description={this.state.ifShow} style={{ height: 200, paddingTop: 50 }} />}
          </Row>
        </Card>
      </div>
    )
  }
}
