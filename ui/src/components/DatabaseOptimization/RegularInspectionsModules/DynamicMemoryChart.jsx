import React, { Component } from 'react';
import { Empty, Card, Row, Col } from 'antd';
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
    if (JSON.stringify(nextProps.dynamicMemoryChart) !== '{}') {
      let arrayData = [], colors = ['#5470c6', '#91cc75', '#fac858', '#007acc' ], legendData = []
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
          <Card title="Dynamic Memory" style={{ height: '100%'}} className="mb-20">
          <Row gutter={16}>
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
