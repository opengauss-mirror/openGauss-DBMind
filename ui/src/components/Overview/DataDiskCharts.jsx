import React, { Component } from 'react';
import {Row, Col, message, Card } from 'antd';
import ReactEcharts from 'echarts-for-react';
import { getDataDisk } from '../../api/overview';
import db from '../../utils/storage';

export default class DataDiskCharts extends Component {
  constructor() {
    super()
    this.state = {
      chartData: [],
      instance:''
    }
  }
  async getDataDisk () {
    const { success, data, msg }= await getDataDisk(db.ss.get('Instance_value'))
    if (success) {
      let obj = data[Object.keys(data)]
      if(obj.tilt_rate > db.ss.get('tilt_rate_Max')){
        db.ss.set('tilt_rate_Max', obj.tilt_rate)
      } else if(obj.tilt_rate < db.ss.get('tilt_rate_Min')){
        db.ss.set('tilt_rate_Min', obj.tilt_rate)
      }
      let allData = [[
        { value: obj.used_space.toFixed(2), name: 'Used Space' },
        { value: obj.free_space.toFixed(2), name: 'Free Space' },
      ],[
        { value: db.ss.get('tilt_rate_Max'), name: 'Max' },
        { value: db.ss.get('tilt_rate_Min'), name: 'Min' },
      ],[{totalLeft:obj.usage_rate,totalRight:obj.tilt_rate}]]
      this.setState(() => ({
        showFlag: 1,
        chartData: allData,
        instance:Object.keys(data)
      }))
    } else {
      message.error(msg)
    }
  }
  getOption = (flg) => {
    return {
      tooltip: {
        trigger: 'item'
      },
        legend: { // 对图形的解释部分
          orient: 'vertical',
          right: flg === 'one' ? '2%': '4%',
          top:'20%',
          y: 'center',
          icon: 'none',			// 添加
          formatter: (name) => {	// 添加
            //只接收一个参数，就是类目名称
              let value
              //使用name去放内容的数组中拿到对应的值
              (flg === 'one' ? this.state.chartData[0] : this.state.chartData[1]).forEach(item => {
                  if(item.name === name){
                      value = item.value
                  }
              })
              return flg === 'one' ? [
                `{name|${name}}`,
                `{value|${value+'GB'}}`
            ].join('\n') : [
              `{name|${name}}`,
              `{value|${value+'MB/s'}}`
            ].join('\n')
          },
          textStyle: {	// 添加
            rich:{
                name:{
                    fontSize:12,
                    color:"#737a80 ",
                    lineHeight:20
                },
                value:{
                    fontSize:16,
                    color:"#272727",
                    lineHeight:24,
                    fontWeight:'bold'
                }
            }
          }
        },
      title: {
        show: true,
        text:flg === 'one' ? 'Usage rate' : 'Tilt Rate',
        textStyle: {    // 标题样式
        color: '#272727',    //字体颜色
        fontSize: 14,    //字体大小
        fontWeight: '400',    //字体粗细
      },
        left: '26%',
        top:'84%'
      },
      series: [
        {
          type: 'pie',
          radius: ['50%', '65%'],
          center: ['36%', '40%'],
          avoidLabelOverlap: false,
          itemStyle: {
            borderRadius: 14,
            borderColor: '#fff',
            borderWidth: 0
          },
          label: {
              position: 'center',
              show: true,
              formatter:() => {
                  let str = (flg === 'one' ? (this.state.chartData[2][0].totalLeft*100+'%') : (this.state.chartData[2][0].totalRight+'MB/s'))
                  return str
              },
              color: '#5990fdff ',
              lineHeight: 16,
              fontSize: 22,
          },
          emphasis: {
            itemStyle: {
              shadowBlur: 10,
              shadowOffsetX: 0,
            }
          },
          labelLine: {
            show: false
          },
          color: ['#5990fdff', '#eeeeeeff'],
          data: flg === 'one' ? this.state.chartData[0] : this.state.chartData[1]
        },
                            {
                    type: 'pie',
                    clockWise: false, //顺时加载
                    hoverAnimation: false, //鼠标移入变大
                    center: ['36%', '40%'],
                    radius: ['77%', '77%'],
                    label: {
                        normal: {
                            show: false
                        }
                    },
                    data: [{
                        tooltip: {
                          trigger: 'none'
                        },
                        value: 1,
                        name: '',
                        itemStyle: {
                            normal: {
                                borderWidth: 1,
                                borderColor: '#5990fdff ',
                                opacity: 0.3
                            }
                        }
                    }]
                },
      ]
    };
  }
  componentDidMount () {
    if(db.ss.get('tilt_rate_Max') === null || db.ss.get('tilt_rate_Min') === null){
      db.ss.set('tilt_rate_Max', 0)
      db.ss.set('tilt_rate_Min', 0)
    }
    this.getDataDisk()
  }
  render () {
    return (
      <div>
        <Card title="Data Disk" className='instancename' style={{ height: 278}} extra={<span>{this.state.instance}</span>} >
        <Row>
            <Col className="gutter-row" span={12}>
              <ReactEcharts
                ref={(e) => {
                  this.echartsElement = e
                }}
                option={this.getOption('one')}
                style={{ width: '100%', height: 221 }}
                lazyUpdate={true}
              >
              </ReactEcharts>
            </Col>
            <Col className="gutter-row" span={12}>
              <ReactEcharts
                ref={(e) => {
                  this.echartsElement = e
                }}
                option={this.getOption('two')}
                style={{ width: '100%', height: 221 }}
                lazyUpdate={true}
              >
              </ReactEcharts>
            </Col>
          </Row>
        </Card>
      </div>
    )
  }
}
