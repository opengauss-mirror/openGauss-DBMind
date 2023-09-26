import React, { Component } from 'react';
import {Row, Col, Empty, message } from 'antd';
import ReactEcharts from 'echarts-for-react';
import { getResponseTime } from '../../api/overview';
import db from '../../utils/storage';

export default class TpsCharts extends Component {
  constructor() {
    super()
    this.state = {
      chartData: [],
      showFlag: 0,
      maxData:0,
    }
  }

  getOption = (flg) => {
    return {
      grid:{
        x:85,
        x2:25,
        top:'center',
        y2:0
      },
      title: {
        show: true,
        text: 'Tps',
        subtext: this.state.maxData,
        textStyle: {    // 标题样式
        color: '#737a80',    //字体颜色
        fontSize: 14,    //字体大小
        fontWeight: '400',    //字体粗细
      },
        subtextStyle: {    // 副标题样式
        color: '#272727', 
        fontWeight:'Bold',
        fontSize:18,
        lineHeight:18,
        },
        left: '5%',
        top:'20%'
      },
      xAxis: {
        type: 'category',
        data: this.state.chartData[0].timestamps,
        show:false,
      },
      yAxis: {
        type: 'value',
         show:false,
      },
      series: [
        {
          data: this.state.chartData[0].values,
          type: 'line',
          smooth: true,
          symbol:"none",
              itemStyle: {
            normal: {
                lineStyle: {
                    color:'#5990fdff'
                }
            }
        },
        markPoint: {
        data: [{
          yAxis: this.state.maxData, 
          x: '96%' 
        }],
        symbol: 'circle',
        symbolSize: 7,
        itemStyle: {
          color:'#5990fdff',
        }
      },
        }
      ],
    }
  }
  
  async getTpsTime () {
    let param = {
      instance:db.ss.get('Instance_value'),
      label:'gaussdb_qps_by_instance'
    }
    const { success, data, msg }= await getResponseTime(param)
    if (success) {
      this.setState(() => ({
        showFlag: 1,
        chartData: data,
        maxData: data[0].values.length ? data[0].values[data[0].values.length-1] : "0"
      }))
    } else {
      message.error(msg)
      this.setState({showFlag: 0})
    }
  }
  componentDidMount () {
    this.getTpsTime()
  }
  render () {
    return (
      <div>
          {this.state.showFlag ?
          <Row>
          <Col className="gutter-row" span={24}>
            <ReactEcharts
              ref={(e) => {
                this.echartsElement = e
              }}
              option={this.getOption()}
              style={{ width: '100%', height: 90 }}
              lazyUpdate={true}
            >
            </ReactEcharts>
          </Col>
        </Row>
          : <Empty description={false} />}
      </div>
    )
  }
}
