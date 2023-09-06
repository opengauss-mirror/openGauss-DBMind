import React, { Component } from 'react';
import {Row, Col, Empty } from 'antd';
import ReactEcharts from 'echarts-for-react';
import { getResponseTime } from '../../api/overview';
import db from '../../utils/storage';
import { commonMetricMethod } from '../../utils/function';

export default class ResponseTimeCharts extends Component {
  constructor() {
    super()
    this.state = {
      chartData: [],
      showFlag: 0,
      maxData:0,
      minData:0,
      param: {
        instance:db.ss.get('Instance_value')
      },
      metricData:['statement_responsetime_percentile_p95','statement_responsetime_percentile_p80']
    }
  }

  getOption = (flg) => {
    return {
      grid:{
        x:165,
        x2:25,
        top:'center',
        y2:0
      },
      title: {
        show: true,
        text: flg === 'one' ? 'p95' : 'p80',
        subtext: (flg === 'one' ? (this.state.maxData === 0 ? "0" : this.state.maxData) : this.state.minData === 0 ? "0" : this.state.minData) + 'ms',
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
        data: flg === 'one' ? this.state.chartData[0][0].timestamps: this.state.chartData[1][0].timestamps,
        show:false,
      },
      yAxis: {
        type: 'value',
         show:false,
      },
      series: [
        {
          data: flg === 'one' ?  this.state.chartData[0][0].values : this.state.chartData[1][0].values,
          type: 'line',
          smooth: true,
          symbol:"none",
              itemStyle: {
            normal: {
                lineStyle: {
                    color: flg === 'one' ? '#5990fdff' : '#9185f0ff '
                }
            }
        },
        markPoint: {
        data: [{
          yAxis:  flg === 'one' ? this.state.maxData : this.state.minData, 
          x: '98.5%' 
        }],
        symbol: 'circle',
        symbolSize: 7,
        itemStyle: {
          color: flg === 'one' ? '#5990fdff' : '#9185f0ff ',
        }
      },
        }
      ],
    }
  }
  getResponseTimeAll(){
    Promise.all([
      commonMetricMethod(this.state.param,{label:this.state.metricData[0]},getResponseTime),
      commonMetricMethod(this.state.param,{label:this.state.metricData[1]},getResponseTime)
    ]).then((result)=>{
      if(result[0]){
        let data = [result[0],result[1]]
        let max = result[0][0].values.length ? result[0][0].values[result[0][0].values.length-1] : "0"
        let min = result[1][0].values.length ? result[1][0].values[result[1][0].values.length-1] : "0"
        this.setState(() => ({
          showFlag: 1,
          chartData: data,
          maxData: max,
          minData: min,
        }))
      } else {
        this.setState({showFlag: 0})
      }
    }).catch((error) => {
      console.log('error', error)
    })
  }
  componentDidMount () {
    this.getResponseTimeAll()
  }
  render () {
    return (
      <div>
          {this.state.showFlag ?
          <Row>
            {this.state.metricData.map((item,index) => {
                return (
                  <Col className="gutter-row" span={12}>
                    <ReactEcharts
                      ref={(e) => {
                        this.echartsElement = e
                      }}
                      option={this.getOption(index ? 'two' : 'one')}
                      style={{ width: '100%', height: 90 }}
                      lazyUpdate={true}
                    >
                    </ReactEcharts>
                  </Col>
                )
              })
            }
        </Row>
          : <Empty description={false} />}
      </div>
    )
  }
}
