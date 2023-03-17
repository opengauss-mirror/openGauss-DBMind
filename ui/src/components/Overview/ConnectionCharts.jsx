import React, { Component } from 'react';
import { Empty, Row, Col, message } from 'antd';
import ReactEcharts from 'echarts-for-react';
import { formatTimestamp } from '../../utils/function';
import { getConnection } from '../../api/overview';
import db from '../../utils/storage';

export default class ConnectionCharts extends Component {
  constructor() {
    super()
    this.state = {
      ifShow: false,
      legendData:[],
      maxData:0,
      minData:0,
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
        text: flg === 'one' ? 'Max Connection' : 'Active Connection',
        subtext: flg === 'one' ? this.state.maxData : this.state.minData === 0 ? "0" : this.state.minData,
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
        data: flg === 'one' ? this.state.legendData[0][0].timestamps: this.state.legendData[1][0].timestamps,
        show:false,
      },
      yAxis: {
        type: 'value',
         show:false,
      },
      series: [
        {
          data: flg === 'one' ?  this.state.legendData[0][0].values : this.state.legendData[1][0].values,
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
          yAxis: 1330, 
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
  async getConnection1 () {
    let param = {
      instance:db.ss.get('Instance_value'),
      label:'gaussdb_total_connection'
    }
    const { success, data, msg }= await getConnection(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getConnection2 () {
    let param = {
      instance:db.ss.get('Instance_value'),
      label:'gaussdb_active_connection'
    }
    const { success, data, msg }= await getConnection(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  getConnectionAll(){
    Promise.all([
      this.getConnection1(),
      this.getConnection2()
    ]).then((result)=>{
      if(result[0]){
        let data = [result[0],result[1]]
        let max = result[0][0].values.length ? result[0][0].values[result[0][0].values.length-1] : "0"
        let min = result[1][0].values.length ? result[1][0].values[result[1][0].values.length-1] : "0"
        this.setState(() => ({
          ifShow: true,
          legendData: data,
          maxData: max,
          minData: min,
        }))
      } else {
        this.setState({ifShow: 0})
      }
    }).catch((error) => {
      console.log('error', error)
    })
  }
  componentDidMount () {
    this.getConnectionAll()
  }
  render () {
    return (
      <div >
        {this.state.ifShow ?
                  <Row>
                  <Col className="gutter-row" span={12}>
                  <ReactEcharts
                    ref={(e) => {
                      this.echartsElement = e
                    }}
                    option={this.getOption('one')}
                    style={{ width: '100%', height: 90 }}
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
