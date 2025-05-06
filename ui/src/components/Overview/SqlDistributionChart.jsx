import React, { Component } from 'react';
import {Empty, message} from 'antd';
import ReactEcharts from 'echarts-for-react';
import { getDistribution } from '../../api/overview';
import db from '../../utils/storage';

export default class SqlDistributionChart extends Component {
  constructor() {
    super()
    this.state = {
      chartData: [],
      showFlag: 0,
      total:0
    }
  }
  getOption = () => {
    return {
      tooltip: {
        trigger: 'item',
        formatter: '{a} <br/>{b} : {c} ({d}%)'
      },
      
            legend: { // 对图形的解释部分
              orient: 'vertical',
              right: 50,
              top:'2%',
              icon: 'circle',	
                  itemHeight: 6,
        itemWidth: 6,
              // 添加
              formatter: (name) => {// 添加
                let total = 0
                let target
                let data = this.state.chartData
                for (let i = 0; i < data.length; i++) {
                  total += data[i].value
                  if (data[i].name === name) {
                    target = data[i].value
                  }
                }
                var arr = [
                  '{a|' + name + '}',
                  '{b|' + target + '}'
                ]
                return arr.join('  ')
              },
              textStyle: {	// 添加
              color:'#737a80',
                padding: [0, 0, 25, 5],
                rich: {
                  a: {
                    fontSize: 12,
                    width: 60,
                    verticalAlign: 'bottom',
                    lineHeight:37,
                  },
                  b: {
                    fontSize: 12,
                    width: 60,
                    verticalAlign: 'bottom',
                    lineHeight:37,
                  },
                }
              }
            },
      series: [
        {
          name: 'Radius Mode',
          type: 'pie',
          radius: ['50%', '78%'],
          center: ['30%', '46%'],
          itemStyle: {
            borderRadius: 5
          },
          label: {
            position: 'center',
            show: true,
            formatter:() => {
                let str = [                         
                  `{value|${this.state.total}}`,
                `{name|Total}`].join('\n')
                return str
            },
            color: ['#cad7efff','#5990fdff','#ffbb33ff','#2da769ff','#e54545ff' ],
            lineHeight: 16,
            fontSize: 22,
            textStyle: {	// 添加
              rich:{
                  name:{
                      fontSize:14,
                      color:"#272727 ",
                      lineHeight:20
                  },
                  value:{
                      fontSize:18,
                      color:"#272727",
                      lineHeight:24,
                      fontWeight:'bold'
                  }
              }
}
          },
          color: ['#5990fdff','#ffbb33ff','#2da769ff','#e54545ff','#cad7efff'],
          emphasis: {
            label: {
              show: true
            }
          },
          data: this.state.chartData
        }
      ]
    };
  }
  async getDistribution1 () {
    let param = {
      instance:db.ss.get('Instance_value'),
      label:'pg_sql_count_select'
    }
    const { success, data, msg }= await getDistribution(param)
    if (success) {
      return data
    }
  }
  async getDistribution2 () {
    let param = {
      instance:db.ss.get('Instance_value'),
      label:'pg_sql_count_update'
    }
    const { success, data, msg }= await getDistribution(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getDistribution3 () {
    let param = {
      instance:db.ss.get('Instance_value'),
      label:'pg_sql_count_delete'
    }
    const { success, data, msg }= await getDistribution(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getDistribution4 () {
    let param = {
      instance:db.ss.get('Instance_value'),
      label:'pg_sql_count_insert'
    }
    const { success, data, msg }= await getDistribution(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getDistribution5 () {
    let param = {
      instance:db.ss.get('Instance_value'),
      label:'pg_sql_count_dcl'
    }
    const { success, data, msg }= await getDistribution(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getDistribution6 () {
    let param = {
      instance:db.ss.get('Instance_value'),
      label:'pg_sql_count_ddl'
    }
    const { success, data, msg }= await getDistribution(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  getDistributionAll(){
    Promise.all([
      this.getDistribution1(),
      this.getDistribution2(),
      this.getDistribution3(),
      this.getDistribution4(),
      this.getDistribution5(),
      this.getDistribution6(),
    ]).then((result)=>{
      if(result[0]){
        let dataObj = [
          { value: result[0][0].values.length ? result[0][0].values[0] : 0, name: 'Select' },
          { value: result[1][0].values.length ? result[1][0].values[0] : 0, name: 'Update' },
          { value: result[2][0].values.length ? result[2][0].values[0] : 0, name: 'Delete' },
          { value: result[3][0].values.length ? result[3][0].values[0] : 0, name: 'Insert' },
          { value: result[4][0].values.length && result[5][0].values.length ? result[4][0].values[0]+result[5][0].values[0] : 0, name: 'Other' }
        ]
        
        this.setState(() => ({
          showFlag: 1,
          chartData: dataObj,
          total:result[0][0].values[0]+result[1][0].values[0]+result[2][0].values[0]+result[3][0].values[0]+(result[4][0].values.length && result[5][0].values.length ? result[4][0].values[0]+result[5][0].values[0] : 0)
        }))
      } else {
        this.setState({showFlag: 0})
      }
    }).catch((error) => {
      console.log('error', error)
    })
  }
  componentDidMount () {
    this.getDistributionAll()
  }
  render () {
    return (
      <div>
        {this.state.showFlag ?
              <ReactEcharts
              ref={(e) => {
                this.echartsElement = e
              }}
              option={this.getOption()}
              style={{ width: '100%', height: 221 }}
              lazyUpdate={true}
            >
            </ReactEcharts>
          : <Empty description={false} style={{paddingTop:50}}/>}
      </div>
    )
  }
}
