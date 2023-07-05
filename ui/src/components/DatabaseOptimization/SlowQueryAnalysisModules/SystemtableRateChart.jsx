import React, { Component } from 'react';
import { Card } from 'antd';
import PropTypes from 'prop-types';
import ReactEcharts from 'echarts-for-react';

export default class SystemtableRateChart extends Component {
  static propTypes={
    sysInSlowQuery:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      chartData: []
    }
  }
  getOption = () => {
    return {
      tooltip: {
        trigger: 'item'
      },
      legend: {
        left: 'center',
        show: true,
        orient: 'horizontal'
      },
      label: {
        show: true
      },
      title: {
        show: true
      },
      series: [
        {
          name: 'Transaction State',
          type: 'pie',
          radius: ['60%', '75%'],
          avoidLabelOverlap: false,
          itemStyle: {
            borderColor: '#fff',
            borderWidth: 0
          },
          top:'10%',
          label: {
            show: false,
            position: 'bottom',
            fontSize: '16px',
            color: 'auto'
          },
          emphasis: {
            itemStyle: {
              shadowBlur: 10,
              shadowOffsetX: 0,
              shadowColor: 'rgba(0, 0, 0,0.5)'
            }
          },
          labelLine: {
            show: false
          },
          color: ['#9fe080', '#5470c6'],
          data: this.state.chartData
        },
        
        {
          type: 'pie',
          clockWise: false, //顺时加载
          hoverAnimation: false, //鼠标移入变大
          radius: ['87%', '87%'],
          top:'10%',
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
  UNSAFE_componentWillReceiveProps (nextProps) {
    let dataArr = []
    if(nextProps.sysInSlowQuery){
      Object.keys(nextProps.sysInSlowQuery).forEach(function (key) {
        let obj = {
          name: key,
          value: nextProps.sysInSlowQuery[key]
        }
        dataArr.push(obj)
      })
    }
    
    this.setState({chartData: dataArr})
  }
  render () {
    return (
      <div className="mb-10" >
        <Card title="System table Rate In Slow Query" style={{height:'278px'}}>
          <ReactEcharts
            ref={(e) => {
              this.echartsElement = e
            }}
            option={this.getOption()}
            style={{ width: '100%', height: 200 }}
            lazyUpdate={true}
          >
          </ReactEcharts>
        </Card>
      </div>
    )
  }
}
