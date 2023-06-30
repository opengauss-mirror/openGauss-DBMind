import React, { Component } from 'react';
import { Card } from 'antd';
import ReactEcharts from 'echarts-for-react';

export default class MeanBufferHitRateChart extends Component {
  constructor(props) {
    super(props)
    this.state = {
      chartData: null
    }
  }
  getOption () {
    return {
      series: [
        {
          
          type: "gauge",
         
          radius: "80%",
          min: 0,
          max: this.state.chartData * 6,
          splitNumber: 5,
          axisLine: {
            roundCap:true,
            lineStyle: {
              width: 18,
            },
          },
          progress: {
            show: true,
            roundCap:true,
            width: 8,
            itemStyle: {
              color: "#5990FD",
            },
          },
          axisTick: {
            show: false,
          },
          splitLine: {
            show: false,
          },
          axisLabel: {
            distance: -5,
            color: "#999",
            fontSize: 12,
            formatter: function (value) {
              return value.toFixed(0);
            },
          },
          axisLine: {
            lineStyle: {
              width: 8,
            },
          },
          anchor: {
            show: true,
            showAbove: true,
            size: 25,
            icon: "none",
            itemStyle: {
              borderWidth: 10,
            },
          },
          pointer: {
            showAbove: false,
            width: 6,
            itemStyle: {
              color: "#5990FD",
            },
          },
          title: {
            show: false,
          },
          detail: {
            backgroundColor: "#f5f5f5",
            valueAnimation: true,
            fontSize: 16,
            width: 30,
            height: 40,
            lineHeight:40,
            borderRadius: 30,
            offsetCenter: [0, 0],
            formatter: function (value) {
              return "{value|" + value + "}{unit|%}";
            },
            rich: {
              value: {
                fontSize: 16,
               
                color: '#272727'
              },
              unit: {
                fontSize: 12,
                color: '#272727',
              
              }
            }
          },
     
          data: [
              {
                value: this.state.chartData
              }
            ]
        }
      ]
    };
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    this.setState({
      chartData: nextProps?.meanBufferHitRate?.toFixed(2)
    })
  }
  render () {
    return (
      
        <Card title="Mean Buffer Hit Rate for Slow Query">
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

    )
  }
}
