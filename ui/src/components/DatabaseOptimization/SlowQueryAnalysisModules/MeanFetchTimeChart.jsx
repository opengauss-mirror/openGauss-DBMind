import React, { Component } from 'react';
import { Card } from 'antd';
import ReactEcharts from 'echarts-for-react';

export default class MeanFetchTimeChart extends Component {
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
          max: this.state.chartData * 3,
          splitNumber: 5,
          axisLine: {
            roundCap:true,
            lineStyle: {
              width: 8,
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
            show: true,
            offsetCenter: [0, "70%"],
            fontSize: 20,
            color: "#272727",
          },
          detail: {
            backgroundColor: "#f5f5f5",
            valueAnimation: true,
            fontSize: 16,
            width: 30,
            height: 40,
            borderRadius: 30,
            offsetCenter: [0, 0],
            lineHeight:40,
            formatter: function (value) {
              return "{value|" + value + "}";
            },
            rich: {
              value: {
                fontSize: 16,
               
                color: '#272727'
              },
             
            }
          },
        
          data: [
              {
                value: this.state.chartData,
                name: "ms",
              }
            ]
        }
      ]
    };
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    this.setState({
      chartData: nextProps?.meanFetchTime?.toFixed(2)
    })
  }
  render () {
    return (
      <div>
        <Card title="Mean Fetch Time">
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
