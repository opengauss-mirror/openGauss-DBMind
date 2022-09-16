import React, {Component} from 'react';
import {Card} from 'antd';
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
          type: 'gauge',
          startAngle: 180,
          endAngle: 0,
          min: 0,
          max: 100,
          splitNumber: 10,
          radius: '140%',
          center: ['50%', '70%'],
          itemStyle: {
            color: '#1890ff',
          },
          progress: {
            show: true,
            roundCap: true,
            width: 8
          },
          pointer: {
            icon: 'path://M2090.36389,615.30999 L2090.36389,615.30999 C2091.48372,615.30999 2092.40383,616.194028 2092.44859,617.312956 L2096.90698,728.755929 C2097.05155,732.369577 2094.2393,735.416212 2090.62566,735.56078 C2090.53845,735.564269 2090.45117,735.566014 2090.36389,735.566014 L2090.36389,735.566014 C2086.74736,735.566014 2083.81557,732.63423 2083.81557,729.017692 C2083.81557,728.930412 2083.81732,728.84314 2083.82081,728.755929 L2088.2792,617.312956 C2088.32396,616.194028 2089.24407,615.30999 2090.36389,615.30999 Z',
            length: '75%',
            width: 8,
            offsetCenter: [0, '5%']
          },
          axisLine: {
            roundCap: true,
            lineStyle: {
              width: 8
            }
          },
          axisTick: {
            splitNumber: 2,
            distance: 5,
            lineStyle: {
              width: 1,
              color: '#999'
            }
          },
          splitLine: {
            length: 4,
            distance: 5,
            lineStyle: {
              width: 2,
              color: '#999'
            }
          },
          axisLabel: {
            distance: 15,
            color: '#999',
            fontSize: 12
          },
          title: {
            show: false
          },
          detail: {
            backgroundColor: '#fff',
            borderColor: '#999',
            borderWidth: 2,
            width: '70%',
            lineHeight: 12,
            height: 14,
            borderRadius: 8,
            offsetCenter: [0, '30%'],
            valueAnimation: true,
            formatter: function (value) {
              return '{value|' + value + '}{unit|%}';
            },
            rich: {
              value: {
                fontSize: 20,
                lineHeight: 20,
                fontWeight: 'bolder',
                color: '#777'
              },
              unit: {
                fontSize: 20,
                color: '#999',
                padding: [10, 0, 10, 10]
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
      chartData: nextProps.meanBufferHitRate.toFixed(2)
    })
  }
  render () {
    return (
      <div className="mb-20" >
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
      </div>
    )
  }
}
