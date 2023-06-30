import React, { Component } from "react";
import { Card } from "antd";
import ReactEcharts from "echarts-for-react";

export default class ImprovementRateChart extends Component {
  constructor(props) {
    super(props);
    this.state = {
      chartVal: "",
    };
  }
  getOption() {
    return {
      series: [
        {
          type: 'gauge',
          radius:"70%",
          startAngle: 90,
          endAngle: -270,
          pointer: {
            show: false
          },
          progress: {
            show: true,
            overlap: false,
            roundCap: true,
            clip: false,
            itemStyle: {
              borderWidth: 1,
              borderColor: '#5990FD',
              color:'#5990FD'
            }
          },
          axisLine: {
            lineStyle: {
              width: 15
            }
          },
          splitLine: {
            show: false,
            distance: 0,
            length: 10
          },
          axisTick: {
            show: false
          },
          axisLabel: {
            show: false,
            distance: 50
          },
          data: [{
            value: this.state.chartVal,
            detail: {
              valueAnimation: true,
              offsetCenter: ['0%', '0%']
            }
          }],
          title: {
            fontSize: 14,
            show:false
          },
          detail: {
            width: 50,
            height: 14,
            fontSize: 14,
            color: 'inherit',
            formatter: '{value}%'
          }
        },
           {
              type: 'pie',
              clockWise: false, //顺时加载
              hoverAnimation: false, //鼠标移入变大
              center: ['50%', '45%'],
              radius: ['90%', '90%'],
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
  UNSAFE_componentWillReceiveProps(props) {
    this.props = props;
    this.setState({ chartVal: props.promoteSqlRate.toFixed(2) });
  }
  render() {
    return (
      <div>
        <Card title="Improvement Rate">
          <ReactEcharts
            ref={(e) => {
              this.echartsElement = e;
            }}
            style={{ width: "100%", height: 200 }}
            option={this.getOption()}
            lazyUpdate={true}
          ></ReactEcharts>
        </Card>
      </div>
    );
  }
}
