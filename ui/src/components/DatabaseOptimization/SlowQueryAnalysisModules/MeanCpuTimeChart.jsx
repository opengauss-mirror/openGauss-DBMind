import React, { Component } from "react";
import { Card } from "antd";
import ReactEcharts from "echarts-for-react";

export default class MeanCpuTimeChart extends Component {
  constructor(props) {
    super(props);
    this.state = {
      chartData: null,
    };
  }
  getOption() {
    return {
      series: [
        {
          type: "gauge",

          radius: "80%",
          min: 0,
          max: this.state.chartData * 6,
          splitNumber: 6,
          axisLine: {
            roundCap: true,
            lineStyle: {
              width: 18,
            },
          },
          progress: {
            show: true,
            roundCap: true,
            width: 8,
            itemStyle: {
              color: "#9185F0",
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
              return value.toFixed(1);
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
              color: "#9185F0",
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
            lineHeight: 40,
            borderRadius: 30,
            offsetCenter: [0, 0],
            formatter: function (value) {
              return "{value|" + value + "}";
            },
            rich: {
              value: {
                fontSize: 16,
                color: "#272727",
              },
            },
          },

          data: [
            {
              value: this.state.chartData,
              name: "ms",
            },
          ],
        },
      ],
    };
  }
  UNSAFE_componentWillReceiveProps(nextProps) {
    this.setState({ chartData: nextProps?.meanCpuTime?.toFixed(2) });
  }
  render() {
    return (
      <div>
        <Card title="Mean CPU Time">
          <ReactEcharts
            ref={(e) => {
              this.echartsElement = e;
            }}
            option={this.getOption()}
            style={{ height: "200px" }}
            lazyUpdate={true}
          ></ReactEcharts>
        </Card>
      </div>
    );
  }
}
