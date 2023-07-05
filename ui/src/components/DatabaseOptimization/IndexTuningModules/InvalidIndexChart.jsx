import React, { Component } from "react";
import { Card } from "antd";
import * as echarts from "echarts";
import PropTypes from "prop-types";

export default class InvalidIndexChart extends Component {
  static propTypes = {
    invalidIndexData: PropTypes.array.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      chartVal: [],
    };
  }
 
  initChart() {
    let myChart = echarts.init(document.getElementById("indexTuningChart3"));
    let option = {
      tooltip: {
        trigger: "item",
      },

      legend: {
        itemHeight: 5,
        itemWidth: 5,
        left: "center",
        top: "-2%",
      },
      label: {
        show: true,
      },
     
      graphic: {
        // 将图片定位到最下方的中间：
        type: "text",
        left: "center", // 水平定位到中间
        top: "44%",
        style: {
          text:this.state.chartVal[0].value,
          textAlign: "center",
          fill: '#9185F0',
         font: 'bolder 14px "Microsoft YaHei", sans-serif'
        },
      },
      series: [
        {
          name: "",
          type: "pie",
          radius: ["50%", "70%"],
          avoidLabelOverlap: false,
          left: "0%",
          bottom: 5,
          itemStyle: {
            borderColor: "#fff",
            borderWidth: 0,
          },
          label: {
            show: false,
            position: "bottom",
            fontSize: "16px",
            color: "auto",
          },
          emphasis: {
            itemStyle: {
              shadowBlur: 10,
              shadowOffsetX: 0,
              shadowColor: "rgba(0, 0, 0,0.5)",
            },
          },
          labelLine: {
            show: false,
          },
          color: ["#9185F0", "#5470c6"],
          data: this.state.chartVal,
        },
        {
          type: "pie",
          clockWise: false, //顺时加载
          hoverAnimation: false, //鼠标移入变大
          center: ["50%", "44%"],
          radius: ["89%", "89%"],
          top: "10%",
          label: {
            normal: {
              show: false,
            },
          },
          data: [
            {
              tooltip: {
                trigger: "none",
              },
              value: 1,
              name: "",
              itemStyle: {
                normal: {
                  borderWidth: 1,
                  borderColor: "#5990fdff ",
                  opacity: 0.3,
                },
              },
            },
          ],
        },
      ],
    };
    myChart.setOption(option);
    myChart.resize();
  }
  UNSAFE_componentWillReceiveProps(nextProps) {
    this.setState({ chartVal: nextProps.invalidIndexData }, () => {
      this.initChart();
    });
  }
  render() {
    return (
      <div>
        <Card title="Valid Index">
          <div
            id="indexTuningChart3"
            style={{ width: "100%", height: 200 }}
          ></div>
        </Card>
      </div>
    );
  }
}
