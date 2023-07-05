import React, { Component } from "react";
import { Card, Empty, Select } from "antd";
import { ReloadOutlined } from "@ant-design/icons";
import ReactEcharts from "echarts-for-react";
import PropTypes from "prop-types";
import { formatTimestamp } from "../../../utils/function";

export default class SuggestionsChangeChart extends Component {
  static propTypes = {
    suggestions: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      seriesData: [],
      xdata: [],
      // yname: "",
      ifShow: true,
    };
  }
  getOption = () => {
    return {
      grid: {
        containLabel: true,
        width: "95%",
        height:"80%",
        left: "2%",
        top: "5%",
        // right: "0%",
      },
      xAxis: {
        axisLine: {
          lineStyle: {
            width: 0,
          },
        },
        splitLine: {
          //网格线
          show: true, //是否显示
          lineStyle: {
            //网格线样式
            color: "#F2F2F2", //网格线颜色
            width: 1, //网格线的加粗程度
            type: "solid", //网格线类型
          },
        },
        axisTick:{
          show:false
        },
        axisLabel: {
          textStyle: {
            color: '#4E4E4E',
            fontSize: "10",
          },
        },
        
        type: "category",
        data: this.state.xdata.map(function (str) {
          return str.replace(' ', '\n');
        })
      },
      yAxis: {
        type: "value",
        // name: this.state.yname,
        nameTextStyle: {
          padding: [0, 0, 0, 120],
          color: '#4E4E4E',
        },
        splitLine: {
          //网格线
          show: true, //是否显示
          lineStyle: {
            //网格线样式
            color: "#F2F2F2", //网格线颜色
            width: 1, //网格线的加粗程度
            type: "solid", //网格线类型
          },
        },
        nameGap: 10,
        axisLabel: {
          textStyle: {
            color: '#4E4E4E',
            fontSize: "10",
          },
        },
      },
      tooltip: {
        trigger: "axis",
      },
     

      series: this.state.seriesData,
    };
  };
  getChartData(data) {
    if (data.timestamps.length > 0) {
      // 处理X轴
      let formatTimeData = [];
      data.timestamps.forEach((ele) => {
        formatTimeData.push(formatTimestamp(ele));
      });
      // 处理Y轴数据
      let ydata = [];
      let seriesItem = {
        data: data.values,
        type: "line",
        smooth: true,
        name: "",
        symbol: "none",
        lineStyle:{
          color:'#5990FD'
        }
      };
      ydata.push(seriesItem);
      this.setState(
        () => ({
          ifShow: true,
          xdata: formatTimeData,
          seriesData: [...ydata],
          // yname: "suggestions",
        }),
        () => {
          this.getOption();
        }
      );
    } else {
      this.setState({ ifShow: false });
    }
  }
  UNSAFE_componentWillReceiveProps(nextProps) {
    this.getChartData(nextProps.suggestions);
  }
  render() {
    return (
      <div>
        <Card
          title="Suggestions"
          extra={
            <Select
           
              defaultValue="1.0h"
             
              disabled
              options={[
                {
                  value: "1.0h",
                  label: "1.0h",
                },
              ]}
            />
          }
        >
          {this.state.ifShow ? (
            <ReactEcharts
              ref={(e) => {
                this.echartsElement = e;
              }}
              option={this.getOption()}
              style={{ width: "100%", height: 200 }}
              lazyUpdate={true}
            ></ReactEcharts>
          ) : (
            <Empty
              description={this.state.ifShow}
              style={{ height: 200, paddingTop: 80 }}
            />
          )}
        </Card>
      </div>
    );
  }
}
