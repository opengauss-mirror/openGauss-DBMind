import React, { Component } from "react";
import { Card, Empty, Spin, Select } from "antd";
import PropTypes from "prop-types";
import ReactEcharts from "echarts-for-react";
import { formatTimestamp } from "../../../../utils/function";

export default class DynamicMemory extends Component {
  static propTypes = {
    dynamicMemory: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      showFlag: 0,
      allDataRegular: [],
    };
  }
  async getQps(data) {
    let arrayData = [], colors = ["#5990FD", "#2CA768",'#EEBA18','#5890FD'];
    Object.keys(data).forEach(function (key, i, v) {
      // 处理X轴
      let formatTimeData = [];
      data[key].timestamps.forEach((ele) => {
        formatTimeData.push(formatTimestamp(ele));
      });
      // 处理Y轴
      let seriesItem = {
        data: data[key].data,
        type: "line",
        smooth: true,
        name: key,
        symbol: "circle",
        itemStyle: {
          normal: {
            color: colors[i % 4],
            lineStyle: {
              width: 1,
            },
          },

        },
        
      };
      let arr = [];
      Object.keys(data[key].statistic).forEach((okey) => {
        arr.push([okey, data[key].statistic[okey]].join(":"));
      });
      let param = {
        xdata: formatTimeData,
        seriesData: seriesItem,
        yname: key + " ( " + arr.join(",") + " )",
      };
      arrayData.push(param);
    });
    this.setState(
      () => ({
        showFlag: 0,
        allDataRegular: [...arrayData],
      }),
      () => {
        this.echartsElement.resize();
      }
    );
  }
  getOption = (item) => {
    return {
      title: {
        text: item.yname,
        left: "center",
        textStyle: {
          fontSize: "14",
          fontFamily: "Arial",
          fontWeight: "Bold",
        },
      },
      grid: {
        top: "15%",
        left: "3%",
        right: "4%",
        bottom: "2%",
        containLabel: true,
      },
      xAxis: {
        type: "category",
        boundaryGap: false,
        splitLine: {
          //网格线
          show: true, //是否显示
          lineStyle: {
            //网格线样式
            color: "#F2F2F2", //网格线颜色
            width: 1, //网格线的加粗程度
            type: "dashed", //网格线类型
          },
        },
        axisLine: {
          show: true,
          lineStyle: {
            color: "#939393",
            width: 1,
            type: "solid",
          },
        },
        axisLabel: {
          show: true,
          margin: 10,
          textStyle: {
            color: "#4D5964",
            fontSize: 11,
            fontFamily: "Arial",
            fontWeight: "normal",
          },
        },
        data: item.xdata.map(function (str) {
          return str.replace(" ", "\n");
        }),
      },
      yAxis: {
        min: 0,
        splitLine: {
          //网格线
          show: true, //是否显示
          lineStyle: {
            //网格线样式
            color: "#F2F2F2", //网格线颜色
            width: 1, //网格线的加粗程度
            type: "dashed", //网格线类型
          },
        },
        ayisLine: {
          show: true,
          lineStyle: {
            color: "#939393",
            width: 1,
            type: "solid",
          },
        },
        axisLabel: {
          margin: 10,

          show: true,
          textStyle: {
            color: "#4D5964",
            fontSize: 11,
            fontFamily: "Arial",
            fontWeight: "normal",
            align: "right",
          },
        },
        type: "value",
      },
      tooltip: {
        trigger: "axis",
        textStyle: {
          align: "left",
        },
      },
      series: item.seriesData,
    };
  };
  UNSAFE_componentWillReceiveProps(nextProps) {
    if (JSON.stringify(nextProps.dynamicMemory) !== "{}") {
      this.getQps(nextProps.dynamicMemory);
    } else {
      this.setState({ showFlag: 1 });
    }
  }
  render() {
    return (
      <div>
        <Card title="Dynamic Memory" style={{ height: "100%" }}>
          <Select
            className="dynamicSelect dailySelect"
            defaultValue="24.0h"
            style={{
              width: 76,
              height: 26,
              float: "right",
            }}
            disabled
            options={[
              {
                value: "24.0h",
                label: "24.0h",
              },
            ]}
          />
          {this.state.showFlag === 0 ? (
            this.state.allDataRegular.map((item) => {
              return (
                <>

                  <ReactEcharts
                    ref={(e) => {
                      this.echartsElement = e;
                    }}
                    option={this.getOption(item)}
                    style={{ width: "100%", height: 240 }}
                    lazyUpdate={true}
                  ></ReactEcharts>
                </>
              );
            })
          ) : this.state.showFlag === 1 ? (
            <Empty description={false} style={{ paddingTop: 50 }} />
          ) : (
            <Spin style={{ margin: "100px auto" }} />
          )}
        </Card>
      </div>
    );
  }
}
