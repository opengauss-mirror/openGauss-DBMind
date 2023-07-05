import React, { Component } from "react";
import { Card, Tag, Row, Col, Spin } from "antd";
import PropTypes from "prop-types";
import ReactEcharts from "echarts-for-react";
import { formatTimestamp } from "../../../utils/function";

export default class TableSizeChart extends Component {
  static propTypes = {
    tableSizeChart: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      seriesData: [],
      yname: "",
      xdata: [],
      ifShow: true,
      allDataRegular: [],
      onlyData: [],
    };
  }
  getOption = (item) => {
    return {
      title: {
        text: item.yname,
        left: "left",
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
      legend: {
        data: item.legendData,
        right: 22,
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
    if (JSON.stringify(nextProps.tableSizeChart) !== "{}") {
      let colors = ["#2CA768", "#5890FD"],
        instanceAllData = [],
        everyData = [];
      Object.keys(nextProps.tableSizeChart.data).forEach(function (key, i, v) {
        // 处理X轴
        let formatTimeData = [];
        nextProps.tableSizeChart.timestamp.forEach((ele) => {
          formatTimeData.push(formatTimestamp(ele));
        });
        let arrayData = [];
        Object.keys(nextProps.tableSizeChart.data[key]).forEach(function (
          name,
          i,
          v
        ) {
          Object.keys(nextProps.tableSizeChart.data[key][name]).forEach(
            function (tData, i, v) {
              let allData = [],
                seriesItem = {},
                legendData = [];
              Object.keys(
                nextProps.tableSizeChart.data[key][name][tData]
              ).forEach(function (data, i, v) {
                legendData.push(data);
                seriesItem = {
                  data: nextProps.tableSizeChart.data[key][name][tData][data],
                  type: "line",
                  smooth: true,
                  name: data,
                  symbol: "circle",
                  symbolSize: 3,
                 
                  itemStyle: {
                    normal: {
                      color: colors[i],
                      lineStyle: {
                        width: 1,
                      },
                    },
                  },
                  
                };
                allData.push(seriesItem);
              });
              let param = {
                xdata: formatTimeData,
                seriesData: allData,
                yname: `${key}-${name}-${v[i]}`,
                legendData: legendData,
              };
              arrayData.push(param);
            }
          );
        });
        if (arrayData.length % 2 !== 0) {
          everyData.push(arrayData[arrayData.length - 1]);
          arrayData.pop();
        } else {
          everyData.push("");
        }
        arrayData.push(key);
        instanceAllData.push(arrayData);
      });
      this.setState({
        ifShow: true,
        onlyData: everyData,
        allDataRegular: [...instanceAllData],
      });
    } else {
      this.setState({
        ifShow: false,
      });
    }
  }
  render() {
    return (
      <div>
        <Card
          title="Instance Table Size"
          style={{ height: "100%" }}
          className="mb-10"
        >
          <Row gutter={10} className="mb-10">
            {this.state.ifShow ? (
              this.state.allDataRegular.map((item, index) => {
                return (
                  <>
                    {item.length ? (
                      item.map((oitem, oindex) => {
                        return oitem?.constructor === Object ? (
                          <Col className="gutter-row mb-10" span={12}>
                            <ReactEcharts
                              className="systemBorder"
                              ref={(e) => {
                                this.echartsElement = e;
                              }}
                              option={this.getOption(oitem)}
                              style={{ height: 250 }}
                              lazyUpdate={true}
                            ></ReactEcharts>
                          </Col>
                        ) : (
                          <></>
                        );
                      })
                    ) : (
                      <></>
                    )}
                    {this.state.onlyData[index] ? (
                      <Col className="gutter-row mb-10" span={24}>
                        <ReactEcharts
                          className="systemBorder"
                          ref={(e) => {
                            this.echartsElement = e;
                          }}
                          option={this.getOption(this.state.onlyData[index])}
                          style={{ width: "100%", height: 250 }}
                          lazyUpdate={true}
                        ></ReactEcharts>
                      </Col>
                    ) : (
                      <></>
                    )}
                  </>
                );
              })
            ) : (
              <div style={{ textAlign: "center" }}>
                <Spin style={{ margin: "100px auto" }} />{" "}
              </div>
            )}
          </Row>
        </Card>
      </div>
    );
  }
}
