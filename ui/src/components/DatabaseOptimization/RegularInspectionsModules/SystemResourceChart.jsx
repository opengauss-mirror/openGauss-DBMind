import React, { Component } from "react";
import { Card, Tag, Row, Col, Spin, Select } from "antd";
import PropTypes from "prop-types";
import ReactEcharts from "echarts-for-react";
import { formatTimestamp } from "../../../utils/function";

export default class SystemResourceChart extends Component {
  static propTypes = {
    systemResourceChart: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      seriesData: [],
      yname: "",
      xdata: [],
      ifShow: true,
      allDataRegular: [],
      legendData: [],
      selectOption: [],
      selectValues:0
    };
  }
  getOption = (item) => {
    return {
      title: {
        text: item.yname,
        left: "left",
        textStyle: {
          color: "#272727",
          fontSize: "14",
          fontFamily: "Arial",
          fontWeight: "Bold",
        },
      },
      tooltip: {
        trigger: "axis",
        axisPointer: {
          type: "cross",
          label: {
            backgroundColor: "#6a7985",
          },
        },
      },
      grid: {
        top: "25%",
        left: "3%",
        right: "4%",
        bottom: "2%",
        containLabel: true,
      },
      legend: {
        data: this.state.legendData,
        // x: "right",
        top: 30,
        right: 22,
      },
      xAxis: [
        {
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
      ],
      yAxis: [
        {
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
      ],

      series: item.seriesData,
    };
  };
  handleChange(e) {
    this.setState({
      selectValues:e
    })
  }
  dealSelwctOption() {
    let selectData=[]
    this.state.allDataRegular.forEach((item,index) => {
      selectData.push({
        value: index,
        label: item[4],
      });
    });
    this.setState({
    selectOption:selectData
    })
  }
  UNSAFE_componentWillReceiveProps(nextProps) {
    if (JSON.stringify(nextProps.systemResourceChart) !== "{}") {
      let colors = ["#2CA768", "#EC6F1A", "#EEBA18", "#5890FD"],
        instanceAllData = [],
        legendData = [];
      // 处理X轴
      let formatTimeData = [];
      nextProps.systemResourceChart.timestamps.forEach((ele) => {
        formatTimeData.push(formatTimestamp(ele));
      });
   
      Object.keys(nextProps.systemResourceChart.data).forEach(function (
        key,
        i,
        v
      ) {
        let arrayData = [];
        Object.keys(nextProps.systemResourceChart.data[key]).forEach(function (
          name,
          i,
          v
        ) {
          let allData = [],
            seriesItem = {};
          Object.keys(nextProps.systemResourceChart.data[key][name]).forEach(
            function (data, i, v) {
              legendData.push(data);
              seriesItem = {
                data: nextProps.systemResourceChart.data[key][name][data],
                type: "line",
                smooth: true,
                name: data,
                symbol: "circle",
              
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
            }
          );

          let param = {
            xdata: formatTimeData,
            seriesData: allData,
            yname: v[i],
          };
          arrayData.push(param);
        });
        arrayData.push(key);
        instanceAllData.push(arrayData);
      });
      this.setState(
        {
          ifShow: true,
          allDataRegular: [...instanceAllData],
          legendData: legendData,
        },
        () => {
          this.dealSelwctOption();
        }
      );
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
          title="System Resource"
          style={{ height: "100%" }}
          className="mb-10 systemRsource"
        >
          {this.state.ifShow && this.state.selectOption.length ? (
            <>
              <Select
                className="dynamicSelect systemNode"
                value={this.state.selectValues}
                style={{
                  width: 128,
                }}
                onChange={(e) => this.handleChange(e)}  
                options={this.state.selectOption}
              />
              <Row gutter={10}>
                <Col className="gutter-row mb-10" span={12}>
                  <Select
                    className="dynamicSelect"
                    value={this.state.allDataRegular[this.state.selectValues][4]}
                    disabled
                    style={{
                      width: 128,
                      float: "right",
                    }}
                    options={[
                      {
                        value: this.state.allDataRegular[this.state.selectValues][4],
                        label: this.state.allDataRegular[this.state.selectValues][4],
                      },
                    ]}
                  />
                  <ReactEcharts
                    className="systemBorder"
                    ref={(e) => {
                      this.echartsElement = e;
                    }}
                    option={this.getOption(this.state.allDataRegular[this.state.selectValues][0])}
                    style={{ height: 250 }}
                    lazyUpdate={true}
                  ></ReactEcharts>
                </Col>
                <Col className="gutter-row mb-10" span={12}>
                  <Select
                    className="dynamicSelect"
                    value={this.state.allDataRegular[this.state.selectValues][4]}
                    style={{
                      width: 128,
                      float: "right",
                    }}
                    disabled
                    options={[
                      {
                        value: this.state.allDataRegular[this.state.selectValues][4],
                        label: this.state.allDataRegular[this.state.selectValues][4],
                      },
                    ]}
                  />
                  <ReactEcharts
                    className="systemBorder"
                    ref={(e) => {
                      this.echartsElement = e;
                    }}
                    option={this.getOption(this.state.allDataRegular[this.state.selectValues][1])}
                    style={{ height: 250 }}
                    lazyUpdate={true}
                  ></ReactEcharts>
                </Col>
              </Row>
              <Row gutter={10} className="mb-10">
                <Col className="gutter-row  mb-10" span={12}>
                  <Select
                    className="dynamicSelect"
                    value={this.state.allDataRegular[this.state.selectValues][4]}
                    style={{
                      width: 128,

                      float: "right",
                    }}
                    disabled
                    options={[
                      {
                        value: this.state.allDataRegular[this.state.selectValues][4],
                        label: this.state.allDataRegular[this.state.selectValues][4],
                      },
                    ]}
                  />
                  <ReactEcharts
                    className="systemBorder"
                    ref={(e) => {
                      this.echartsElement = e;
                    }}
                    option={this.getOption(this.state.allDataRegular[this.state.selectValues][2])}
                    style={{ width: "100%", height: 250 }}
                    lazyUpdate={true}
                  ></ReactEcharts>
                </Col>
                <Col className="gutter-row mb-10" span={12}>
                  <Select
                    className="dynamicSelect"
                    value={this.state.allDataRegular[this.state.selectValues][4]}
                    style={{
                      width: 128,
                      float: "right",
                    }}
                    disabled
                    options={[
                      {
                        value: this.state.allDataRegular[this.state.selectValues][4],
                        label: this.state.allDataRegular[this.state.selectValues][4],
                      },
                    ]}
                  />
                  <ReactEcharts
                    className="systemBorder"
                    ref={(e) => {
                      this.echartsElement = e;
                    }}
                    option={this.getOption(this.state.allDataRegular[this.state.selectValues][3])}
                    style={{ width: "100%", height: 250 }}
                    lazyUpdate={true}
                  ></ReactEcharts>
                </Col>
              </Row>
            </>
          ) : (
            <div style={{ textAlign: "center" }}>
              <Spin style={{ margin: "100px auto" }} />
            </div>
          )}
        </Card>
      </div>
    );
  }
}
