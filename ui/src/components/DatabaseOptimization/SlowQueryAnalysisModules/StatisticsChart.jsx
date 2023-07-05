import React, { Component } from "react";
import { Card } from "antd";
import PropTypes from "prop-types";
import ReactEcharts from "echarts-for-react";

export default class StatisticsChart extends Component {
  static propTypes = {
    statistics: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      xdata: [],
      ydata: [],
    };
  }
  getOption = () => {
    return {
      tooltip: { show: true, trigger:'axis' },
      xAxis: {
        type: "category",
        axisLabel: {
          textStyle: {
            color: "#4D5964",
            fontSize: "12",
          },
           formatter: function (name) {
          if (name.length > 5) {
            name = name.slice(0, 5) + "...";
          }
          return name;
        },
        },
        data: this.state.xdata,
      },
      yAxis: {
        type: "value",
        axisLabel: {
          textStyle: {
            color: "#4D5964",
            fontSize: "10",
          },
        },
      },
      grid: {
       height:'80%',
        top: '5%'
      },
      series: [
        {
          data: this.state.ydata,
          type: "bar",
        
          itemStyle: {
            color: "#9185F0",
          },
        },
      ],
    };
  };
  UNSAFE_componentWillReceiveProps(nextProps) {
    let xdataArr = [];
    let ydataArr = [];

    nextProps?.statistics.rows.forEach((element) => {
      xdataArr.push(element[0]);
      ydataArr.push(element[1]);
    });

    this.setState({
      xdata: xdataArr,
      ydata: ydataArr,
    });
  }
  render() {
    return (
      <div className="mb-10">
        <Card
          title="Statistics for Slow Query Template"
          style={{ height: "278px" }}
        >
          <ReactEcharts
            ref={(e) => {
              this.echartsElement = e;
            }}
            option={this.getOption()}
            style={{ width: "100%", height: "200px" }}
            lazyUpdate={true}
          ></ReactEcharts>
        </Card>
      </div>
    );
  }
}
