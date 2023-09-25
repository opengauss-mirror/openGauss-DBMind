import React, { Component } from "react";
import { Col, Row, Card } from "antd";

import NodeEchartFormWork from "../../../NodeInformation/NodeModules/NodeEchartFormWork";

export default class SystemMemory extends Component {
  static propTypes = {
    systemMemory: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      chartData1: {},
      chartData2: {},
      chartData3: {},
      chartData4: {},
      chartData5: {},
    };
  }

  getMemoryData(data) {
    let result = [];
    result.push(
      data["node_memory_MemTotal_bytes"] ?? [],
      data["os_mem_usage"] ?? [],
      data["node_memory_MemAvailable_bytes"] ?? [],
      data["node_memory_Buffers_bytes"] ?? [],
      data["node_memory_Cached_bytes"] ?? []
    );
    if (result[0]) {
      let totalLeft = [],
        usageData = [],
        usageValues = [];
      result[1][0].values.forEach((oitem) => {
        usageValues.push(result[0][0].values[0] * oitem);
      });
      usageData = JSON.parse(JSON.stringify(result[1]));
      usageData[0].values = usageValues;
      let allChartArray = [
        usageData,
        result[2],
        result[3],
        result[4],
        result[1],
      ],
        xDataArray = [[], [], [], [], []],
        yDataArray = [[], [], [], [], []];
      allChartArray.forEach((item, index) => {
        xDataArray[index] = item[0].timestamps;
      });
      allChartArray.forEach((item, index) => {
        item[0].values.forEach((oitem) => {
          if (index === 4) {
            yDataArray[index].push(oitem);
          } else {
            yDataArray[index].push((oitem / 1024 / 1024 / 1024).toFixed(2));
          }
        });
      });
      yDataArray.forEach((oitem) => {
        totalLeft.push(Number(oitem[oitem.length - 1]));
      });
      let data1 = {
        legend: [{ image: "", description: "Used Space(GB)" }],
        xAxisData: xDataArray[0],
        seriesData: [
          {
            data: yDataArray[0],
            description: "Used Space(GB)",
            colors: "#EC6F1A",
          },
        ],

        flg: 0,
        legendFlg: 1,
        unit: "GB",
        fixedflg: 0,

      };
      let data2 = {
        legend: [{ image: "", description: "Available Space" }],
        xAxisData: xDataArray[1],
        seriesData: [
          {
            data: yDataArray[1],
            description: "Available Space",
            colors: "#2DA769",
          },
        ],
        flg: 0,
        legendFlg: 1,
        unit: "GB",
        fixedflg: 0,
      };
      let data3 = {
        legend: [{ image: "", description: "Buffer Space" }],
        xAxisData: xDataArray[2],
        seriesData: [
          {
            data: yDataArray[2],
            description: "Buffer Space",
            colors: "#9185F0",
          },
        ],
        flg: 0,
        legendFlg: 1,
        unit: "GB",
        fixedflg: 0,
      };
      let data4 = {
        legend: [{ image: "", description: "Cache Space" }],
        xAxisData: xDataArray[3],
        seriesData: [
          {
            data: yDataArray[3],
            description: "Cache Space",
            colors: "#EEBA18",
          },
        ],
        flg: 0,
        legendFlg: 1,
        unit: "GB",
        fixedflg: 0,
      };
      let data5 = {
        legend: [{ image: "", description: "Used Space(%)" }],
        xAxisData: xDataArray[4],
        seriesData: [
          {
            data: yDataArray[4],
            description: "Used Space(%)",
            colors: "#EC6F1A",
          },
        ],
        flg: 1,
        legendFlg: 1,
        unit: "%",
        fixedflg: 0,
      };
      this.setState({
        chartData1: data1,
        chartData2: data2,
        chartData3: data3,
        chartData4: data4,
        chartData5: data5,
      });
    }
  }
  UNSAFE_componentWillReceiveProps(nextProps) {
    this.getMemoryData(nextProps.systemMemory);
  }

  render() {
    return (
      <Card title="Memory" className="Memoryclass mb-10">
        <Row gutter={[10, 10]} className="mb-10">
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork echartData={this.state.chartData1} />
          </Col>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork echartData={this.state.chartData2} />
          </Col>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork echartData={this.state.chartData3} />
          </Col>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork echartData={this.state.chartData4} />
          </Col>
          <Col className="gutter-row cpuborder" span={24}>
            <NodeEchartFormWork echartData={this.state.chartData5} />
          </Col>
        </Row>
      </Card>
    );
  }
}
