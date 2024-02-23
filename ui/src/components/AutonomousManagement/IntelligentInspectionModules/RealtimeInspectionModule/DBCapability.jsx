import React, { Component } from "react";
import { Col, Row, Card, Collapse } from "antd";
import NodeEchartFormWork from "../../../NodeInformation/NodeModules/NodeEchartFormWork";

const panelStyle = {
  marginBottom: 15,
  background: "#F6F6F6",
  borderRadius: "3px 3px 0 0",
};
const { Panel } = Collapse;
export default class DBCapability extends Component {
  static propTypes = {
    dbCapability: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      selValue: this.props.selValue,
      selTimeValue: this.props.selTimeValue,
      primitiveDataAll: [],
      serviceAllData: [],
      vectorKey: "",
      isShow: false
    };
  }

  compare(property) {
    return function (a, b) {
      var value1 = a.labels[property];
      var value2 = b.labels[property];
      return value1 - value2;
    };
  }
  additionItem(arr1, arr2) {
    let newArr = arr1.map(function (item, index) {
      return item + arr2[index];
    });
    return newArr;
  }
  divisionItem(arr1, arr2) {
    let newArr = arr1.map(function (item, index) {
      return item / (item + arr2[index]);
    });
    return newArr;
  }
  getServiceData(data) {
    let result = [];

    result.push(
      data["pg_db_xact_commit"] ?? [],
      data["pg_db_xact_rollback"] ?? [],
      data["pg_db_conflicts"] ?? [],
      data["pg_db_confl_lock"] ?? [],
      data["pg_db_confl_snapshot"] ?? [],
      data["pg_db_confl_bufferpin"] ?? [],
      data["pg_db_confl_deadlock"] ?? [],
      data["gaussdb_deadlocks_rate"] ?? [],
      data["pg_db_temp_bytes"] ?? [],
      data["pg_db_temp_files"] ?? [],
      data["gaussdb_tup_inserted_rate"] ?? [],
      data["gaussdb_tup_deleted_rate"] ?? [],
      data["gaussdb_tup_updated_rate"] ?? [],
      data["gaussdb_tup_fetched_rate"] ?? []
    );
    if (result[0]) {
      result.forEach((item, index) => {
        item.sort(this.compare("datname"));
      });

      let newResult = [],
        totalArrayData = [],
        successRateArrayData = [],
        failureRateArrayData = [];
      result[0].forEach((item, index) => {
        let lengthDiff = 0;
        totalArrayData.push(JSON.parse(JSON.stringify(item)));
        successRateArrayData.push(JSON.parse(JSON.stringify(item)));
        failureRateArrayData.push(JSON.parse(JSON.stringify(item)));
        if (result[0][index].values.length > result[1][index].values.length) {
          lengthDiff =
            result[0][index].values.length - result[1][index].values.length;
          result[0][index].values.length.splice(0, lengthDiff);
          totalArrayData[index].timestamps = result[1][index].timestamps;
        } else if (
          result[0][index].values.length < result[1][index].values.length
        ) {
          lengthDiff =
            result[1][index].values.length - result[0][index].values.length;
          result[1][index].values.length.splice(0, lengthDiff);
          totalArrayData[index].timestamps = result[0][index].timestamps;
        }
        totalArrayData[index].values = this.additionItem(
          result[0][index].values,
          result[1][index].values
        );
        successRateArrayData[index].values = this.divisionItem(
          result[0][index].values,
          result[1][index].values
        );
        failureRateArrayData[index].values = this.divisionItem(
          result[1][index].values,
          result[0][index].values
        );
      });
      newResult = [
        result[0],
        result[1],
        totalArrayData,
        successRateArrayData,
        failureRateArrayData,
        result[2],
        result[3],
        result[4],
        result[5],
        result[6],
        result[7],
        result[8],
        result[9],
        result[10],
        result[11],
        result[12],
        result[13],
      ];
      let primitiveDataAll = [],
        serviceAllArray = [];
      result[0].forEach((item, index) => {
        let DataItems = [];
        newResult.forEach((oitem, oindex) => {
          DataItems.push(oitem[index]);
        });
        primitiveDataAll.push(DataItems);
      });
      primitiveDataAll.forEach((item, index) => {
        let chartData = [];
        let data1 = {
          legend: [
            { image: "", description: "Success" },
            { image: "", description: "Failure" },
            { image: "", description: "Total" },
          ],
          xAxisData: item[0].timestamps,
          seriesData: [
            { data: item[0].values, description: "Success", colors: "#2DA769" },
            { data: item[1].values, description: "Failure", colors: "#F43146" },
            { data: item[2].values, description: "Total", colors: "#5990FD" },
          ],
          flg: 0,
          legendFlg: 2,
          title: "Success/Failure/Total Transactions",
          unit: "",
          fixedflg: 0,
        };
        let data2 = {
          legend: [
            { image: "", description: "Success" },
            { image: "", description: "Failure" },
          ],
          xAxisData: item[3].timestamps,
          seriesData: [
            { data: item[3].values, description: "Success", colors: "#2DA769" },
            { data: item[4].values, description: "Failure", colors: "#5990FD" },
          ],
          flg: 1,
          legendFlg: 2,
          title: "Transaction Success/Failure Rate",
          unit: "%",
          fixedflg: 0,
        };
        let data3 = {
          legend: [
            { image: "", description: "Conflicts" },
            { image: "", description: "Confl Lock" },
            { image: "", description: "Confl Snapshot" },
            { image: "", description: "Confl Bufferpin" },
            { image: "", description: "Confl Deadlock" },
          ],
          xAxisData: item[5].timestamps,
          seriesData: [
            {
              data: item[5].values,
              description: "Conflicts",
              colors: "#2DA769",
            },
            {
              data: item[6].values,
              description: "Confl Lock",
              colors: "#F43146",
            },
            {
              data: item[7].values,
              description: "Confl Snapshot",
              colors: "#5990FD",
            },
            {
              data: item[8].values,
              description: "Confl Bufferpin",
              colors: "#EEBA18",
            },
            {
              data: item[9].values,
              description: "Confl Deadlock",
              colors: "#9185F0",
            },
          ],
          flg: 0,
          legendFlg: 2,
          title: "Conflicts Rate",
          unit: "",
          fixedflg: 0,
        };
        let data4 = {
          legend: [{ image: "", description: "Deadlock Rate" }],
          xAxisData: item[10].timestamps,
          seriesData: [
            {
              data: item[10].values,
              description: "Deadlock Rate",
              colors: "#EEBA18",
            },
          ],
          flg: 0,
          legendFlg: 2,
          title: "Deadlock Rate",
          unit: "",
          fixedflg: 0,
        };
        let data5 = {
          legend: [
            { image: "", description: "Temp Files" },
            { image: "", description: "Temp Bytes" },
          ],
          xAxisData: item[11].timestamps,
          seriesData: [
            {
              data: item[12].values,
              description: "Temp Files",
              colors: "#EC6F1A",
            },
            {
              data: item[11].values,
              description: "Temp Bytes",
              colors: "#9185EF",
            },
          ],
          flg: 0,
          legendFlg: 2,
          title: "Temp File",
          unit: "",
          fixedflg: 0,
        };
        let data6 = {
          legend: [
            { image: "", description: "Insert" },
            { image: "", description: "Delete" },
            { image: "", description: "Update" },
            { image: "", description: "Select" },
          ],
          xAxisData: item[13].timestamps,
          seriesData: [
            { data: item[13].values, description: "Insert", colors: "#2DA769" },
            { data: item[14].values, description: "Delete", colors: "#F43146" },
            { data: item[15].values, description: "Update", colors: "#5990FD" },
            { data: item[16].values, description: "Select", colors: "#EEBA18" },
          ],
          flg: 0,
          legendFlg: 2,
          title: "Averaged Rate Of DML",
          unit: "",
          fixedflg: 0,
        };
        chartData.push(data1, data2, data3, data4, data5, data6);
        serviceAllArray.push(chartData);
      });
      this.setState(
        () => ({
          serviceAllData: serviceAllArray,
          primitiveDataAll: primitiveDataAll,
          vectorKey: 0,
          isShow: true
        }),
        () => {
          this.onChange(0);
        }
      );
    }
  }
  UNSAFE_componentWillReceiveProps(nextProps) {
    this.getServiceData(nextProps.dbCapability);
  }

  onChange = (key) => {
    this.setState({ vectorKey: key });
  };
  render() {
    return (
      <Card title="DB Service Capability" className="mb-10">
        {this.state.isShow
          ? this.state.serviceAllData.map((item, index) => {
            return (
              <Collapse
                activeKey={this.state.vectorKey}
                onChange={(key) => {
                  this.onChange(key);
                }}
                expandIconPosition="end"
                style={{
                  background: "#ffffffff",
                  borderRadius: "3px 3px 0 0",
                }}
              >
                <Panel
                  header={
                    this.state.primitiveDataAll[index][0].labels.datname
                  }
                  key={index}
                  forceRender={true}
                  style={panelStyle}
                >
                  <Row gutter={[10, 10]}>
                    <Col className="gutter-row cpuborder" span={12}>
                      <NodeEchartFormWork echartData={item[0]} />
                    </Col>
                    <Col className="gutter-row cpuborder" span={12}>
                      <NodeEchartFormWork echartData={item[1]} />
                    </Col>
                    <Col className="gutter-row cpuborder" span={12}>
                      <NodeEchartFormWork echartData={item[2]} />
                    </Col>
                    <Col className="gutter-row cpuborder" span={12}>
                      <NodeEchartFormWork echartData={item[3]} />
                    </Col>
                    <Col className="gutter-row cpuborder" span={12}>
                      <NodeEchartFormWork echartData={item[4]} />
                    </Col>

                    <Col className="gutter-row cpuborder" span={12}>
                      <NodeEchartFormWork echartData={item[5]} />
                    </Col>
                  </Row>
                </Panel>
              </Collapse>
            );
          })
          : ""}
      </Card>
    );
  }
}
