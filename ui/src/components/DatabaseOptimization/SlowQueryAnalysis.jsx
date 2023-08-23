import React, { Component } from "react";
import {
  Col,
  message,
  Row,
  Spin,
  Card,
  Empty,
  Modal,
  Button,
  Input,
} from "antd";
import { InfoCircleFilled } from "@ant-design/icons";
import "../../assets/css/common.css";
import "../../assets/css/main/slowQueryAnaly.css";

import Refresh from "../../assets/imgs/Refresh.png";
import Setup from "../../assets/imgs/Setup.png";
import SystemtableRateChart from "./SlowQueryAnalysisModules/SystemtableRateChart";
import StatisticsForSchemaChart from "./SlowQueryAnalysisModules/StatisticsForSchemaChart";
import SlowquerycountChart from "./SlowQueryAnalysisModules/SlowquerycountChart";
import DistributionChart from "./SlowQueryAnalysisModules/DistributionChart";
import MeanCpuTimeChart from "./SlowQueryAnalysisModules/MeanCpuTimeChart";
import MeanIoTimeChart from "./SlowQueryAnalysisModules/MeanIoTimeChart";
import MeanBufferHitRateChart from "./SlowQueryAnalysisModules/MeanBufferHitRateChart";
import MeanFetchTimeChart from "./SlowQueryAnalysisModules/MeanFetchTimeChart";
import StatisticsChart from "./SlowQueryAnalysisModules/StatisticsChart";
import SlowQueryTable from "./SlowQueryAnalysisModules/SlowQueryTable";
import TableofSlowQueryTable from "./SlowQueryAnalysisModules/TableofSlowQueryTable";
import KilledSlowQueryTable from "./SlowQueryAnalysisModules/KilledSlowQuery";
import StatisticsForDatabaseChart from "./SlowQueryAnalysisModules/StatisticsForDatabaseChart";
import main from "../../assets/imgs/main.png";
import nb from "../../assets/imgs/nb.png";
import {
  getSlowQueryAnalysisInterface,
  getSlowQueryRecentCount,
} from "../../api/databaseOptimization";
import {
  getSettingListInterface,
  putSettingDetailInterface,
  getSettingDefaults,
} from "../../api/dbmindSettings";
import { getTimedTaskStatus } from "../../api/overview";
import {
  FileSearchOutlined,
  FundOutlined,
  MonitorOutlined,
} from "@ant-design/icons";
const iconimg = [
  <FileSearchOutlined key="1" />,
  <MonitorOutlined key="2" />,
  <FundOutlined key="3" />,
  <FileSearchOutlined key="4" />,
  <MonitorOutlined key="5" />,
  <FundOutlined key="6" />,
];

const labelStyle = {
  width: 194,
  float: "left",
  textAlign: "right",
  lineHeight: "32px",
};
let resultArr = [];
const inputStyle = { marginLeft: 10, marginRight: 20, width: 220 };
export default class SlowQueryAnalysis extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showflag: true,
      topList: [],
      statisticsForDatabase: {},
      statisticsforSchema: {},
      sysInSlowQuery: {},
      slowQueryCount: {},
      distribution: {},
      meanCpuTime: {},
      meanIoTime: {},
      meanBufferHitRate: {},
      meanFetchTime: {},
      statistics: {},
      slowQueryTemplate: {},
      tableOfSlowQuery: {},
      isSettingVisible: false,
      planThreshold: "",
      complexThreshold: "",
      nestloopThreshold: "",
      largeThreshold: "",
      isKillerShow: false,
      isSlowQuery: false,
    };
  }
  // 查询页面是否显示
  async getTaskStatus() {
    const { success, data, msg } = await getTimedTaskStatus();
    if (success) {
      data.rows.forEach((item) => {
        if (item[0] === "slow_sql_diagnosis" && item[1] === "Running") {
          this.setState({ isSlowQuery: true }, () => {
            this.getSlowQueryAnalysis({ current: 1, pagesize: 10 });
          });
        }
        if (item[0] === "slow_query_killer" && item[1] === "Running") {
          this.setState({ isKillerShow: true });
        }
      });
    } else {
      message.error(msg);
    }
  }
  // 获取设置表单默认值
  async initValue() {
    const { success, data } = await getSettingListInterface();

    let resultMap = new Map();
    if (success) {
      data.dynamic.slow_sql_threshold.forEach((item) => {
        if (
          item[0] === "nestloop_rows_threshold" ||
          item[0] === "plan_height_threshold" ||
          item[0] === "complex_operator_threshold" ||
          item[0] === "large_in_list_threshold"
        ) {
          resultMap.set(item[0], item[1]);
        }
      });
      this.setState({
        complexThreshold: resultMap.get("complex_operator_threshold"),
        largeThreshold: resultMap.get("large_in_list_threshold"),
        nestloopThreshold: resultMap.get("nestloop_rows_threshold"),
        planThreshold: resultMap.get("plan_height_threshold"),
      });
    }
  }
  onBlurChangeLarge(e) {
    if (e && !this.getHasSame("large_in_list_threshold")) {
      resultArr.push({
        name: "large_in_list_threshold",
        value: e.target.value,
      });
    }
  }
  handleChangeLarge = (e) => {
    if (e) {
      this.setState({ largeThreshold: e.target.value });
    } else {
      message.warning("The input value is a positive integer greater than 0");
    }
  };
  onBlurChangeComplex(e) {
    if (e && !this.getHasSame("complex_operator_threshold")) {
      resultArr.push({
        name: "complex_operator_threshold",
        value: e.target.value,
      });
    }
  }
  handleChangeComplex = (e) => {
    if (e) {
      this.setState({ complexThreshold: e.target.value });
    } else {
      message.warning("The input value is a positive integer greater than 0");
    }
  };
  // 插入前查询是否有重复
  getHasSame(elem) {
    let hasSame = false;
    if (resultArr.length !== 0) {
      resultArr.forEach((item) => {
        if (item.name === elem) {
          hasSame = true;
        }
      });
    }
    return hasSame;
  }
  onBlurChangeNestloop(e) {
    if (e && !this.getHasSame("nestloop_rows_threshold")) {
      resultArr.push({
        name: "nestloop_rows_threshold",
        value: e.target.value,
      });
    }
  }
  handleChangeNestloop = (e) => {
    if (e) {
      this.setState({ nestloopThreshold: e.target.value });
    } else {
      message.warning("The input value is a positive integer greater than 0");
    }
  };
  onBlurChangePlan(e) {
    if (e && !this.getHasSame("plan_height_threshold")) {
      resultArr.push({ name: "plan_height_threshold", value: e.target.value });
    }
  }
  handleChangePlan = (e) => {
    if (e) {
      this.setState({ planThreshold: e.target.value });
    } else {
      message.warning("The input value is a positive integer greater than 0");
    }
  };
  handleSetting() {
    this.setState(
      {
        isSettingVisible: true,
      },
      () => {
        this.initValue();
      }
    );
  }
  async handleSettingReset() {
    const { success, data, msg } = await getSettingDefaults({
      configname: "slow_sql_threshold",
    });
    if (success) {
      this.setState({
        nestloopThreshold: data.nestloop_rows_threshold[0].toString(),
        planThreshold: data.plan_height_threshold[0].toString(),
        complexThreshold: data.complex_operator_threshold[0].toString(),
        largeThreshold: data.large_in_list_threshold[0].toString(),
      });
    } else {
      message.error(msg);
    }
  }
  handleSettingCancel() {
    this.setState({
      isSettingVisible: false,
    });
  }
  // 设置-保存
  async handleSettingOk() {
    if (
      this.state.nestloopThreshold &&
      this.state.planThreshold &&
      this.state.complexThreshold &&
      this.state.largeThreshold
    ) {
      resultArr.forEach((item) => {
        let params = {
          config: "slow_sql_threshold",
          name: item.name,
          value: item.value,
          dynamic: true,
        };
        this.putSettingDetails(params);
      });
    } else {
      message.warning("The input value is a positive integer greater than 0");
    }
    resultArr = [];
  }
  async putSettingDetails(params) {
    const { success } = await putSettingDetailInterface(params);
    if (success) {
      this.setState({
        isSettingVisible: false,
      });
      message.success("SAVE SUCCESS");
    }
  }
  async getSlowQueryAnalysis(params) {
    const { success, data, msg } = await getSlowQueryAnalysisInterface(params);
    if (success) {
      this.getSlowQueryRecentCount();
      let toplistArr = [];
      this.setState(
        {
          showflag: false,
        },
        () => {
          Object.keys(data).forEach(function (key) {
            if (
              key === "main_slow_queries" ||
              key === "nb_unique_slow_queries" ||
              key === "slow_query_threshold"
            ) {
              let obj = {
                name:
                  key === "slow_query_threshold"
                    ? key.replace(/_/g, " ") + "    (ms)"
                    : key.replace(/_/g, " "),
                num: data[key],
                img: iconimg[Math.ceil(Math.random() * 5)],
              };
              toplistArr.push(obj);
            }
          });
        }
      );
      this.setState({
        topList: toplistArr,
        statisticsForDatabase: data.statistics_for_database,
        statisticsforSchema: data.statistics_for_schema,
        sysInSlowQuery: data.systable,
        slowQueryCount: data.slow_query_count,
        distribution: data.distribution,
        meanCpuTime: data.mean_cpu_time,
        meanIoTime: data.mean_io_time,
        meanBufferHitRate: data.mean_buffer_hit_rate,
        meanFetchTime: data.mean_fetch_rate,
        statistics: data.slow_query_template,
        slowQueryTemplate: data.slow_query_template,
        tableOfSlowQuery: data.table_of_slow_query,
      });
    } else {
      message.error(msg);
    }
  }
  async getSlowQueryRecentCount() {
    const { success, data, msg } = await getSlowQueryRecentCount();
    if (success) {
      let dataObj = this.state.tableOfSlowQuery;
      dataObj["total"] = data;
      this.setState(() => ({
        tableOfSlowQuery: dataObj,
      }));
    } else {
      message.error(msg);
    }
  }
  handleRefresh() {
    this.setState({ showflag: true }, () => {
      this.getSlowQueryAnalysis({ current: 1, pagesize: 10 });
    });
  }

  componentDidMount() {
    this.props.onRef && this.props.onRef(this);
    this.getTaskStatus();
    this.initValue();
  }
  render() {
    return (
      <div className="contentWrap">
        <div className="slowQueryTitle">
          {this.state.isSlowQuery || this.state.isKillerShow ? (
            <>
              {this.state.isSlowQuery && !this.state.showflag ? (
                <div className="slowSql">
                  <div className="buttonstyle" style={{ textAlign: "right" }}>
                    <img
                      src={Refresh}
                      alt=""
                      title="Refresh"
                      style={{ marginRight: 6 }}
                      onClick={() => this.handleRefresh()}
                    ></img>
                    <img
                      src={Setup}
                      alt=""
                      title="Setup"
                      onClick={() => this.handleSetting()}
                    ></img>
                  </div>
                  <div
                    className="content slowQueryAnaly"
                    style={{ display: "flex" }}
                  >
                    <div className="leftGrid" style={{ width: "80%" }}>
                      <Row gutter={10} className="mb-10">
                        <Col className="gutter-row cardName" span={8}>
                          <Card
                            title="Main Slow Queries"
                            style={{ height: 100 }}
                            extra={<img src={main} alt=""></img>}
                          >
                            <span style={{ padding: "0" }}>
                              {this.state.topList.length !== 0
                                ? this.state.topList[0].num
                                : 0}
                            </span>
                          </Card>
                        </Col>
                        <Col className="gutter-row cardName" span={8}>
                          <Card
                            title="Nb Unique Slow Queries"
                            style={{ height: 100 }}
                            extra={<img src={nb} alt=""></img>}
                          >
                            <span style={{ padding: "0" }}>
                              {this.state.topList.length !== 0
                                ? this.state.topList[1].num
                                : 0}
                            </span>
                          </Card>
                        </Col>
                        <Col className="gutter-row cardName" span={8}>
                          <Card
                            title="Slow Query Threshold(ms)"
                            style={{ height: 100 }}
                            extra={<img src={nb} alt=""></img>}
                          >
                            {/* this.state.topList[2].num */}
                            <span style={{ padding: "0" }}>
                              {this.state.topList.length !== 0
                                ? this.state.topList[2].num
                                : 0}
                            </span>
                          </Card>
                        </Col>
                      </Row>
                      <Row gutter={10}>
                        <Col className="gutter-row" span={9}>
                          <StatisticsForDatabaseChart
                            statisticsForDatabase={
                              this.state.statisticsForDatabase
                            }
                          />
                        </Col>
                        <Col className="gutter-row" span={9}>
                          <StatisticsForSchemaChart
                            statisticsforSchema={this.state.statisticsforSchema}
                          />
                        </Col>
                        <Col className="gutter-row" span={6}>
                          <SystemtableRateChart
                            sysInSlowQuery={this.state.sysInSlowQuery}
                          />
                        </Col>
                      </Row>
                      <Row gutter={10}>
                        <Col className="gutter-row" span={15}>
                          <SlowquerycountChart
                            slowQueryCount={this.state.slowQueryCount}
                          />
                        </Col>
                        <Col className="gutter-row" span={9}>
                          <DistributionChart
                            distribution={this.state.distribution}
                          />
                        </Col>
                      </Row>

                      <Row gutter={10}>
                        <Col className="gutter-row" span={15}>
                          <SlowQueryTable
                            slowQueryTemplate={this.state.slowQueryTemplate}
                          />
                        </Col>
                        <Col className="gutter-row" span={9}>
                          <StatisticsChart statistics={this.state.statistics} />
                        </Col>
                      </Row>
                    </div>
                    <div className="rightGrid" style={{ width: "20%" }}>
                      <MeanCpuTimeChart meanCpuTime={this.state.meanCpuTime} />
                      <MeanIoTimeChart meanIoTime={this.state.meanIoTime} />
                      <MeanBufferHitRateChart
                        meanBufferHitRate={this.state.meanBufferHitRate}
                      />
                    </div>
                  </div>
                  <div className="queryTable">
                    <TableofSlowQueryTable
                      tableOfSlowQuery={this.state.tableOfSlowQuery}
                    />
                  </div>
                </div>
              ) : (
                <div style={{ textAlign: "center" }}>
                  <Spin style={{ margin: "100px auto" }} />{" "}
                </div>
              )}
              <div className="queryTable">
                {this.state.isKillerShow ? <KilledSlowQueryTable /> : <Empty />}
              </div>
              <Modal
                title="Setting"
                width="40vw"
                className="slowSetting"
                footer={
                  <div style={{ textAlign: "center" }}>
                    <Button
                      key="submit"
                      type="primary"
                      onClick={() => this.handleSettingOk()}
                    >
                      Save
                    </Button>
                    <Button
                      key="back"
                      onClick={() => this.handleSettingReset()}
                    >
                      Reset
                    </Button>
                  </div>
                }
                destroyOnClose="true"
                visible={this.state.isSettingVisible}
                maskClosable={false}
                onOk={() => this.handleSettingOk()}
                onCancel={() => this.handleSettingCancel()}
              >
                <p>
                  <label style={labelStyle}>nestloop_rows_threshold：</label>
                  <Input
                    style={inputStyle}
                    min={0}
                    onChange={(e) => this.handleChangeNestloop(e)}
                    onBlur={(e) => this.onBlurChangeNestloop(e)}
                    stringMode
                    value={this.state.nestloopThreshold}
                  />
                  <label style={{ color: "#ADA6ED" }} className="labelInfo">
                    <InfoCircleFilled />
                    The maximum number of tuples suitable for the nested loop
                    operator
                  </label>
                </p>
                <p>
                  <label style={labelStyle}>plan_height_threshold：</label>
                  <Input
                    style={inputStyle}
                    min={1}
                    onChange={(e) => this.handleChangePlan(e)}
                    onBlur={(e) => this.onBlurChangePlan(e)}
                    value={this.state.planThreshold}
                  />
                  <label style={{ color: "#ADA6ED" }} className="labelInfo">
                    <InfoCircleFilled />
                    The threshold of execution plan
                  </label>
                </p>
                <p>
                  <label style={labelStyle}>complex_operator_threshold：</label>
                  <Input
                    style={inputStyle}
                    min={1}
                    onChange={(e) => this.handleChangeComplex(e)}
                    onBlur={(e) => this.onBlurChangeComplex(e)}
                    value={this.state.complexThreshold}
                  />
                  <label style={{ color: "#ADA6ED" }} className="labelInfo">
                    <InfoCircleFilled />
                    The thresold of complex operator, now it refers to the join
                    operator
                  </label>
                </p>
                <p>
                  <label style={labelStyle}>large_in_list_threshold：</label>
                  <Input
                    style={inputStyle}
                    min={1}
                    onChange={(e) => this.handleChangeLarge(e)}
                    onBlur={(e) => this.onBlurChangeLarge(e)}
                    value={this.state.largeThreshold}
                  />
                  <label style={{ color: "#ADA6ED" }} className="labelInfo">
                    <InfoCircleFilled />
                    the threshold of the number of elements in the in-clause
                  </label>
                </p>
              </Modal>
            </>
          ) : (
            <Empty className="mainContent" />
          )}
        </div>
      </div>
    );
  }
}
