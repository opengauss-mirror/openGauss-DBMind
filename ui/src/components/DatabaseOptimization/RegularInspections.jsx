import React, { Component } from "react";
import { Col, message, Row, Spin, Select, DatePicker, Radio } from "antd";
import "../../assets/css/common.css";
import "../../assets/css/main/regularInspections.css";
import moment from "moment";
import Refresh from "../../assets/imgs/Refresh.png";
import Export from "../../assets/imgs/Export.png";
import RegularInspectionsDay from "./RegularInspectionsDay";
import RegularInspectionsWeek from "./RegularInspectionsWeek";
import { formatTimestamp } from "../../utils/function";
import { exportPDF } from "../../utils/exportPdf"
import { getRegularInspectionsInterface } from "../../api/clusterInformation";

const { RangePicker } = DatePicker;
export default class RegularInspections extends Component {
  constructor(props) {
    super(props);
    this.pdfRef = React.createRef()
    this.state = {
      checkType: "daily_check",
      typenewval: "",
      typeOptionsFilter: ["daily_check", "weekly_check", "monthly_check"],
      metricStatisticCount: {},
      showflag: true,
      startTime: "",
      endTime: "",
      reportVal: "",
      conclusionVal: "",
      instanceResource: "",
      activeConnections: "",
      totalConnections: "",
      tpsData: "",
      responseTime: "",
      dmlData: "",
      databaseSize: "",
      tableSize: "",
      historyAlarm: "",
      futureAlarm: "",
      distributionSlowSql: "",
      distributionRootCause: "",
      dynamicMemory: "",
      regularInspectionsDay: {},
      regularInspectionsWeek: {},
      regularDown: []
    };
  }
  changeTypeVal(e) {
    this.setState(
      {
        checkType: e.target.value,
        showflag: true,
      },
      () => this.getRegularInspections(e.target.value)
    );
  }
  placementChange = (e) => {
    this.setState({
      checkType: e.target.value,
    });
  };
  async getRegularInspections(value) {
    let params = { inspection_type: value ? value : this.state.checkType };
    const { success, data, msg } = await getRegularInspectionsInterface(params);
    if (success) {
      this.setState({ showflag: false });
      let sTime = formatTimestamp(data.rows[0][2]);
      let eTime = formatTimestamp(data.rows[0][3]);
      if (this.state.checkType === "daily_check") {
        this.setState({
          startTime: sTime,
          endTime: eTime,
          regularInspectionsDay: data,
          regularDown: data.rows

        });

      } else if (this.state.checkType === "weekly_check") {
        this.setState({
          startTime: sTime,
          endTime: eTime,
          regularInspectionsWeek: data,
          regularDown: data.rows
        });
      } else {
        this.setState({
          startTime: sTime,
          endTime: eTime,
          regularInspectionsWeek: data,
          regularDown: data.rows
        });
      }
    } else {
      message.error(msg);
    }
  }
  componentDidMount() {
    this.props.onRef && this.props.onRef(this);
    this.getRegularInspections();
  }
  handleRefresh() {
    this.setState({ showflag: true }, () => {
      this.getRegularInspections();
    });
  }
  // 导出
  handleDownload() {
    exportPDF('result', this.pdfRef.current)

  }
  componentWillUnmount = () => {
    this.setState = () => {
      return;
    };
  };
  render() {
    return (
      <div className="contentWrap RegularInspection">
        <div ref={this.pdfRef}>
          <>
            <div className="timeRange mb-10">
              <Radio.Group
                value={this.state.checkType}
                onChange={(val) => {
                  this.changeTypeVal(val);
                }}
              >
                <Radio.Button value="daily_check">Daily</Radio.Button>
                <Radio.Button value="weekly_check">Week</Radio.Button>
                <Radio.Button value="monthly_check">Month</Radio.Button>
              </Radio.Group>
              <RangePicker
                format="YYYY-MM-DD HH:mm:ss"
                value={[
                  moment(this.state.startTime),
                  moment(this.state.endTime),
                ]}
                style={{ marginLeft: 10 }}
                showTime
                disabled
              />
              <div
                className="buttonstyle"
                style={{ textAlign: "right", float: "right" }}
              >
                <img
                  src={Refresh}
                  alt=""
                  title="Refresh"
                  style={{ marginRight: 6 }}
                  onClick={() => this.handleRefresh()}
                ></img>
                <img
                  src={Export}
                  title="Export"
                  alt=""
                  onClick={() => this.handleDownload()}
                ></img>
              </div>
            </div>

            {this.state.showflag ? (
              <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>
            ) : this.state.checkType === "daily_check" ? (
              <RegularInspectionsDay
                regularInspectionsDay={this.state.regularInspectionsDay}
              />
            ) : this.state.checkType === "weekly_check" ? (
              <RegularInspectionsWeek
                regularInspectionsWeek={this.state.regularInspectionsWeek}
              />
            ) : (
              <RegularInspectionsWeek
                regularInspectionsWeek={this.state.regularInspectionsWeek}
              />
            )}
          </>
        </div>
      </div>
    );
  }
}
