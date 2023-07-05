import React, { Component } from "react";
import { Tabs, Spin, message } from "antd";
import "../../../assets/css/main/IntelligentInspection.css";
import RealtimeSystem from "./RealtimeSystem";
import RealtimeDatabase from "./RealtimeDatabase";
import InspectionsDay from "./InspectionsDay";
import InspectionsWeek from "./InspectionsWeek";
import InspectionsDaySystem from "./InspectionsDaySystem";
import InspectionsWeekSystem from "./InspectionsWeekSystem";
import Export from "../../../assets/imgs/Export.png";
import Back from "../../../assets/imgs/getback.png";
import { exportPDF } from "../../../utils/exportPdf";
import { getRegularInspectionsInterface } from "../../../api/clusterInformation";
import { getTaskReport } from "../../../api/intelligentInspection";

export default class IntelligentInspectionDetail extends Component {
  constructor(props) {
    super(props);
    this.databaseRef = React.createRef();
    this.state = {
      checkedData: "1",
      regularInspectionsDay: {},
      regularInspectionsWeek: {},
      regularInspectionsMonth: {},
      showflag: false,
      isShowRealtime: false,
      realtimeInspections: {},
    };
  }
  changeTypeVal(e) {
    this.setState({ checkedData: e });
  }

  okHandle() {
    exportPDF("results", this.databaseRef.current);
  }
  async getRealtimeInspections() {
    const { success, data, msg } = await getTaskReport(
      this.props.inspectionMode.id
    );
    if (success) {
      this.setState(
        {
          realtimeInspections: data.rows[0][1],
          isShowRealtime: true,
        },
        () => {
          if (!Object.keys(this.state.realtimeInspections).includes("system")) {
            this.changeTypeVal("2");
          }
        }
      );
    } else {
      message.error(msg);
    }
  }
  async getRegularInspections() {
    let params = { inspection_type: this.props.inspectionMode.inspection_type };
    const { success, data, msg } = await getRegularInspectionsInterface(params);
    if (success) {
      this.setState({ showflag: true });

      if (this.props.inspectionMode.inspection_type === "daily_check") {
        this.setState({
          regularInspectionsDay: data,
        });
      } else if (this.props.inspectionMode.inspection_type === "weekly_check") {
        this.setState({
          regularInspectionsWeek: data,
        });
      } else if (
        this.props.inspectionMode.inspection_type === "monthly_check"
      ) {
        this.setState({
          regularInspectionsMonth: data,
        });
      }
    } else {
      message.error(msg);
    }
  }
  goback() {
    this.props.getBack(false);
  }
  componentDidMount() {
    if (this.props.inspectionMode.inspection_type === "real_time_check") {
      this.getRealtimeInspections();
    } else {
      this.getRegularInspections();
    }
  }
  render() {
    const items = [
      {
        key: "1",
        label: "system",

        children: (
          <div>
            {this.props.inspectionMode.inspection_type === "real_time_check" ? (
              this.state.isShowRealtime ? (
                <RealtimeSystem
                  realtimeInspections={this.state.realtimeInspections.system}
                />
              ) : (
                ""
              )
            ) : this.state.showflag ? (
              this.props.inspectionMode.inspection_type === "daily_check" ? (
                <InspectionsDaySystem
                  regularInspectionsDay={this.state.regularInspectionsDay}
                />
              ) : this.props.inspectionMode.inspection_type ===
                "weekly_check" ? (
                <InspectionsWeekSystem
                  regularInspectionsWeek={this.state.regularInspectionsWeek}
                />
              ) : (
                <InspectionsWeekSystem
                  regularInspectionsWeek={this.state.regularInspectionsMonth}
                />
              )
            ) : (
              <div style={{ textAlign: "center" }}>
                <Spin style={{ margin: "100px auto" }} />
              </div>
            )}
          </div>
        ),
      },
      {
        key: "2",
        label: "database",
        children: (
          <div>
            {this.props.inspectionMode.inspection_type === "real_time_check" ? (
              this.state.isShowRealtime ? (
                <RealtimeDatabase
                  DBrealtimeInspections={
                    this.state.realtimeInspections.database
                  }
                />
              ) : (
                ""
              )
            ) : this.state.showflag ? (
              this.props.inspectionMode.inspection_type === "daily_check" ? (
                <InspectionsDay
                  regularInspectionsDay={this.state.regularInspectionsDay}
                />
              ) : this.props.inspectionMode.inspection_type ===
                "weekly_check" ? (
                <InspectionsWeek
                  regularInspectionsWeek={this.state.regularInspectionsWeek}
                />
              ) : (
                <InspectionsWeek
                  regularInspectionsWeek={this.state.regularInspectionsMonth}
                />
              )
            ) : (
              <div style={{ textAlign: "center" }}>
                <Spin style={{ margin: "100px auto" }} />{" "}
              </div>
            )}
          </div>
        ),
      },
    ];
    const itemData = items.map((item) => {
      let itemArr = [];
      if (this.props.inspectionMode.inspection_type === "real_time_check") {
        if (Object.keys(this.state.realtimeInspections).includes(item.label)) {
          itemArr = item
        }
      } else {
        itemArr = item
      }
      return itemArr
    });

    return (
      <>
        <div className="detail" ref={this.databaseRef}>
          <Tabs
            activeKey={this.state.checkedData}
            onChange={(val) => {
              this.changeTypeVal(val);
            }}
            tabBarExtraContent={
              <div>
                {this.props.isShowBtn ? (
                  <img
                    src={Export}
                    alt=""
                    title="Export"
                    onClick={() => this.okHandle()}
                  ></img>
                ) : (
                  ""
                )}
                <img src={Back} alt="" title="back" onClick={() => this.goback()}></img>
              </div>
            }
            className="spectionTask"
            items={itemData}
          ></Tabs>
        </div>
      </>
    );
  }
}
