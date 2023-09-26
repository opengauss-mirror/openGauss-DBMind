import React, { Component } from "react";
import { Tabs, Spin, message } from "antd";
import "../../../assets/css/main/IntelligentInspection.css";
import RealtimeSystem from "./RealtimeSystem";
import RealtimeDatabase from "./RealtimeDatabase";
import Export from "../../../assets/imgs/Export.png";
import Back from "../../../assets/imgs/getback.png";
import { exportPDF } from "../../../utils/exportPdf";
import { getTaskReport } from "../../../api/intelligentInspection";

export default class IntelligentInspectionDetail extends Component {
  constructor(props) {
    super(props);
    this.databaseRef = React.createRef();
    this.state = {
      checkedData: "1",
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
      if (this.props.inspectionMode.inspection_type === "real_time_check_monthly") {
        message.info('实时查询，数据受tsdb限制');
      }
    } else {
      message.error(msg);
    }
  }

  goback() {
    this.props.getBack(false);
  }
  componentDidMount() {
    this.getRealtimeInspections();
  }
  render() {
    const items = [
      {
        key: "1",
        label: "system",

        children: (
          <div>
            {
              this.state.isShowRealtime ? (
                <RealtimeSystem
                  realtimeInspections={this.state.realtimeInspections.system}
                />
              ) : (
                ""
              )
            }
          </div>
        ),
      },
      {
        key: "2",
        label: "database",
        children: (
          <div>
            {
              this.state.isShowRealtime ? (
                <RealtimeDatabase
                  DBrealtimeInspections={
                    this.state.realtimeInspections.database
                  }
                />
              ) : (
                ""
              )
            }
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
