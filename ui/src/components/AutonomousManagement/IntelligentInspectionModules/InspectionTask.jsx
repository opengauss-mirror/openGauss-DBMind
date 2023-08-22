import React, { Component } from "react";
import {
  Card,
  Table,
  Popconfirm,
  Modal,
  DatePicker,
  Tabs,
  Checkbox,
  message,
} from "antd";
import ResizeableTitle from "../../common/ResizeableTitle";
import Over from "../../../assets/imgs/over.png";
import Failure from "../../../assets/imgs/failure.png";

import moment from "moment";
import TabPane from "antd/lib/tabs/TabPane";
import { formatTableTitleToUpper } from "../../../utils/function";
import {
  getTaskSubmit,
  getRealtimeInspectionList,
  deleteTask,
} from "../../../api/intelligentInspection";
const { RangePicker } = DatePicker;

export default class InspectionTask extends Component {
  constructor() {
    super();
    this.state = {
      dataSource: [],
      columns: [],
      isModalVisible: false,
      startTime: "",
      endTime: "",
      checkedKey: "real_time_check",
      selected: ["System", "Database"],
      isShowTask: false,
    };
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  //   删除
  async handleDelete(e) {
    const { success, data, msg } = await deleteTask(e.id);
    if (success) {
      message.success("DELETE SUCCESS");
      this.getRealtimeInspectionList();
    } else {
      message.error(msg);
    }
  }
  //   处理表格数据
  handleTableData(header, rows) {
    let inspectionColumObj = {};
    let tableHeader = [];
    header.push("operate");
    header.forEach((item) => {
      inspectionColumObj = {
        title: formatTableTitleToUpper(item),
        dataIndex: item,
        key: item,
        ellipsis: true,
        width: "13%",
        render: (row, record) => {
          if (item === "state") {
            let src = record.state === "success" ? Over : Failure;
            return <img src={src} alt="" className="iconstyle"></img>;
          } else if (item === "operate") {
            return (
              <Popconfirm
                title="Sure to delete?"
                onConfirm={() => this.handleDelete(record)}
              >
                <span className="deleteStyle">Delete</span>
              </Popconfirm>
            );
          } else {
            return row;
          }
        },
      };
      tableHeader.push(inspectionColumObj);
    });
    let res = [];
    rows.forEach((item, index) => {
      let tabledata = {};
      for (let i = 0; i < header.length; i++) {
        tabledata[header[i]] = item[i];
      }
      tabledata["key"] = index + "";
      res.push(tabledata);
    });
    this.setState(() => ({
      dataSource: res,
      columns: tableHeader,
    }));
  }
  async getRealtimeInspectionList() {
    const { success, data, msg } = await getRealtimeInspectionList();
    if (success) {
      this.setState({ isShowTask: true });
      this.handleTableData(data.header, data.rows);
    } else {
      message.error(msg);
    }
  }

  addTask() {
    this.setState({ isModalVisible: true });
  }
  async handleOk() {
    if(this.state.checkedKey === "real_time_check" && !this.state.startTime){
      message.warning("Please select time!");
      return;
    }
    let params = {
      inspectionType: this.state.checkedKey,
      startTime: this.state.startTime,
      endTime: this.state.endTime,
      selectMetrics: this.state.selected.toString(),
    };
    const { success, data, msg } = await getTaskSubmit(params);
    this.setState({
      isModalVisible: false,
    });
    if (success) {
      message.success("SAVE SUCCESS");
      this.getRealtimeInspectionList();
    } else {
      message.error(msg);
    }
  }
  handleCancel() {
    this.setState({ isModalVisible: false });
  }
  //   改变任务模式
  changeTypeVal(e) {
    this.setState({ checkedKey: e });
  }

  //   System or database
  onSelectChange(e) {
    this.setState({ selected: e });
  }
  componentDidMount() {
    this.getRealtimeInspectionList();
  }
  disabledDate = (current) => {
    return current < moment().subtract(3, "days");
  };

  setDates = (dates, dateStrings) => {
    this.setState({
      startTime: new Date(dateStrings[0]).getTime(),
      endTime: new Date(dateStrings[1]).getTime(),
    });
  };
  onChange = (e) => {
    this.setState({
      unit: e.target.value,
    });
  };
  render() {
    const columns = this.state.columns.map((col, index) => ({
      ...col,
    }));
    return (
      <div>
        {this.state.isShowTask ? (
          <Card
            title="Inspection Task"
            className="InspectionTask"
            extra={
              <div className="addTask" onClick={() => this.addTask()}>
                <span>new inspection</span>
              </div>
            }
          >
            <Table
              bordered
              components={this.components}
              columns={columns}
              dataSource={this.state.dataSource}
              size="small"
              rowKey={(record) => record.key}
              pagination={false}
              style={{ height: 170, overflowY: "auto" }}
              scroll={{ x: "100%" }}
            />
            <Modal
              className="taskModal"
              title="New Inspection"
              destroyOnClose="true"
              visible={this.state.isModalVisible}
              maskClosable={false}
              centered="true"
              onOk={() => this.handleOk()}
              onCancel={() => this.handleCancel()}
              okText="Submit"
            >
              <div className="spectionModal">
                <Tabs
                  activeKey={this.state.checkedKey}
                  onChange={(val) => {
                    this.changeTypeVal(val);
                  }}
                  className="spectionTask"
                >
                  <TabPane tab="daily_check" key="daily_check"></TabPane>
                  <TabPane tab="week_check" key="weekly_check"></TabPane>
                  <TabPane tab="month_check" key="monthly_check"></TabPane>
                  <TabPane tab="Real-time Inspection" key="real_time_check">
                    <div className="regularInspection">
                      <div>
                        <span className="label">Time Limit：</span>
                        <RangePicker
                          format="YYYY-MM-DD HH:mm:ss"
                          disabledDate={(e) => this.disabledDate(e)}
                          onChange={this.setDates}
                          showTime
                        />
                      </div>
                    </div>
                  </TabPane>
                </Tabs>
                <p className="line"></p>
                <Checkbox.Group
                  value={this.state.selected}
                  onChange={(val) => {
                    this.onSelectChange(val);
                  }}
                  disabled={
                    this.state.checkedKey === "real_time_check" ? false : true
                  }
                  className="systemCheckbox"
                >
                  <Checkbox value="System">
                    <h5>System</h5>
                    <p>CPU / Memory / IO / Network/Storage</p>
                  </Checkbox>
                  <Checkbox value="Database">
                    <h5>Database</h5>
                    <p>
                      Memory Context / Database Capacity / Database service /
                      Intance status
                    </p>
                  </Checkbox>
                </Checkbox.Group>
              </div>
            </Modal>
          </Card>
        ) : (
          ""
        )}
      </div>
    );
  }
}
