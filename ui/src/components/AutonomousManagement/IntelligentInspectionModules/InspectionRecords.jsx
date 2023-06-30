import React, { Component } from "react";
import { Card, Table, Modal, message } from "antd";
import ResizeableTitle from "../../common/ResizeableTitle";
import Over from "../../../assets/imgs/over.png";
import Failure from "../../../assets/imgs/failure.png";
import Detail from "../../../assets/imgs/particular.png";
import Export from "../../../assets/imgs/Export.png";
import Refresh from "../../../assets/imgs/Refresh.png";

import { ExclamationCircleFilled } from "@ant-design/icons";
import { formatTableTitleToUpper } from "../../../utils/function";
import {
  deleteTask,
  getRealtimeInspectionList,
} from "../../../api/intelligentInspection";

const { confirm } = Modal;

export default class InspectionRecords extends Component {
  constructor(props) {
    super(props);
    this.state = {
      dataSource: [],
      columns: [],
      interval: 0,
      recordselectedRowKeys: [],
      selectRows: [],
      downloadData: {},
      showDetail: false,
      isShowRecord: false,
    };
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  // 详情
  goDetail(e) {
    this.setState(
      {
        showDetail: true,
      },
      () => {
        this.props.getData(this.state.showDetail, e, false);
      }
    );
  }
  // 导出
  handleDownload(e) {
    this.setState(
      {
        showDetail: true,
      },
      () => {
        this.props.getData(this.state.showDetail, e, true);
      }
    );
  }
  handleTableData(header, rows) {
    let recordColumObj = {};
    let tableHeader = [];
    header.push("operate");
    header.forEach((item) => {
      recordColumObj = {
        title: formatTableTitleToUpper(item),
        dataIndex: item,
        key: item,
        ellipsis: true,
        width: "13%",
        render: (row, record, index) => {
          if (item === "state") {
            let src = record.state === "success" ? Over : Failure;
            return <img src={src} alt="" className="iconstyle"></img>;
          } else if (item === "operate" && (record.state === "success")) {
            return (
              <>
                <img
                  src={Detail}
                  alt=""
                  title="Detail"
                  onClick={() => this.goDetail(record)}
                ></img>

                <img
                  src={Export}
                  alt=""
                  title="Preview"
                  onClick={() => this.handleDownload(record)}
                ></img>
              </>
            );
          } else {
            return row;
          }
        },
      };
      tableHeader.push(recordColumObj);
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
      this.setState({ isShowRecord: true });
      this.handleTableData(data.header, data.rows);
    } else {
      message.error(msg);
    }
  }

  onSelectChange(selectedRowKeys, selectRows) {
    this.setState({
      recordselectedRowKeys: selectedRowKeys,
      selectRows: selectRows
    });
  }
  clearSelect() {
    this.setState({
      recordselectedRowKeys: []
    })
  }
  refresh() {
    this.getRealtimeInspectionList();
  }
  //  行点击事件
  clickRow(event, record) {
    this.setState({
      downloadData: record,
    })
  }
  deleteBtn() {
    confirm({
      title: "Are you sure delete this task?",
      icon: <ExclamationCircleFilled />,
      content: "Some descriptions",
      okText: "Yes",
      okType: "danger",
      cancelText: "No",
      onOk: () => {
        this.confirm();
      },
      onCancel() { },
    });
  }
  // 删除报告
  async confirm() {
    let idMap = []
    this.state.selectRows.forEach(item => {
      idMap.push(item.id)
    })
    const { success, data, msg } = await deleteTask(
      idMap.toString()
    );
    if (success) {
      message.success("DELETE SUCCESS");
      this.getRealtimeInspectionList();
      this.setState({
        recordselectedRowKeys: []
      })
    } else {
      message.error(msg);
    }
  }
  componentDidMount() {
    this.getRealtimeInspectionList();
  }
  render() {
    const rowSelection = {
      type: "checkbox",
      selectedRowKeys: this.state.recordselectedRowKeys,
      onChange: (val, e) => {
        this.onSelectChange(val, e);
      },
    };
    const columns = this.state.columns.map((col, index) => ({
      ...col,
    }));
    return (
      <div className="InspectionRecords">
        {this.state.isShowRecord ? (
          <Card
            title="Inspection Records"
            extra={
              <div>
                <button
                  className="delBtn"
                  onClick={() => this.deleteBtn()}
                  type="dashed"
                >
                  Delete Record
                </button>
                <img
                  src={Refresh}
                  alt=""
                  style={{ marginLeft: 6 }}
                  onClick={() => this.refresh()}
                ></img>
              </div>
            }
          >
            <Table
              bordered
              rowSelection={rowSelection}
              components={this.components}
              columns={columns}
              dataSource={this.state.dataSource}
              size="small"
              rowKey={(record) => record.key}
              pagination={false}
              onRow={(record) => ({
                onClick: (event) => this.clickRow(event, record), // 点击行
              })}
              style={{ height: 170, overflowY: "auto" }}
              scroll={{ x: "100%" }}
            />
          </Card>
        ) : (
          ""
        )}
      </div>
    );
  }
}
