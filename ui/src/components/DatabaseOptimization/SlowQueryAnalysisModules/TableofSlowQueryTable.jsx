import React, { Component } from "react";
import { Card, Table } from "antd";
import PropTypes from "prop-types";
import ResizeableTitle from "../../common/ResizeableTitle";
import { formatTimestamp } from "../../../utils/function";
import { getSlowQueryRecent } from "../../../api/databaseOptimization";
import Export from "../../../assets/imgs/Export.png";
import Refresh from "../../../assets/imgs/Refresh.png";

export default class TableofSlowQueryTable extends Component {
  static propTypes = {
    tableOfSlowQuery: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      dataSource: [],
      columns: [],
      pageSize: 10,
      current: 1,
      total: 0,
      tableWidth: 0,
      slowQueryDown: []
    };
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  handleTableData(header, rows, total) {
    let historyColumObj = {};
    let tableHeader = [];
    header.forEach((item) => {
      historyColumObj = {
        title: item.replace(/_/g, " "),
        dataIndex: item,
        key: item,
        ellipsis: true,
        width: 180,
      };
      tableHeader.push(historyColumObj);
    });
    let res = [], downData = [];
    rows.forEach((item, index) => {
      let tabledata = {};
      for (let i = 0; i < header.length; i++) {
        tabledata["key"] = index + "";
        tabledata[header[i]] = item[i];
        if (header[i] && header[i] === "start_at") {
          tabledata[header[i]] = formatTimestamp(item[i] + "");
        }
        downData.push(item[i])

      }
      res.push(tabledata);
    });
    this.setState(() => ({
      dataSource: res,
      columns: tableHeader,
      pageSize: this.state.pageSize,
      current: this.state.current,
      total: total,
      slowQueryDown: downData
    }));
  }
  async getSlowQueryRecent(params) {
    const { success, data, msg } = await getSlowQueryRecent(params);
    if (success) {
      this.handleTableData(data.header, data.rows, this.state.total);
    } else {
      message.error(msg);
    }
  }
  // 导出
  handleDownload() {
    //获取表格
    var exportFileContent = document.getElementById('slowTable').outerHTML;
    var blob = new Blob([exportFileContent], { type: "application/vnd.ms-excel" });         //解决中文乱码问题
    blob = new Blob([String.fromCharCode(0xFEFF), blob], { type: blob.type });
    var link = window.URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.download = 'result';
    a.href = link;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);


  }
  // 回调函数，切换下一页
  changePage(current, pageSize) {
    let params = {
      current: current,
      pagesize: pageSize,
    };
    this.setState({
      current: current,
    });
    this.getSlowQueryRecent(params);
  }
  // 回调函数,每页显示多少条
  changePageSize(pageSize, current) {
    // 将当前改变的每页条数存到state中
    this.setState({
      pageSize: pageSize,
    });
    let params = {
      current: current,
      pagesize: pageSize,
    };
    this.getSlowQueryRecent(params);
  }
  handleResize = (index) => (e, { size }) => {
    this.setState(({ columns }) => {
      const nextColumns = [...columns];
      nextColumns[index] = {
        ...nextColumns[index],
        width: size.width,
      };
      return { columns: nextColumns };
    });
  };
  UNSAFE_componentWillReceiveProps(nextProps) {
    if (nextProps.tableOfSlowQuery) {
      this.handleTableData(
        nextProps.tableOfSlowQuery.header,
        nextProps.tableOfSlowQuery.rows,
        nextProps.tableOfSlowQuery.total
      );
    }
  }
  handleRefresh() {
    this.getSlowQueryRecent({ current: 1, pagesize: 10 });
  }
  render() {
    const columns = this.state.columns.map((col, index) => ({
      ...col,
      onHeaderCell: (column) => ({
        width: column.width,
        onResize: this.handleResize(index),
      }),
    }));
    const paginationProps = {
      showSizeChanger: true,
      showQuickJumper: true,
      showTotal: () => `Total ${this.state.total} items`,
      pageSize: this.state.pageSize,
      current: this.state.current,
      total: this.state.total,
      onShowSizeChange: (current, pageSize) =>
        this.changePageSize(pageSize, current),
      onChange: (current, pageSize) => this.changePage(current, pageSize),
    };
    return (
      <div>
        <Card
          title="Table of Recent Slow Query"
          className="mb-10 tableSlowQuery"
          extra={
            <div>
              <img
                src={Export}
                alt=""
                title="Export"
                onClick={() => this.handleDownload()}
              ></img>
              <img
                src={Refresh}
                alt=""
                title="Refresh"
                onClick={() => this.handleRefresh()}
              ></img>
            </div>
          }
        >
          <Table
            id="slowTable"
            bordered
            components={this.components}
            columns={columns}
            dataSource={this.state.dataSource}
            size="small"
            rowKey={(record) => record.key}
            pagination={paginationProps}
            scroll={{ x: "100%" }}
          />
        </Card>
      </div>
    );
  }
}
