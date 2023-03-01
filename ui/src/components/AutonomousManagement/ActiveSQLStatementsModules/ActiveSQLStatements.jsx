import React, { Component } from 'react';
import { Card, message, Table } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import ResizeableTitle from '../../common/ResizeableTitle';
import { getActiveSQLDataInterface } from '../../../api/autonomousManagement';
import { formatTableTime, formatTableTitle } from '../../../utils/function';

export default class ActiveSql extends Component {
  constructor() {
    super()
    this.state = {
      dataSource: [],
      columns: [],
      pagination: {
        total: 0,
        defaultCurrent: 1
      },
      loadingActiveSql: false,
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  async getActiveSQLData () {
    this.setState({loadingActiveSql: true})
    const { success, data, msg } = await getActiveSQLDataInterface()
    if (success) {
      if (data.header.length > 0) {
        let historyColumObj = {}
        let tableHeader = []
        data.header.forEach((item) => {
          historyColumObj = {
            title: formatTableTitle(item),
            dataIndex: item,
            ellipsis: true,
            width: 180
          }
          tableHeader.push(historyColumObj)
        })
        let res = []
        data.rows.forEach((item, index) => {
          let tabledata = {}
          for (let i = 0; i < data.header.length; i++) {
            tabledata[data.header[i]] = item[i]
            tabledata['key'] = index + ''
          }
          res.push(tabledata)
        });
        let formatData = formatTableTime(res)
        this.setState(() => ({
          loadingActiveSql: false,
          dataSource: formatData,
          columns: tableHeader,
          pagination: {
            total: res.length,
            defaultCurrent: 1
          }
        }))
      } else {
        this.setState({
          loadingActiveSql: false,
          dataSource: [],
          columns: [],
        })
      }
    } else {
      this.setState({
        loadingActiveSql: false,
        dataSource: [],
        columns: [],
      })
      message.error(msg)
    }
  }
  handleResize = index => (e, { size }) => {
    this.setState(({ columns }) => {
      const nextColumns = [...columns];
      nextColumns[index] = {
        ...nextColumns[index],
        width: size.width,
      };
      return { columns: nextColumns };
    });
  };
  componentDidMount () {
    this.getActiveSQLData()
  }
  render () {
    const columns = this.state.columns.map((col, index) => ({
      ...col,
      onHeaderCell: column => ({
        width: column.width,
        onResize: this.handleResize(index)
      })
    }))
    return (
      <div>
        <Card title="Active SQL Statements" extra={<ReloadOutlined className="more_link" onClick={() => { this.getActiveSQLData() }} />} className="mb-20" >
          <Table bordered showSorterTooltip={false} components={this.components} columns={columns} dataSource={this.state.dataSource} rowKey={record => record.key} pagination={this.state.pagination} loading={this.state.loadingActiveSql} scroll={{ x: '100%'}} />
        </Card>
      </div>
    )
  }
}
