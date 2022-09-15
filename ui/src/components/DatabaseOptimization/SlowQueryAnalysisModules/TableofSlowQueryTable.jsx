import React, { Component } from 'react';
import { Card, Table } from 'antd';
import PropTypes from 'prop-types';
import ResizeableTitle from '../../common/ResizeableTitle';
import { formatTimestamp } from '../../../utils/function';
export default class TableofSlowQueryTable extends Component {
  static propTypes={
    tableOfSlowQuery:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      dataSource: [],
      columns: [],
      pagination: {
        total: 0,
        defaultCurrent: 1
      },
      tableWidth: 0
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  handleTableData (header, rows) {
    let historyColumObj = {}
    let tableHeader = []
    header.forEach(item => {
      historyColumObj = {
        title: item.replace(/_/g, ' '),
        dataIndex: item,
        key: item,
        ellipsis: true,
        width: 180
      }
      tableHeader.push(historyColumObj)
    })
    let res = []
    rows.forEach((item, index) => {
      let tabledata = {}
      for (let i = 0; i < header.length; i++) {
        tabledata['key'] = index + ''
        tabledata[header[i]] = item[i]
        if (header[i] && header[i] === 'start_at') {
          tabledata[header[i]] = formatTimestamp(item[i] + '')
        }
      }
      res.push(tabledata)
    });
    this.setState(() => ({
      dataSource: res,
      columns: tableHeader,
      pagination: {
        total: res.length,
        defaultCurrent: 1
      }
    }))
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
  UNSAFE_componentWillReceiveProps (nextProps) {
    this.handleTableData(nextProps.tableOfSlowQuery.header, nextProps.tableOfSlowQuery.rows)
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
        <Card title="Table of Recent Slow Query">
          <Table bordered components={this.components} columns={columns} dataSource={this.state.dataSource} size="small" rowKey={record => record.key} pagination={this.state.pagination} scroll={{ x: '100%'}} />
        </Card>
      </div>
    )
  }
}
