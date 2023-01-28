import React, { Component } from 'react';
import { Card, Table } from 'antd';
import PropTypes from 'prop-types';
import ResizeableTitle from '../../common/ResizeableTitle';

export default class InstanceResource extends Component {
  static propTypes={
    instanceResource:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      dataSource: [],
      columns: []
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

      }
      tableHeader.push(historyColumObj)
    })
    let res = []
    rows.forEach((item, index) => {
      let tabledata = {}
      for (let i = 0; i < header.length; i++) {
        tabledata[header[i]] = item[i]
      }
      tabledata['key'] = index + ''
      res.push(tabledata)
    });
    this.setState(() => ({
      dataSource: res,
      columns: tableHeader,
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
    this.handleTableData(nextProps.instanceResource.header, nextProps.instanceResource.rows)
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
      <div className="mb-20" style={{ textAlign: 'center' }}>
        <Card title="Instance Resource">
          <Table bordered components={this.components} columns={columns} dataSource={this.state.dataSource} size="small" rowKey={record => record.key} pagination={false}  style={{ height: 264, overflowY: 'auto' }} scroll={{ x: '100%'}}/>
        </Card>
      </div>
    )
  }
}
