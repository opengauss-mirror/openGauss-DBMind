import React, { Component } from 'react';
import { Card, Table } from 'antd';
import PropTypes from 'prop-types';
import ResizeableTitle from '../../../common/ResizeableTitle';

export default class DistributionSlowSql extends Component {
  static propTypes={
    distributionSlowSql:PropTypes.object.isRequired
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
    if(rows){
      rows.forEach((item, index) => {
        let tabledata = {}
        for (let i = 0; i < header.length; i++) {
          tabledata[header[i]] = item[i]
        }
        tabledata['key'] = index + ''
        res.push(tabledata)
      });
    }
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
    this.handleTableData(nextProps.distributionSlowSql.header, nextProps.distributionSlowSql.rows)
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
        <Card title="Distribution Slow Sql" className="tps tableHeight mb-20">
          <Table bordered components={this.components} columns={columns} dataSource={this.state.dataSource} size="small" rowKey={record => record.key} pagination={false} style={{ height: 200, overflowY: 'auto' }} scroll={{ x: '100%'}}/>
        </Card>
      </div>
    )
  }
}
