import React, { Component } from 'react';
import { Card, Table, message } from 'antd';
import ResizeableTitle from '../common/ResizeableTitle';
import { getNode } from '../../api/overview';
import iconokgreen from '../../assets/imgs/iconokgreen.png';
import iconstop from '../../assets/imgs/iconstop.png';

export default class NodeTable extends Component {
  constructor() {
    super()
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
  async getNode () {
    const { success, data, msg } = await getNode()
    if (success) {
      this.handleTableData(data.header, data.rows)
    } else {
      message.error(msg)
    }
  }
  handleTableData (header, rows) {
    let historyColumObj = {}
    let tableHeader = []
    header.forEach(item => {
      historyColumObj = {
        title: item.replace(/_/g, ' '),
        dataIndex: item,
        key: item,
        align:item === 'state' ? 'center' : 'left',
        ellipsis: true,
        width:item === 'state' ? '20%' : '40%',
        render: (row, record) => {
          if(item === 'state'){
            return <img src={record.state ? iconokgreen : iconstop} alt="" className='iconstyle'></img>
          } else {
            return row
          }
        },
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
  componentDidMount () {
    this.getNode()
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
      <div className='overviewTable'>
        <Table bordered components={this.components} columns={columns} dataSource={this.state.dataSource} size="small" rowKey={record => record.key} pagination={false} style={{ height: 198, overflowY: 'auto' }} scroll={{ x: '100%'}}/>
      </div>
    )
  }
}
