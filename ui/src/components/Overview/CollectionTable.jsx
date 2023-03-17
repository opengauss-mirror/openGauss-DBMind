import React, { Component } from 'react';
import { Card, Table, message } from 'antd';
import ResizeableTitle from '../common/ResizeableTitle';
import { getCollectionTable } from '../../api/overview';
import iconok from '../../assets/imgs/iconok.png';
import iconstop from '../../assets/imgs/iconstop.png';

export default class CollectionTable extends Component {
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
  async getCollectionTable () {
    const { success, data, msg } = await getCollectionTable()
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
        align:item === 'is_alive' ? 'center' : 'left',
        ellipsis: true,
        width:item === 'is_alive' ? '20%' : '40%',
        render: (row, record) => {
          if(item === 'is_alive'){
            return <img src={record.is_alive ? iconok : iconstop} alt="" className='iconstyle'></img>
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
      debugger
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
    this.getCollectionTable()
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
