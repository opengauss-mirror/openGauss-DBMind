import React, { Component } from 'react';
import { Card, message, Table } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import ResizeableTitle from '../../common/ResizeableTitle';
import { getLockingQueryInterface } from '../../../api/autonormousMangemant';
import { formatTableTitle } from '../../../utils/function';

export default class ActiveSql extends Component {
  constructor() {
    super()
    this.state = {
      lockDataSource: [],
      columns: [],
      lockPagination: {
        total: 0,
        defaultCurrent: 1
      },
      loadingLock: false
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  async getLockingQueryData () {
    this.setState({loadingLock: true})
    const { success, data,msg } = await getLockingQueryInterface()
    if (success) {
      if (data.header && data.header.length > 0) {
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
            tabledata[data.header[i]] = item[i] + ''
            tabledata['key'] = index + ''
          }
          res.push(tabledata)
        });
        this.setState(() => ({
          loadingLock: false,
          lockDataSource: res,
          columns: tableHeader,
          lockPagination: {
            total: res.length,
            defaultCurrent: 1
          }
        }))
      } else {
        this.setState({
          loadingLock: false,
          lockDataSource: [],
          columns: [],
        })
      }
    } else {
      this.setState({
        loadingLock: false,
        lockDataSource: [],
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
    this.getLockingQueryData()
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
        <Card title="Locking Query" extra={<ReloadOutlined className="more_link" onClick={() => { this.getLockingQueryData() }} />}>
          <Table bordered showSorterTooltip={false} components={this.components} columns={columns} dataSource={this.state.lockDataSource} rowKey={record => record.key} pagination={this.state.lockPagination} loading={this.state.loadingLock} scroll={{ x: '100%'}}/>
        </Card>
      </div>
    )
  }
}
