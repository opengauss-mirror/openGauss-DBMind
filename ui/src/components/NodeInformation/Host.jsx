import React, { Component } from 'react';
import { Card, message, Progress, Table } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { getHostListInterface } from '../../api/clusterInformation';
import { formatTableTime, formatTableTitle } from '../../utils/function';
import ResizeableTitle from '../common/ResizeableTitle';

export default class Host extends Component {
  constructor() {
    super()
    this.state = {
      data: [],
      columns: [],
      pagination: {
        total: 0,
        defaultCurrent: 1
      },
      loading: false,
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  async getHostList () {
    this.setState({ loading: true })
    const { success, data, msg } = await getHostListInterface()
    if (success) {
      if(JSON.stringify(data) !== '{}'){
      let arr = []
      let columnArr = []
      Object.keys(data).forEach(function (key) {
        data[key]['ip'] = key
        arr.push(data[key])
      })
      let columnName = {}
      Object.keys(arr[0]).forEach(function (key) {
        columnName = {
          title: formatTableTitle(key),
          dataIndex: key,
          key: key,
          ellipsis: true,
        }
        columnArr.push(columnName)
      })
      columnArr.forEach((item) => {
        if (item.key === 'os_cpu_usage' || item.key === 'os_mem_usage' || item.key === 'os_disk_usage') {
          item.render = (num) => {
            return (
              <Progress percent={(num * 100).toFixed(2)} strokeColor="#ff4d4f" size="small" />
            )
          }
        }
        item.width = 160
      })
      formatTableTime(arr)
      this.setState(() => ({
        loading: false,
        data: arr,
        columns: columnArr,
        pagination: {
          total: arr.length,
          defaultCurrent: 1
        },
      }))
    }else{
      this.setState({ loading: false })
    }
    } else {
      this.setState({ loading: false })
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
    this.getHostList()
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
        <Card title="Node Information" extra={<ReloadOutlined className="more_link" onClick={() => { this.getHostList() }} />} style={{ height: 800 }}>
          <Table bordered loading={this.state.loading} components={this.components} columns={columns} dataSource={this.state.data} rowKey={record => record.ip} pagination={this.state.pagination} scroll={{ x: '100%' }} />
        </Card>
      </div>
    )
  }
}
