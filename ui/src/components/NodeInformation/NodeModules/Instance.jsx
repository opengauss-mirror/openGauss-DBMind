import React, { Component } from 'react';
import { Card } from 'antd';
import { Table, message } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import ResizeableTitle from '../../common/ResizeableTitle';
import { getstatusNodeInterface } from '../../../api/clusterInformation';
import { formatSecond, formatTableTitle } from '../../../utils/function';

export default class Instances extends Component {
  constructor() {
    super()
    this.state = {
      data: [],
      columns: [],
      loading: false,
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  async getInstanceTable () {
    this.setState({ loading: true })
    const { success, data, msg } = await getstatusNodeInterface()
    if (success) {
      if (data.node_list.length > 0) {
        let arr = [JSON.parse(JSON.stringify(...data.node_list))]
        let columnsArr = []
        Object.keys(data.node_list[0]).forEach(function (key) {
          let obj = {
            title: formatTableTitle(key),
            dataIndex: key,
            key: key,
            ellipsis: true,
            width: 180
          }
          columnsArr.push(obj)
        })
        arr.map((item, index) => {
          item['key'] = index
          return item.uptime = formatSecond(item.uptime)
        })
        this.setState(() => ({
          loading: false,
          columns: columnsArr,
          data: arr
        }))
      } else {
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
    this.getInstanceTable()
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
        <Card style={{ height: 490 }} title="openGauss Instance" extra={<ReloadOutlined className="more_link" onClick={() => { this.getInstanceTable() }} />}>
          <Table bordered components={this.components} columns={columns} dataSource={this.state.data} rowKey={record => record.key} loading={this.state.loading} scroll={{ x: '100%' }} />
        </Card>
      </div>
    )
  }
}
