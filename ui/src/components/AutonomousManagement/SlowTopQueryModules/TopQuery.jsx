import React, { Component } from 'react';
import { Card, message, Table, } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import ResizeableTitle from '../../common/ResizeableTitle';
import { getTopQueryInterface } from '../../../api/autonormousMangemant';
import { formatTableTitle } from '../../../utils/function';

export default class SlowTopQuery extends Component {
  constructor() {
    super()
    this.state = {
      dataSource1: [],
      columns: [],
      pagination1: {
        total: 0,
        defaultCurrent: 1
      },
      loadingTop: false,
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  async getTopQuery () {
    this.setState({ loadingTop: true })
    const { success, data, msg } = await getTopQueryInterface()
    if (success) {
      if (data.header.length > 0) {
        let historyColumObj = {}
        let tableHeader = []
        data.header.forEach((item, index) => {
          historyColumObj = {
            title: formatTableTitle(item),
            dataIndex: item,
            key: index,
            width: 180,
            ellipsis: true,
            sorter: (a, b) => {
              let aVal = a[item]
              let bVal = b[item]
              let c = isFinite(aVal),
                d = isFinite(bVal);
              return (c !== d && d - c) || (c && d ? aVal - bVal : aVal.localeCompare(bVal));
            }
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
          loadingTop: false,
          dataSource1: res,
          columns: tableHeader,
          pagination1: {
            total: res.length,
            defaultCurrent: 1
          }
        }))
      } else {
        this.setState({
          loadingTop: false,
          dataSource1: [],
          columns: [],
        })
      }
    } else {
      this.setState({
        loadingTop: false,
        dataSource1: [],
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
    this.getTopQuery()
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
        <Card title="Top Query" extra={<ReloadOutlined className="more_link" onClick={() => { this.getTopQuery() }} />}>
          <Table bordered showSorterTooltip={false} components={this.components} columns={columns} dataSource={this.state.dataSource1} rowKey={record => record.key} pagination={this.state.pagination1} loading={this.state.loadingTop} scroll={{ x: '100%'}} />
        </Card>
      </div>
    )
  }
}
