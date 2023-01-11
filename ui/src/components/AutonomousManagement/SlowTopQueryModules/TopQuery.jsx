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
      pageSize: 10,
      current: 1,
      total: 0,
      loadingTop: false,
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  async getTopQuery (params) {
    this.setState({ loadingTop: true })
    const { success, data, msg } = await getTopQueryInterface(params)
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
          pageSize: this.state.pageSize,
          current: this.state.current,
          total: res.length
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
  // 回调函数，切换下一页
  changePage(current,pageSize){
    let params = {
      current: current,
      pagesize: pageSize,
    };
    this.setState({
      current: current,
    });
    this.getTopQuery(params);
  }
    // 回调函数,每页显示多少条
    changePageSize(pageSize,current){
    // 将当前改变的每页条数存到state中
    this.setState({
      pageSize: pageSize
    });
    let params = {
      current: current,
      pagesize: pageSize,
    };
    this.getTopQuery(params);
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
  handleRefresh(){
    this.setState({
      pageSize: 10,
      current: 1,
    },()=>{
      this.getTopQuery({current: 1,pagesize: 10})
    })
  }
  componentDidMount () {
    this.getTopQuery({current: 1,pagesize: 10})
  }
  render () {
    const columns = this.state.columns.map((col, index) => ({
      ...col,
      onHeaderCell: column => ({
        width: column.width,
        onResize: this.handleResize(index)
      })
    }))
    const paginationProps = {
      showSizeChanger: true,
      showQuickJumper: true,
      showTotal: () => `Total ${this.state.total} items`,
      pageSize: this.state.pageSize,
      current: this.state.current,
      total: this.state.total,
      onShowSizeChange: (current,pageSize) => this.changePageSize(pageSize,current),
      onChange: (current,pageSize) => this.changePage(current,pageSize)
    };
    return (
      <div>
        <Card title="Top Query" extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />}>
          <Table bordered showSorterTooltip={false} components={this.components} columns={columns} dataSource={this.state.dataSource1} rowKey={record => record.key} pagination={paginationProps} loading={this.state.loadingTop} scroll={{ x: '100%'}} />
        </Card>
      </div>
    )
  }
}
