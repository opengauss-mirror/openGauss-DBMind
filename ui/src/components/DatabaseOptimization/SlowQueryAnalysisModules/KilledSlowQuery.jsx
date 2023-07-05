import React, { Component } from 'react';
import { Card, Table } from 'antd';

import ResizeableTitle from '../../common/ResizeableTitle';
import { formatTimestamp,formatTableTitle } from '../../../utils/function';

import { getKillSlowQueryInterface, getKillSlowQueryInterfaceCount} from '../../../api/autonomousManagement';
import Refresh from "../../../assets/imgs/Refresh.png";


export default class KilledSlowQueryTable extends Component {
  
  constructor() {
    super()
   
    this.state = {
      dataSource2: [],
      columns: [],
      pageSize: 10,
      current: 1,
      total: 0,
      loadingKill: false,
      tableWidth: 0
     
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };

  async getKillSlowQuery (params) {
    this.setState({loadingKill: true})
    const { success, data, msg } = await getKillSlowQueryInterface(params)
    if (success) {
      if (data.header.length > 0) {
        let historyColumObj = {}
        let tableHeader = []
        data.header.forEach((item) => {
          historyColumObj = {
            title: formatTableTitle(item),
            dataIndex: item,
            width: 180,
            key: item,
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
            tabledata['key'] = index
            if ((data.header[i] && data.header[i] === 'killed_time')) {
              tabledata[data.header[i]] = formatTimestamp(item[i])
            }
          }
          res.unshift(tabledata)
        });
        this.setState(() => ({
          loadingKill: false,
          dataSource2: res,
          columns: tableHeader,
          pageSize: this.state.pageSize,
          current: this.state.current
        }))
      } else {
        this.setState({
          loadingKill: false,
          dataSource2: [],
          columns: [],
        })
      }
    } else {
      this.setState({
        loadingKill: false,
        dataSource2: [],
        columns: [],
      })
      message.error(msg)
    }
  }
  async getKillSlowQueryCount (values) {
    let params = {
      start: this.state.startTime ? this.state.startTime : null,
      end: this.state.endTime ? this.state.endTime : null,
      query: (values && values.query) ? encodeURIComponent(values.query) : null,
      limit: (values && values.limit) ? values.limit : null
    }
    const { success, data, msg } = await getKillSlowQueryInterfaceCount(params)
    if (success) {
      this.setState(() => ({
        total: data
      }))
    } else {
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
    this.getKillSlowQuery(params);
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
    this.getKillSlowQuery(params);
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
  handleRefresh () {
    this.getKillSlowQuery({current: 1,pagesize: 10}).then(() => {
      this.getKillSlowQueryCount()
    })
  }
  componentDidMount () {
    this.getKillSlowQuery({current: 1,pagesize: 10}).then(() => {
      this.getKillSlowQueryCount()
    })
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
        <Card title="Killed Slow Query" className="tableSlowQuery"  extra={
            <div>
              <img src={Refresh} title='Refresh' alt="" onClick={() => this.handleRefresh()}></img>
            </div>
          }>
        <Table bordered components={this.components} columns={columns} dataSource={this.state.dataSource2}  loading={this.state.loadingKill} size="small" rowKey={record => record.key} pagination={paginationProps} scroll={{ x: '100%' }} />
         
        </Card>
      </div>
    )
  }
}
