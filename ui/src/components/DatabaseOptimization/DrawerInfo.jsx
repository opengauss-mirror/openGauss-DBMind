import React, { Component } from 'react';
import { message, Select, Table, DatePicker, Tabs } from 'antd';
import Analyze from '../../assets/imgs/Analyze.png';
import { CloseOutlined, ReloadOutlined } from '@ant-design/icons';
import { getCollect } from '../../api/databaseOptimization';
import '../../assets/css/common.css'
import '../../assets/css/main/databaseOptimization.css'
import { formatTableTitle, formatTimestamp } from '../../utils/function';
import DrawerStatistics from '../DatabaseOptimization/DrawerStatistics';

const { TabPane } = Tabs;
export default class DrawerInfo extends Component {
  constructor(props) {
    super(props)
    this.state = {
      dataSource: [],
      columns: [],
      loading: false,
    }
  }
  async getCollect (params) {
    this.setState({ loading: true })
    const { success, msg, data } = await getCollect(params)
    if (success) {
      if (data.rows.length > 0) {
        let historyColumObj = {}
        let tableHeader = []
        data.header.push('operation')
        data.header.forEach(item => {
          historyColumObj = {
            title: formatTableTitle(item),
            dataIndex: item,
            width: 180,
            key: item,
            ellipsis: true,
            align:item === 'operation' ? 'center' : 'left',
            fixed:item === 'operation' ? 'right' : 'false',
            render: (row, record) => {
              if(item === 'operation'){
                return <img src={Analyze} disabled alt="" onClick={(e) => {e.stopPropagation();this.props.isModal(row, record)}} ></img>
              } else {
                return row
              }
            },
          }
          tableHeader.push(historyColumObj)
        })
        let res = []
        data.rows.forEach((item, index) => {
          let tabledata = {}
          for (let i = 0; i < data.header.length; i++) {
            tabledata[data.header[i]] = item[i]
            tabledata['key'] = index
          }
          res.push(tabledata)
        });
        this.setState(() => ({
          loading: false,
          dataSource: res,
          columns: tableHeader,
        }))
      } else {
        this.setState({
          loading: false,
          dataSource: [],
          columns: [],
        })
      }
    } else {
      this.setState({
        loading: false,
        dataSource: [],
        columns: [],
      })
      message.error(msg)
    }
  }
  componentDidMount () {
    let params = {
      unique_sql_id:this.props.uniqueSqlId,
      start_time:this.props.startTime,
      end_time:this.props.endTime
    }
    this.getCollect(params)
  }
  render () {
    return (
      <Tabs   size={'large'} tabBarExtraContent={<CloseOutlined onClick={() => this.props.onClose()} /> } >
          <TabPane
            tab={<span>Statistics</span>}
            key="1"
          >
            {this.state.dataSource.length ? <DrawerStatistics dataSource={this.state.dataSource.length ? this.state.dataSource : []} columns={this.state.columns ? this.state.columns : []} /> : 
            <DrawerStatistics dataSource={[]} columns={[]} />}
          </TabPane>
          <TabPane
            tab={<span>Details</span>}
            key="2"
          >
            <Table size="small" bordered dataSource={this.state.dataSource} columns={this.state.columns} rowKey={record => record.key} loading={this.state.loading} scroll={{ x: '100%' }} />
          </TabPane>
        </Tabs> 
    )
  }
}
