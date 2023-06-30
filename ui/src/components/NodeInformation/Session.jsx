import React, { Component } from 'react';
import { Card, message, Table, Popconfirm, Button, Modal, Dropdown, Input } from 'antd';
import ResizeableTitle from '../common/ResizeableTitle';
import { DownOutlined } from '@ant-design/icons';
import SqlPlan from './SqlPlan';
import { getActiveSQLDataInterface, getKillData, getDetailsData, getExecutionPlan } from '../../api/autonomousManagement';
import { formatTableTime, formatTableTitle, capitalizeFirst } from '../../utils/function';

const { TextArea } = Input;
const items = [
  {
    key: '1',
    label: 'Details',
  },
  {
    key: '2',
    label: 'ExecutionPlan',
  }]
export default class Session extends Component {
  constructor() {
    super()
    this.state = {
      dataSource: [],
      columns: [],
      pagination: {
        total: 0,
        defaultCurrent: 1
      },
      dataSourceDetails: [],
      columnsDetails: [],
      paginationDetails: {
        total: 0,
        defaultCurrent: 1
      },
      loadingActiveSql: false,
      isModalVisible: false,
      isPlanVisible: false,
      planData:''
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  handleChildActions = (record,key)=>{
    if (key==='1'){
      this.isModal(record)
    } else if(key==='2'){
      this.isPlan(record)
    }
  }
  async getSessionData() {
    this.setState({ loadingActiveSql: true })
    const { success, data, msg } = await getActiveSQLDataInterface()
    if (success) {
      if (data.header.length > 0) {
        let historyColumObj = {}
        let tableHeader = []
        data.header.push('operation')
        data.header.forEach((item) => {
          historyColumObj = {
            title: formatTableTitle(capitalizeFirst(item)),
            dataIndex: item,
            ellipsis: true,
            width: 180,
            align:item === 'operation' ? 'center' : 'left',
            fixed:item === 'operation' ? 'right' : 'false',
            render: (row, record) => {
              if(item === 'operation'){
                return <div><Popconfirm title="end the session" description="Are you sure you want to end this session?" onConfirm={() => {this.isKill(record)}}
                   onCancel={this.cancel()} okText="Yes" cancelText="No" > <Button type="primary" style={{marginRight:10}} >Kill</Button>
                 </Popconfirm><Dropdown
                menu={{items,onClick: ({key}) => {
                  return  this.handleChildActions(record, key);
                } }
                }
              >
                <span>
                  More <DownOutlined />
                </span>
              </Dropdown></div>
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
            tabledata['key'] = index + ''
          }
          res.push(tabledata)
        });
        let formatData = formatTableTime(res)
        this.setState(() => ({
          loadingActiveSql: false,
          dataSource: formatData,
          columns: tableHeader,
          pagination: {
            total: res.length,
            defaultCurrent: 1
          }
        }))
      } else {
        this.setState({
          loadingActiveSql: false,
          dataSource: [],
          columns: [],
        })
      }
    } else {
      this.setState({
        loadingActiveSql: false,
        dataSource: [],
        columns: [],
      })
      message.error(msg)
    }
  }
  async isKill(item) {
    const { success, data, msg } = await getKillData(item.pid)
    if (success) {
      if(data.rows[0]){
        this.getSessionData()
        message.success('Ended successfully')
      } else {
        message.warning('Ending failed')
      }
    } else {
      message.error(msg)
    }
  }
  handleCancel = () => {
    this.setState({
      isModalVisible: false
    })
  }
  handlePlanCancel = () => {
    this.setState({
      isPlanVisible: false
    })
  }
  async isModal(item) {
    this.setState({
      isModalVisible: true
    })
    let params = {
      pid:item.pid,
      sessionid:item.sessionid
    }
    const { success, data, msg } = await getDetailsData(params)
    if (success) {
      if (data.header) {
        let historyColumObj = {}
        let tableHeader = []
        data.header.push('operation')
        data.header.forEach((item) => {
          historyColumObj = {
            title: formatTableTitle(capitalizeFirst(item)),
            dataIndex: item,
            ellipsis: true,
            width: 180,
          }
          tableHeader.push(historyColumObj)
        })
        let res = []
        data.rows.forEach((item, index) => {
          let tabledata = {}
          for (let i = 0; i < data.header.length; i++) {
            tabledata[data.header[i]] = item[i]
            tabledata['key'] = index + ''
          }
          res.push(tabledata)
        });
        let formatData = formatTableTime(res)
        this.setState(() => ({
          dataSourceDetails: formatData,
          columnsDetails: tableHeader,
          paginationDetails: {
            total: res.length,
            defaultCurrent: 1
          }
        }))
      } else {
        this.setState({
          dataSourceDetails: [],
          columnsDetails: [],
        })
      }
    } else {
      this.setState({
        dataSourceDetails: [],
        columnsDetails: [],
      })
      message.error(msg)
    }
  }
  async isPlan(item) {
    let params = {
      query:item.query,
      db_name:item.datname,
      schema_name:''
    }
    const { success, data, msg } = await getExecutionPlan(params)
    if (success) {
      this.setState(() => ({
        isPlanVisible: true,
        planData: data
      }))
    } else {
      this.setState({
        planData: ''
      })
      message.error(msg)
    }
  }
  cancel(){}
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
  componentDidUpdate(prevProps) {
    if( prevProps.tabkey !== this.props.tabkey || prevProps.tabSessionkey !== this.props.tabSessionkey) {
      if(this.props.tabkey === "6" && this.props.tabSessionkey === "1" ){
        this.getSessionData()
      }
    }
  }
  componentDidMount() {
    this.getSessionData()
  }

  render() {
    const columns = this.state.columns.map((col, index) => ({
      ...col,
      onHeaderCell: column => ({
        width: column.width,
        onResize: this.handleResize(index)
      })
    }))
    const columnsDetails = this.state.columnsDetails.map((col, index) => ({
      ...col,
      onHeaderCell: column => ({
        width: column.width,
        onResize: this.handleResize(index)
      })
    }))
    return (
      <div>
        <Table bordered showSorterTooltip={false} components={this.components} columns={columns} dataSource={this.state.dataSource} rowKey={record => record.key} pagination={this.state.pagination} loading={this.state.loadingActiveSql} scroll={{ x: '100%'}} />
        <Modal title="Abnormal Root Cause Analysis" style={{maxWidth: "60vw"}} bodyStyle={{overflowY: "auto", background: '#f1f1f1'}} width="60vw" okButtonProps={{ style: { display: 'none' } }} 
         destroyOnClose='true' visible={this.state.isModalVisible} maskClosable = {false} centered='true' onCancel={() => this.handleCancel()}>
           <Table bordered showSorterTooltip={false} components={this.components} columns={columnsDetails} dataSource={this.state.dataSourceDetails} rowKey={record => record.key} pagination={this.state.paginationDetails} scroll={{ x: '100%'}} />
        </Modal>
        <Modal title={`Execution Plan (SQL: ${this.state.planData[1]} , schame: ${this.state.planData[2]})`} style={{maxWidth: "60vw"}} bodyStyle={{overflowY: "auto",height:600, background: '#f1f1f1'}} width="60vw" okButtonProps={{ style: { display: 'none' } }} 
         destroyOnClose={true} visible={this.state.isPlanVisible} maskClosable = {false} centered='true' onCancel={() => this.handlePlanCancel()}>
           <SqlPlan planData = {this.state.planData[3]} />
        </Modal>
      </div>
    )
  }
}
