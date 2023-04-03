import React, { Component } from 'react';
import { message, Table, Row, Col } from 'antd';
import ResizeableTitle from '../common/ResizeableTitle';
import { getLockingQueryInterface } from '../../api/autonomousManagement';
import { formatTableTitle } from '../../utils/function';

export default class LockInformation extends Component {
  constructor(props) {
    super(props)
    this.state = {
      lockDataSource: [],
      columns: [],
      lockPagination: {
        total: 0,
        defaultCurrent: 1
      },
      loadingLock: false,
      dataLocks:0,
      dataTableLevelLocks:0,
      dataRowLevelLocks:0,
      dataOtherLocks:0,
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
        let res = [],dataTableLevelLocks = 0,dataRowLevelLocks = 0,dataOtherLocks = 0
        data.rows.forEach((item, index) => {
          let tabledata = {}
          for (let i = 0; i < data.header.length; i++) {
            tabledata[data.header[i]] = item[i] + ''
            tabledata['key'] = index + ''
          }
          res.push(tabledata)
          if(item[4] === "r" && item[5] === "relation"){
            dataTableLevelLocks++
          }
          if(item[5] === "tuple"){
            dataRowLevelLocks++
          }
        });
        dataOtherLocks = data.rows.length - dataTableLevelLocks - dataRowLevelLocks
        this.setState(() => ({
          loadingLock: false,
          lockDataSource: res,
          columns: tableHeader,
          lockPagination: {
            total: res.length,
            defaultCurrent: 1
          },
          dataLocks:data.rows.length,
          dataTableLevelLocks:dataTableLevelLocks,
          dataRowLevelLocks:dataRowLevelLocks,
          dataOtherLocks:dataOtherLocks,
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
  componentDidUpdate(prevProps) {
    if( prevProps.tabkey !== this.props.tabkey || prevProps.tabChildkey !== this.props.tabChildkey) {
      if(this.props.tabkey === "3" && this.props.tabChildkey === "1" ){
        this.getLockingQueryData()
      }
    }
  }
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
      <div className='lockinformation'>
          <Row gutter={10} style={{marginBottom:16}}>
          <Col className="gutter-row cpuborder" span={6}>
            <div className='lockstyle'><span className='spanleft'>{'Total Number Of Locks'}</span><span className='spanright'>{this.state.dataLocks}</span></div>
          </Col>
          <Col className="gutter-row cpuborder" span={6}>
            <div className='lockstyle'><span className='spanleft'>{'Total Number Of Table-Level Locks'}</span><span className='spanright'>{this.state.dataTableLevelLocks}</span></div>
          </Col>
          <Col className="gutter-row cpuborder" span={6}>
          <div className='lockstyle'><span className='spanleft'>{'Total Number Of Row-Level Locks'}</span><span className='spanright'>{this.state.dataRowLevelLocks}</span></div>
          </Col>
          <Col className="gutter-row cpuborder" span={6}>
          <div className='lockstyle'><span className='spanleft'>{'Total Number Of Other Locks'}</span><span className='spanright'>{this.state.dataOtherLocks}</span></div>
          </Col>
        </Row>
          <Table bordered showSorterTooltip={false} components={this.components} columns={columns} dataSource={this.state.lockDataSource} rowKey={record => record.key} pagination={this.state.lockPagination} loading={this.state.loadingLock} scroll={{ x: '100%'}}/>
      </div>
    )
  }
}