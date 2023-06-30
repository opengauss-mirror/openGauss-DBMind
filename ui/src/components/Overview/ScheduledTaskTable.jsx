import React, { Component } from 'react';
import { Modal, message, Table, InputNumber } from 'antd';
import ResizeableTitle from '../common/ResizeableTitle';
import { getTimedTaskStatus, getStartTimed, getStopTimed, getResetInterval } from '../../api/overview';
import Stopped from '../../assets/imgs/stop.png';
import Running from '../../assets/imgs/run.png';
import iconrun from '../../assets/imgs/Initiate.png';
import iconsetting from '../../assets/imgs/update.png';
import iconwait from '../../assets/imgs/Pause.png';
import { capitalizeFirst } from '../../utils/function';

const labelStyle = {width:160,float:'left',textAlign:'right',lineHeight:'32px'}
const inputStyle = {marginLeft:20,marginRight:20}
export default class ScheduledTaskTable extends Component {
  constructor() {
    super()
    this.state = {
      dataSource: [],
      columns: [],
      interval:0,
      name:'',
      isSettingVisible:false
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  handleTableData (header, rows) {
    let historyColumObj = {}
    let tableHeader = []
    header.push('setting')
    header.forEach(item => {
      historyColumObj = {
        title: capitalizeFirst(item.replace(/_/g, ' ')),
        dataIndex: item,
        key: item,
        ellipsis: true,
        width:item === 'name' ? '61%' : '13%',
        render: (row, record) => {
          if(item === 'current_status'){
            return <img src={record.current_status === 'Running' ? Running : Stopped}  title={record.current_status === 'Running' ? 'Running' : 'Stopped'} alt="" className='iconstyle'></img>
          } else if(item === 'setting'){
            return <span>
            <img src={iconsetting} title='Setting' alt="" className='iconstyle grayimg' style={{marginRight:12}} ></img>
            <img src={iconwait} title='Waiting' alt="" className='iconstyle grayimg' style={{marginRight:12}} ></img>
            <img  src={iconrun} title='Running' alt="" className='iconstyle grayimg' style={{marginRight:12}}></img>
            </span>
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
      res.push(tabledata)
    });
    this.setState(() => ({
      dataSource: res,
      columns: tableHeader,
    }))
  }
  async getTimedTaskStatus () {
    const { success, data, msg } = await getTimedTaskStatus()
    if (success) {
      this.handleTableData(data.header, data.rows)
    } else {
      message.error(msg)
    }
  }
  handleSetting(name,interval){
    this.setState({
      name:name,
      interval:interval,
      isSettingVisible: true,
    },()=>{

    })
  }
  async handleStart(name){
    const { success, data, msg } = await getStartTimed(name)
    if (success) {
      this.getTimedTaskStatus()
    } else {
      message.error(msg)
    }
  }
  async handleStopped(name){
    const { success, data, msg } = await getStopTimed(name)
    if (success) {
      this.getTimedTaskStatus()
    } else {
      message.error(msg)
    }
  }
  async handleSettingOk(){
    if(this.state.name && this.state.interval){
      let param = {
        funcname:this.state.name,
        seconds:this.state.interval
      }
      const { success, data, msg } = await getResetInterval(param)
      if (success) {
        this.getTimedTaskStatus()
        this.setState({
          isSettingVisible: false,
        },()=>{
          
        })
      } else {
        this.setState(() => ({
          dataSource: [],
          columns: [],
        }))
        message.error(msg)
      }
    } else {
      message.warning('The input value is a positive integer greater than 0')
    }
  }
  handleSettingCancel(){
    this.setState({
      isSettingVisible: false,
    })
  }
  handleChangeRate = (e) => {
    if(e){
      this.setState({interval: e})
    } else {
      message.warning('The input value is a positive integer greater than 0')
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
  refresh() {
    this.getTimedTaskStatus()
  }
  componentDidMount () {
    this.getTimedTaskStatus()
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
        <Table bordered components={this.components} columns={columns} dataSource={this.state.dataSource} size="small" rowKey={record => record.key} pagination={false} style={{ height: 450, overflowY: 'auto' }} scroll={{ x: '100%'}}/>
        <Modal title="Setting" destroyOnClose='true' visible={this.state.isSettingVisible} maskClosable = {false} onOk={() => this.handleSettingOk()}  onCancel={() => this.handleSettingCancel()}>
          <p><label style={labelStyle}>interval(second): </label><InputNumber style={inputStyle} min={0} onChange={(e) => this.handleChangeRate(e)} value={this.state.interval} /></p>
        </Modal>
      </div>
    )
  }
}
