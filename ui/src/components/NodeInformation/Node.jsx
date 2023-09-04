import React, { Component } from 'react';
import { Tabs, Select, message, DatePicker } from 'antd';
import moment from 'moment';
import NodeCpu from '../NodeInformation/NodeCpu';
import NodeIO from '../NodeInformation/NodeIO';
import NodeMemory from '../NodeInformation/NodeMemory';
import NodeNetwork from '../NodeInformation/NodeNetwork';
import Storage from '../NodeInformation/Storage';
import { getAgentListInterface } from '../../api/common';
import db from '../../utils/storage';

const { Option } = Select;
export default class Node extends Component {
  constructor() {
    super()
    this.state = {
      ifShow: false,
      selValue:'',
      selTimeValue:5,
      startTime: new Date().getTime() - 300000,
      endTime: new Date().getTime(),
      options:[],
      tabkey:"1",
      minoptions:[{name:'5min',value:5},{name:'10min',value:10},{name:'30min',value:30},{name:'1hour',value:60},{name:'3hours',value:180},
      {name:'6hours',value:360},{name:'12hours',value:720},{name:'1days',value:1440},{name:'3days',value:4320},{name:'7days',value:10080},{name:'15days',value:21600}],
    }
  }
  onChange = (key) => {
    this.setState(() => ({tabkey: key}))
    
  };
  changeSelVal (value) {
    this.setState({selValue: value})
  }
  changeTimeSelVal (value) {
    this.setState(() => ({
      startTime: this.state.endTime ? new Date(this.state.endTime).getTime() - value * 60000 : '',
      endTime: this.state.endTime ? new Date(this.state.endTime).getTime() : '',
      selTimeValue: value}))
  }
  async getItemList () {
    const { success, data, msg } = await getAgentListInterface()
    let optionArr = [],newOptionArr = []
    Object.keys(data).forEach((item) => {
      if(item === db.ss.get('Instance_value')){
        optionArr = data[item]
      }
    })
    optionArr.forEach((item) => {
      newOptionArr.push(item.split(':')[0])
    })
    if (success) {
      this.setState(() => ({
        options: newOptionArr,
        selValue:newOptionArr[0],
        ifShow: true
      }))
    } else {
      message.error(msg)
    }
  }
  setDates = (dates, dateStrings) => {
    this.setState(() => ({
      startTime: dateStrings ? new Date(dateStrings).getTime() - this.state.selTimeValue * 60000 : '',
      endTime: dateStrings ? new Date(dateStrings).getTime() : '',}))
  };
  componentDidMount () {
    this.getItemList()
  }
  render() {
    let items = [
      {
        key: '1',
        label: `CPU`,
        children: <NodeCpu ref={(e) => {this.NodeCpuChartRef = e}} tabkey={this.state.tabkey} selValue={this.state.selValue} selTimeValue={this.state.endTime ? '' : this.state.selTimeValue} startTime={this.state.startTime} endTime={this.state.endTime} />,
      },
      {
        key: '2',
        label: `I/O`,
        children: <NodeIO ref={(e) => {this.NodeIoChartRef = e}} tabkey={this.state.tabkey} selValue={this.state.selValue} selTimeValue={this.state.endTime ? '' : this.state.selTimeValue} startTime={this.state.startTime} endTime={this.state.endTime} />,
      },
      {
        key: '3',
        label: `Memory`,
        children: <NodeMemory ref={(e) => {this.NodeMemoryChartRef = e}} tabkey={this.state.tabkey} selValue={this.state.selValue} selTimeValue={this.state.endTime ? '' : this.state.selTimeValue} startTime={this.state.startTime} endTime={this.state.endTime} />,
      },
      {
        key: '4',
        label: `Network`,
        children: <NodeNetwork ref={(e) => {this.NodeNetworkChartRef = e}} tabkey={this.state.tabkey} selValue={this.state.selValue} selTimeValue={this.state.endTime ? '' : this.state.selTimeValue} startTime={this.state.startTime} endTime={this.state.endTime} />,
      },
      {
        key: '5',
        label: `Storage`,
        children: <Storage ref={(e) => {this.NodeStorageTableRef = e}} tabkey={this.state.tabkey} selValue={this.state.selValue} selTimeValue={this.state.endTime ? '' : this.state.selTimeValue} startTime={this.state.startTime} endTime={this.state.endTime} />,
      }
    ]
    return (
      <div className='nodeselect'>
        {this.state.ifShow ? 
        <Tabs tabBarGutter={30}  className='childstyle' type="card "  defaultActiveKey="1" items={items} onChange={this.onChange} destroyInactiveTabPane={true}
         tabBarExtraContent={
          <div>
          <Select value={this.state.selValue} onChange={(val) => { this.changeSelVal(val) }} showSearch
          optionFilterProp="children"  filterOption={(input, option) =>
            option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 124, marginRight: 10 }} className='mb-10' >
          {
            this.state.options.map(item => {
              return (
                <Option value={item} key={item}>{item}</Option>
              )
            })
          }
        </Select>
        <Select value={this.state.selTimeValue} onChange={(val) => { this.changeTimeSelVal(val) }}
           style={{ width: 100,marginRight: 10}} className='mb-10' >
              {
            this.state.minoptions.map((item,index) => {
              return (
                <Option value={item.value} key={index}>{item.name}</Option>
              )
            })
          }
        </Select>
        <DatePicker
          defaultValue={moment(new Date(), 'YYYY-MM-DD HH:mm:ss')}
          placeholder='endTime'
          format="YYYY-MM-DD HH:mm:ss"
          onChange={this.setDates}
          showTime
        />
          </div>
         } /> : ''}
      </div>
    )
  }
}
