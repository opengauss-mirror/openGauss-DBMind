import React, { Component } from 'react';
import { Tabs, Select, message, Input } from 'antd';
import CacheInformation from '../NodeInformation/CacheInformation';
import LockInformation from '../NodeInformation/LockInformation';
import Refresh from '../../assets/imgs/Refresh.png';

export default class DBLockingAndCaching extends Component {
  constructor(props) {
    super(props)
    this.state = {
      ifShow: true,
      tabChildkey:"1",
    }
  }
  onChange = (key) => {
    this.setState(() => ({tabChildkey: key}))
  };
  handleRefresh(){
    this.LockInformationRef.getLockingQueryData()
  }
  componentDidMount () {

  }
  render() {
    let items = [
      {
        key: '1',
        label: `Lock Information`,
        children: <LockInformation ref={(e) => {this.LockInformationRef = e}} tabkey={this.props.tabkey} tabChildkey={this.state.tabChildkey} />,
      },
      {
        key: '2',
        label: `Cache Information`,
        children: <CacheInformation ref={(e) => {this.CacheInformationRef = e}} tabkey={this.props.tabkey} tabChildkey={this.state.tabChildkey} selValue={this.props.selValue} selTimeValue={this.props.selTimeValue} />,
      }
    ]
    return (
      <div className='thirdTabClass'>
        {this.state.ifShow ? 
        <Tabs tabBarGutter={30}  className='childstyle' type="card "  defaultActiveKey="1" items={items} onChange={this.onChange} tabBarExtraContent={
          (this.state.tabChildkey === '1') &&<img
          src={Refresh}
          title='Refresh'
          alt=""
          onClick={() => this.handleRefresh()}
        ></img>
        } /> : ''}
      </div>
    )
  }
}
