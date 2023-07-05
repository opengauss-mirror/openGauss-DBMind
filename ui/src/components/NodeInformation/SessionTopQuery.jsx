import React, { Component } from 'react';
import { Tabs, Select, message, Input } from 'antd';
import TopQuery from '../NodeInformation/TopQuery';
import Session from '../NodeInformation/Session';
import Refresh from '../../assets/imgs/Refresh.png';

export default class SessionTopQuery extends Component {
  constructor(props) {
    super(props)
    this.state = {
      ifShow: true,
      tabSessionkey:"1",
    }
  }
  onChange = (key) => {
    this.setState(() => ({tabSessionkey: key}))
  };
  handleRefresh(){
    if(this.state.tabSessionkey === "1"){
      this.SessionRef.getSessionData()
    } else {
      this.TopQueryRef.getTopQueryData()
    }
  }
  componentDidMount () {

  }
  render() {
    let items = [
      {
        key: '1',
        label: `Session`,
        children: <Session ref={(e) => {this.SessionRef = e}} tabkey={this.props.tabkey} tabSessionkey={this.state.tabSessionkey} />,
      },
      {
        key: '2',
        label: `Top Query`,
        children: <TopQuery ref={(e) => {this.TopQueryRef = e}} tabkey={this.props.tabkey} tabSessionkey={this.state.tabSessionkey} />,
      }
    ]
    return (
      <div className='thirdTabClass'>
        {this.state.ifShow ? 
        <Tabs tabBarGutter={30}  className='childstyle' type="card "  defaultActiveKey="1" items={items} onChange={this.onChange} abBarExtraContent={
          <img
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
