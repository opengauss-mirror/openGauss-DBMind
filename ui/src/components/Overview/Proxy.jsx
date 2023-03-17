import React, { Component } from 'react';
import { Card, Empty, Progress, message } from 'antd';
import icon7 from '../../assets/imgs/icon7.png';
import icon8 from '../../assets/imgs/icon8.png';
import { getProxy } from '../../api/overview';

export default class Proxy extends Component {
  constructor() {
    super()
    this.state = {
      ifShow: false,
      status:true,
      proxy_name:'',
      proxy_host:'',
    }
  }
  async getProxy () {
    const { success, data, msg } = await getProxy()
    if (success) {
      this.setState(() => ({
        ifShow: true,
        status:data.status,
        proxy_host:data.agent_address,
      }))
    } else {
      this.setState({ifShow: false})
      message.error(msg)
    }
  }
  componentDidMount () {
    this.getProxy()
  }
  render () {
    return (
      <div className='proxystyle'>
        {this.state.ifShow ? 
        <>
        <p style={{textAlign:'right',margin: '0'}}>
          <span style={{marginRight:20}}><img src={icon7} alt="" className='iconstyle'></img>normal</span>
          <span><img src={icon8} alt="" className='iconstyle'></img>false</span>
        </p>
        <Progress className={this.state.status ? 'procolorblue' : 'procolorred'} percent={this.state.status ? 100 : 0} size="small" type='line' trailColor='#EB7373' strokeColor='#82ACFD' showInfo={false} /> 
        <p style={{color:'#4e4e4e',fontSize:12}}>{this.state.proxy_host}</p>
        </>
        : <Empty description={this.state.ifShow} />}
      </div>
    )
  }
}
