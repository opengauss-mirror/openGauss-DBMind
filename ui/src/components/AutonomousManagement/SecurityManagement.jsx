import React, { Component } from 'react';
// 移除未使用的 Col/Row 导入
import IsDeveloping from '../../assets/imgs/IsDeveloping.png';
export default class SecurityManagement extends Component {
  constructor() {
    super()
    this.state = {}
  }
  componentDidMount () { }
  render () {
    return (
      <div style={{marginTop:50,textAlign:'-webkit-center'}}>
        <div style={{width:595,height:353,backgroundImage:'url('+IsDeveloping+')'}}><span style={{display:'block',padding:'40% 0'}}>Don't worry, the current page is under development.....</span></div>
      </div>
    )
  }
}
