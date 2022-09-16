import React, { Component } from 'react';
import { Card, message, Tooltip, Spin, Empty } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { getAlertInterface } from '../../api/overview';
import { formatTimestamp } from '../../utils/function';
let timer = null
export default class Alert extends Component {
  constructor() {
    super()
    this.state = {
      alertData: [],
      showFlag: 0
    }
  }
  // 自动播放
  scollAlert () {
    let alertrolling1 = document.getElementById('alertrolling1')
    let RollingStep = 1
    timer = setInterval(() => {
      let ThatRollingStep = RollingStep--
      alertrolling1.style.top = ThatRollingStep + 'px'
      let alertRefHeight = this.alertRef.clientHeight
      let alertRefTop = this.alertRef.offsetTop
      if (alertRefHeight - 250 < Math.abs(alertRefTop)) {
        alertrolling1.style.top = 0
        RollingStep = 10
      }
    }, 100)
  }
  async getAlert () {
    const { success, data, msg } = await getAlertInterface()
    if (success) {
      this.setState(() => ({
        showFlag: 0,
        alertData: data
      }))
    } else {
      this.setState({showFlag: 1})
      message.error(msg)
    }
  }
  handleRefresh () {
    this.setState({showFlag: 2}, () => {
      this.getAlert()
    })
  }
  componentWillUnmount () {
    clearInterval(timer)
  }
  componentDidMount () {
    this.scollAlert()
    this.getAlert()
  }
  render () {
    return (
      <div>
        <Card title="Alert" style={{ height: 350 }} extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />}>
          <div id="alertrolling">
            <ul id="alertrolling1" ref={(v)=>{this.alertRef=v}}>
              {
                this.state.showFlag === 0 ? <>
                  {this.state.alertData.map((item, index) => {
                    return (
                      <Tooltip title={item.catalog + ' ' + formatTimestamp(item.time)} key={index}>
                        <li key={index}>{item.msg}</li>
                      </Tooltip>
                    )
                  })
                  }
                </> : this.state.showFlag === 1 ? <Empty description={false} style={{ paddingTop: 50 }} /> : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>
              }
            </ul>
          </div>
        </Card>
      </div>
    )
  }
}
