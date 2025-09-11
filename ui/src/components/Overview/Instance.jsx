import React, { Component } from 'react';
import { Card, message, Row, Col } from 'antd';
import { getInterface } from '../../api/overview';

const demoImgArr = ['icon1','icon2','icon3','icon4','icon5','icon6','alarm1','alarm2','alarm3','alarm4']
const ticks = demoImgArr.map(item => require("../../assets/imgs/" + item + ".png"))
export default class Instance extends Component {
  constructor() {
    super()
    this.state = {
      alertData: [],
    }
  }
  async getInterface () {
    const { success, data, msg } = await getInterface()
    if (success) {
      this.setState(() => ({
        alertData: data
      }))
    } else {
      message.error(msg)
    }
  }
  componentDidMount () {
    this.getInterface()
  }
  render () {
    return (
      <Row gutter={10} className="bgcolor">
      <Col className="gutter-row antclopercent_20" >
        <Card title="Instance Status" className='instancefontsize' style={{ height: 100}} extra={<img src={ticks[0].default} alt="" ></img>}>
          <span className='textstyle'><img src={ticks[5].default} alt="" className='iconstyle'></img>{this.state.alertData.status}</span>
        </Card>
      </Col>
      <Col className="gutter-row antclopercent_20" >
        <Card title="Deployment Mode" className='instancefontsize' style={{ height: 100}} extra={<img src={ticks[1].default} alt="" ></img>}>
          <span className='textstyle'>{this.state.alertData.deployment_mode}</span>
        </Card>
      </Col>
      <Col className="gutter-row antclopercent_20" >
        <Card title="Strength Version" className='instancefontsize' style={{ height: 100}} extra={<img src={ticks[2].default} alt="" ></img>}>
        <span className='textstyle' title={this.state.alertData.strength_version}>{this.state.alertData.strength_version}</span>
        </Card>
      </Col>
      <Col className="gutter-row antclopercent_20" >
        <Card title="Operating System" className='instancefontsize' style={{ height: 100}}  extra={<img src={ticks[3].default} alt="" ></img>}>
        <span className='textstyle' >{this.state.alertData.operating_system}</span>
        </Card>
      </Col>
      <Col className="gutter-row antclopercent_20" >
        <Card title="Alarm List" className='instancefontsize' style={{ height: 100}}  extra={<img src={ticks[4].default} alt="" ></img>}>
            <Row gutter={2}>
                <Col className="gutter-row" span={6}>
                  <span className='textstyle'><img src={ticks[6].default} alt="" className='alarmstyle'></img><span className='numstyle'>{this.state.alertData.major_risk}</span></span>
                </Col>
                <Col className="gutter-row" span={6}>
                  <span className='textstyle'><img src={ticks[7].default} alt="" className='alarmstyle'></img><span className='numstyle'>{this.state.alertData.high_risk}</span></span>
                </Col>
                <Col className="gutter-row" span={6}>
                  <span className='textstyle'><img src={ticks[8].default} alt="" className='alarmstyle'></img><span className='numstyle'>{this.state.alertData.general_risk}</span></span>
                </Col>
                <Col className="gutter-row" span={6}>
                  <span className='textstyle'><img src={ticks[9].default} alt="" className='alarmstyle'></img><span className='numstyle'>{this.state.alertData.low_risk}</span></span>
                </Col>
            </Row>
        </Card>
      </Col>
    </Row>
    )
  }
}
