import React, { Component } from 'react';
import { Card, message, Tooltip, Spin, Empty, Row, Col } from 'antd';
import { getInterface } from '../../api/overview';
import icon1 from '../../assets/imgs/icon1.png';
import icon2 from '../../assets/imgs/icon2.png';
import icon3 from '../../assets/imgs/icon3.png';
import icon4 from '../../assets/imgs/icon4.png';
import icon5 from '../../assets/imgs/icon5.png';
import icon6 from '../../assets/imgs/icon6.png';
import alarm1 from '../../assets/imgs/alarm1.png';
import alarm2 from '../../assets/imgs/alarm2.png';
import alarm3 from '../../assets/imgs/alarm3.png';
import alarm4 from '../../assets/imgs/alarm4.png';

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
      <Col className="gutter-row ant-col-5" >
        <Card title="Instance Status" className='instancefontsize' style={{ height: 100}} extra={<img src={icon1} alt="" ></img>}>
          <span className='textstyle'><img src={icon6} alt="" className='iconstyle'></img>{this.state.alertData.status}</span>
        </Card>
      </Col>
      <Col className="gutter-row ant-col-5" >
        <Card title="Deployment Mode" className='instancefontsize' style={{ height: 100}} extra={<img src={icon2} alt="" ></img>}>
          <span className='textstyle'>{this.state.alertData.deployment_mode}</span>
        </Card>
      </Col>
      <Col className="gutter-row ant-col-5" >
        <Card title="Strength Version" className='instancefontsize' style={{ height: 100}} extra={<img src={icon3} alt="" ></img>}>
        <span className='textstyle' title={this.state.alertData.strength_version}>{this.state.alertData.strength_version}</span>
        </Card>
      </Col>
      <Col className="gutter-row ant-col-5" >
        <Card title="Operating System" className='instancefontsize' style={{ height: 100}}  extra={<img src={icon4} alt="" ></img>}>
        <span className='textstyle' >{this.state.alertData.operating_system}</span>
        </Card>
      </Col>
      <Col className="gutter-row ant-col-5" >
        <Card title="Aralm List" className='instancefontsize' style={{ height: 100}}  extra={<img src={icon5} alt="" ></img>}>
            <Row gutter={2}>
                <Col className="gutter-row" span={6}>
                  <span className='textstyle'><img src={alarm1} alt="" className='alarmstyle'></img><span className='numstyle'>{this.state.alertData.major_risk}</span></span>
                </Col>
                <Col className="gutter-row" span={6}>
                  <span className='textstyle'><img src={alarm2} alt="" className='alarmstyle'></img><span className='numstyle'>{this.state.alertData.high_risk}</span></span>
                </Col>
                <Col className="gutter-row" span={6}>
                  <span className='textstyle'><img src={alarm3} alt="" className='alarmstyle'></img><span className='numstyle'>{this.state.alertData.general_risk}</span></span>
                </Col>
                <Col className="gutter-row" span={6}>
                  <span className='textstyle'><img src={alarm4} alt="" className='alarmstyle'></img><span className='numstyle'>{this.state.alertData.low_risk}</span></span>
                </Col>
            </Row>
        </Card>
      </Col>
    </Row>
    )
  }
}
