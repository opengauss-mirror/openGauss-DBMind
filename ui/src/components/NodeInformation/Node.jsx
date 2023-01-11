import React, { Component } from 'react';
import { Col, Row } from 'antd';
import OpengaussExporters from './NodeModules/OpengaussExporters';
import Alert from './NodeModules/Alert';
import Instances from './NodeModules/Instance';

export default class Instance extends Component {
  constructor() {
    super()
    this.state = {}
  }
  render () {
    return (
      <div>
        <Row gutter={[16, 16]}>
          <Col className="gutter-row" span={17}>
            <OpengaussExporters />
          </Col>
          <Col className="gutter-row" span={7}>
            <Alert />
          </Col>
          <Col className="gutter-row" span={24}>
            <Instances />
          </Col>
        </Row>
      </div>
    )
  }
}
