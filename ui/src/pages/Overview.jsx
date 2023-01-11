import React, { Component } from 'react';
import { Row, Col, Card, Spin } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import TransactionStateChart from '../components/Overview/TransactionStateChart';
import ExporterInformationChart from '../components/Overview/ExporterInformationChart';
import RunningStatusChart from '../components/Overview/RunningStatusChart';
import Alert from '../components/Overview/Alert';
import ThroughputChart from '../components/Overview/ThroughputChart';
import ResponseTimeChart from '../components/Overview/ResponseTimeChart';
import SystemMemChart from '../components/Overview/SystemMemChart';
import SystemDiskChart from '../components/Overview/SystemDiskChart';
import SystemCpuChart from '../components/Overview/SystemCpuChart';

const endTime = (new Date()).getTime();
const startTime = endTime - 60 * 60 * 1000//前一个小时
export default class Overview1 extends Component {
  constructor(props) {
    super(props)
    this.state = {
      showFlag: 0
    }
  }
  refresh () {
    this.setState({
      showFlag: 2
    })
    const p1 = this.SystemMemChartRef.getQps()
    const p2 = this.SystemDiskChartRef.getQps()
    const p3 = this.SystemCpuChartRef.getQps()
    const p = Promise.all([p1, p2, p3])
    p.then(() => {
      this.setState({
        showFlag: 0
      })
    })
  }
  render () {
    return (
      <div className="contentWrap">
        <Row gutter={16} className="mb-20">
          <Col className="gutter-row" span={6}>
            <TransactionStateChart />
          </Col>
          <Col className="gutter-row" span={6}>
            <ExporterInformationChart />
          </Col>
          <Col className="gutter-row" span={6}>
            <RunningStatusChart />
          </Col>
          <Col className="gutter-row" span={6}>
            <Alert />
          </Col>
        </Row>
        <ThroughputChart startTime={startTime} endTime={endTime} />
        <ResponseTimeChart startTime={startTime} endTime={endTime} />
        <Card title="System Information" style={{ height: 340}} extra={<ReloadOutlined className="more_link" onClick={() => { this.refresh() }} />} >
          {this.state.showFlag === 0 ? <Row gutter={16}>
            <Col className="gutter-row" span={8}>
              <SystemMemChart startTime={startTime} endTime={endTime} ref={(e) => {
                this.SystemMemChartRef = e
              }} />
            </Col>
            <Col className="gutter-row" span={8}>
              <SystemDiskChart startTime={startTime} endTime={endTime} ref={(e) => {
                this.SystemDiskChartRef = e
              }} />
            </Col>
            <Col className="gutter-row" span={8}>
              <SystemCpuChart startTime={startTime} endTime={endTime} ref={(e) => {
                this.SystemCpuChartRef = e
              }} />
            </Col>
          </Row> : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>}
        </Card>
      </div>
    )
  }
}

