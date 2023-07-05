import React, { Component } from 'react';
import { Row, Col, Card, Spin, Tooltip } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import Refresh from '../assets/imgs/Refresh.png';
import Help from '../assets/imgs/Help.png';
import '../assets/css/main/overview.css';
import Instance from '../components/Overview/Instance';
import ResponseTimeCharts from '../components/Overview/ResponseTimeCharts';
import ConnectionCharts from '../components/Overview/ConnectionCharts';
import TpsCharts from '../components/Overview/TpsCharts';
import DataDiskCharts from '../components/Overview/DataDiskCharts';
import SqlDistributionChart from '../components/Overview/SqlDistributionChart';
import TransactionStateChart from '../components/Overview/TransactionStateChart';
import DatabaseSizeChart from '../components/Overview/DatabaseSizeChart';
import Proxy from '../components/Overview/Proxy';
import NodeTable from '../components/Overview/NodeTable';
import CollectionTable from '../components/Overview/CollectionTable';
import ScheduledTaskTable from '../components/Overview/ScheduledTaskTable';


export default class Overview extends Component {
  constructor(props) {
    super(props)
    this.state = {
      showFlag: 0,
      reFreshKey:new Date()
    }
  }
  handleRefresh() {
    this.setState({ reFreshKey: new Date() });
  };
  render () {
    return (
      <div className="contentWrap">
        <div className="buttonstyle mb-10" style={{ textAlign: "right" }}>
          <img
            src={Refresh}
            title='Refresh'
            alt=""
            onClick={() => this.handleRefresh()}
          ></img>
        </div>
        <Card title="Instance Name" className='instancename mb-10' style={{ height: 168}} >
          <Instance key={this.state.reFreshKey} />
        </Card>
        <Row gutter={10} >
            <Col className="gutter-row" span={18}>
              <Row gutter={10} className='mb-10'>
                <Col className="gutter-row" span={10}>
                <Card title="Response Time" className='instancename' style={{ height: 158}} >
                  <ResponseTimeCharts key={this.state.reFreshKey} />
                </Card>
                </Col>
                <Col className="gutter-row" span={10}>
                  <Card title="Total Connection" className='instancename' style={{ height: 158}} >
                    <ConnectionCharts key={this.state.reFreshKey} />
                  </Card>
                </Col>
                <Col className="gutter-row" span={4}>
                  <Card title="TPS" className='instancename' style={{ height: 158}} >
                    <TpsCharts key={this.state.reFreshKey} />
                  </Card>
                </Col>
              </Row>
              <Row gutter={10} className='mb-10'>
                <Col className="gutter-row" span={14}>
                  <DataDiskCharts key={this.state.reFreshKey} />
                </Col>
                <Col className="gutter-row" span={10}>
                  <Card title="SQL Distribution" className='instancename' style={{ height: 278}} >
                    <SqlDistributionChart key={this.state.reFreshKey} />
                  </Card>
                </Col>
              </Row>
              <Row gutter={10} className='mb-10'>
                <Col className="gutter-row" span={12}>
                  <Card title="Transaction State" className='instancename' style={{ height: 278}} extra={<span className='more_link' onClick={() => this.TransactionStateChartRef.isMore('false')}>more</span>}>
                  <TransactionStateChart key={this.state.reFreshKey} ref={(e) => {this.TransactionStateChartRef = e}}/>
                  </Card>
                </Col>
                <Col className="gutter-row" span={12}>
                  <Card title="Database Size(Unit:GB)" className='instancename' style={{ height: 278}} extra={<span className='more_link' onClick={() => this.DatabaseSizeChartRef.isMore('false')}>more</span>}>
                  <DatabaseSizeChart key={this.state.reFreshKey} ref={(e) => {this.DatabaseSizeChartRef = e}} />
                  </Card>
                </Col>
              </Row>
            </Col>
            <Col className="gutter-row" span={6}>
              <Card title="Proxy" className='instancename' style={{ height: 158}} >
                <Proxy key={this.state.reFreshKey} />
              </Card>
              <Card title="Node" className='instancename' style={{ height: 288}} >
              <NodeTable key={this.state.reFreshKey} />
              </Card>
              <CollectionTable key={this.state.reFreshKey} />
            </Col>
        </Row>
        <Card title="Scheduled Task" className='instancename' style={{ height: 520}} extra={<div><Tooltip placement="left" color={'#ffffff'} title={<span style={{ color: '#000' }}>The current status needs to be modified in the background. The front-end setting is temporarily unavailable.</span>}><img src={Help} alt="" className='iconstyle' ></img></Tooltip><img src={Refresh} title='Refresh' alt="" className='iconstyle' onClick={() => this.ScheduledTaskTableRef.refresh()} ></img></div>}>
          {this.state.showFlag === 0 ?
          <ScheduledTaskTable key={this.state.reFreshKey} ref={(e) => {this.ScheduledTaskTableRef = e}}/> 
          : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>}
        </Card>
      </div>
    )
  }
}

