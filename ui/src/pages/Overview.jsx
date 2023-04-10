import React, { Component } from 'react';
import { Row, Col, Card, Spin, Tooltip } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import Refresh from '../assets/imgs/Refresh.png';
import Help from '../assets/imgs/Help.png';
import '../assets/css/main/overview.css';
import Instance from '../components/Overview/Instance';
import ResponseTimeCharts from '../components/Overview/ResponseTimeCharts';
import ConnectionCharts from '../components/Overview/ConnectionCharts';
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
      showFlag: 0
    }
  }
  render () {
    return (
      <div className="contentWrap">
        <Card title="Instance Name" className='instancename mb-10' style={{ height: 168}} >
          <Instance />
        </Card>
        <Row gutter={10} >
            <Col className="gutter-row" span={18}>
              <Row gutter={10} className='mb-10'>
                <Col className="gutter-row" span={12}>
                <Card title="Response Time" className='instancename' style={{ height: 158}} >
                  <ResponseTimeCharts />
                </Card>
                </Col>
                <Col className="gutter-row" span={12}>
                  <Card title="Total Connection" className='instancename' style={{ height: 158}} >
                    <ConnectionCharts />
                  </Card>
                </Col>
              </Row>
              <Row gutter={10} className='mb-10'>
                <Col className="gutter-row" span={14}>
                  <DataDiskCharts  />
                </Col>
                <Col className="gutter-row" span={10}>
                  <Card title="SQL Distribution" className='instancename' style={{ height: 278}} >
                    <SqlDistributionChart />
                  </Card>
                </Col>
              </Row>
              <Row gutter={10} className='mb-10'>
                <Col className="gutter-row" span={12}>
                  <Card title="Transaction State" className='instancename' style={{ height: 278}} extra={<span className='more_link' onClick={() => this.TransactionStateChartRef.isMore('false')}>more</span>}>
                  <TransactionStateChart ref={(e) => {this.TransactionStateChartRef = e}}/>
                  </Card>
                </Col>
                <Col className="gutter-row" span={12}>
                  <Card title="Database Size(Unit:MB)" className='instancename' style={{ height: 278}} extra={<span className='more_link' onClick={() => this.DatabaseSizeChartRef.isMore('false')}>more</span>}>
                  <DatabaseSizeChart ref={(e) => {this.DatabaseSizeChartRef = e}} />
                  </Card>
                </Col>
              </Row>
            </Col>
            <Col className="gutter-row" span={6}>
              <Card title="Proxy" className='instancename' style={{ height: 158}} >
                <Proxy />
              </Card>
              <Card title="Node" className='instancename' style={{ height: 288}} >
              <NodeTable />
              </Card>
              <CollectionTable />
            </Col>
        </Row>
        <Card title="Scheduled Task" className='instancename' style={{ height: 520}} extra={<div><Tooltip placement="left" color={'#ffffff'} title={<span style={{ color: '#000' }}>The current status needs to be modified in the background. The front-end setting is temporarily unavailable.</span>}><img src={Help} alt="" className='iconstyle' ></img></Tooltip><img src={Refresh} alt="" className='iconstyle' onClick={() => this.ScheduledTaskTableRef.refresh()} ></img></div>}>
          {this.state.showFlag === 0 ?
          <ScheduledTaskTable ref={(e) => {this.ScheduledTaskTableRef = e}}/> 
          : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>}
        </Card>
      </div>
    )
  }
}

