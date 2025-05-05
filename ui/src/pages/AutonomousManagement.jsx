import React, { Component } from 'react'
import { Tabs } from 'antd';
import {
    BarChartOutlined,
    PieChartOutlined,
    SnippetsOutlined,
    UnorderedListOutlined
} from '@ant-design/icons';
import '../assets/css/common.css';
import Alarms from '../components/AutonomousManagement/Alarms';
import SlowTopQuery from '../components/AutonomousManagement/SlowTopQuery';
import ActiveSql from '../components/AutonomousManagement/ActiveSql';
import SummaryLog from '../components/AutonomousManagement/SummaryLog';

const { TabPane } = Tabs;

export default class AutonomousManagement extends Component {
  constructor() {
    super()
    this.state = {}
  }
  render () {
    return (
      <div className="contentWrap">
        <Tabs tabBarGutter={0} type="card" size={'small'}>
          <TabPane
            tab={
              <span>
                <UnorderedListOutlined />
                Alarms
              </span>
            }
            key="1"
          >
            <Alarms />
          </TabPane>
          <TabPane
            tab={
              <span>
                <BarChartOutlined />
                Slow/Top Query
              </span>
            }
            key="2"
          >
            <SlowTopQuery />
          </TabPane>
          <TabPane
            tab={
              <span>
                <PieChartOutlined />
                Active SQL Statements
              </span>
            }
            key="3"
          >
            <ActiveSql />
          </TabPane>
          <TabPane
            tab={
              <span>
                <SnippetsOutlined />
                Summary Log
              </span>
            }
            key="4"
          >
            <SummaryLog />
          </TabPane>
        </Tabs>
      </div>
    )
  }
}