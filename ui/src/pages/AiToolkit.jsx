import React, { Component } from 'react';
import IndexAdvisor from '../components/AiTookit/IndexAdvisor.jsx';
import QueryTuning from '../components/AiTookit/QueryTuning.jsx';
import Inteligent from '../components/AiTookit/InteligentSqlAnalysis';
import RiskAnalysis from '../components/AiTookit/RiskAnalysis';
import { AreaChartOutlined, BarChartOutlined, BarsOutlined, PieChartOutlined } from '@ant-design/icons';
import '../assets/css/common.css'
import '../assets/css/main/aiToolkit.css';
import { Tabs } from 'antd';

const { TabPane } = Tabs;

export default class AlToolkit extends Component {
  constructor() {
    super()
    this.state = {}
  }
  callback = () => { }
  render () {
    return (
      <div className="contentWrap">
        <Tabs onChange={this.callback} type="card" tabBarGutter={0}>
          <TabPane
            tab={
              <span>
                <AreaChartOutlined />
                Index Advisor
              </span>
            }
            key="1">
            <IndexAdvisor />
          </TabPane>
          <TabPane
            tab={
              <span>
                <BarsOutlined />
                Query Tuning
              </span>
            }
            key="2">
            <QueryTuning />
          </TabPane>
          <TabPane
            tab={
              <span>
                <BarChartOutlined />
                Inteligent SQL Analysis
              </span>
            }
            key="3">
            < Inteligent />
          </TabPane>
          <TabPane
            tab={
              <span>
                <PieChartOutlined />
                Risk Analysis
              </span>
            }
            key="4">
            <RiskAnalysis />
          </TabPane>
        </Tabs>
      </div>
    )
  }
}
