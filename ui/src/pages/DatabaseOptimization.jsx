import React, { Component } from 'react';
import { Tabs } from 'antd';
import { EditOutlined, PieChartOutlined, SlidersOutlined } from '@ant-design/icons';
import '../assets/css/common.css';
import DatabaseTuning from '../components/DatabaseOptimization/DatabaseTuning';
import IndexTuning from '../components/DatabaseOptimization/IndexTuning';
import SlowQueryAnalysis from '../components/DatabaseOptimization/SlowQueryAnalysis';
import RegularInspections from '../components/DatabaseOptimization/RegularInspections';

const { TabPane } = Tabs;
export default class DatabaseOptimization extends Component {
  constructor() {
    super()
    this.state = {}
  }
  render () {
    return (
      <div className="contentWrap">
        <Tabs tabBarGutter={0} type="card" size="small">
          <TabPane
            tab={
              <span>
                <EditOutlined />
                Index Tuning
              </span>
            }
            key="1"
          >
            <IndexTuning />
          </TabPane>
          <TabPane
            tab={
              <span>
                <SlidersOutlined />
                Database Tuning
              </span>
            }
            key="2"
          >
            <DatabaseTuning />
          </TabPane>
          <TabPane
            tab={
              <span>
                <PieChartOutlined />
                Slow Query Analysis
              </span>
            }
            key="3"
          >
            <SlowQueryAnalysis />
          </TabPane>
          <TabPane
            tab={
              <span>
                <PieChartOutlined />
                Regular Inspections
              </span>
            }
            key="4"
          >
            <RegularInspections />
          </TabPane>
        </Tabs>
      </div>
    )
  }
}
