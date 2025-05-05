import React, { Component } from 'react';
import { Tabs } from 'antd';
import { EditOutlined, PieChartOutlined, SlidersOutlined, ReloadOutlined} from '@ant-design/icons';
import '../assets/css/common.css';
import DatabaseTuning from '../components/DatabaseOptimization/DatabaseTuning';
import IndexTuning from '../components/DatabaseOptimization/IndexTuning';
import SlowQueryAnalysis from '../components/DatabaseOptimization/SlowQueryAnalysis';
import RegularInspections from '../components/DatabaseOptimization/RegularInspections';

const { TabPane } = Tabs;
export default class DatabaseOptimization extends Component {
  constructor() {
    super()
    this.state = {
      paneKey:'1',
    }
  }
  handleOnClick(){
    if(this.state.paneKey === '1'){
      this.IndexTuning.handleRefresh();
    } else if(this.state.paneKey === '3'){
      this.SlowQueryAnalysis.handleRefresh();
    } else if(this.state.paneKey === '4'){
      this.RegularInspections.handleRefresh();
    }
	}
  onChangePane = (key) => {
    this.setState({
      paneKey: key,
    });
  };
  createBarExtraContent = () => {
    return (
      (this.state.paneKey === '1' || this.state.paneKey === '3'|| this.state.paneKey === '4') &&
      (<div style={{ padding: '0px 24px' }}>
        {<ReloadOutlined className="more_link" onClick={() => { this.handleOnClick() }} />}
      </div>)
    );
  }
  render () {
    return (
      <div className="contentWrap" activeKey={this.state.numMo}>
        <Tabs tabBarGutter={0} type="card" size="small" 
        onChange={this.onChangePane} tabBarExtraContent={this.createBarExtraContent()}>
          <TabPane
            tab={
              <span>
                <EditOutlined />
                Index Tuning
              </span>
            }
            key="1"
          >
            <IndexTuning onRef={ node => this.IndexTuning = node } />
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
            <SlowQueryAnalysis  onRef={ node => this.SlowQueryAnalysis = node }/>
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
            <RegularInspections onRef={ node => this.RegularInspections = node }/>
          </TabPane>
        </Tabs>
      </div>
    )
  }
}
