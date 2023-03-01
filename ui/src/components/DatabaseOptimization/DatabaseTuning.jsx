import React, { Component } from 'react';
import { Card, message, Table, Spin } from 'antd';
import KnobData from './DatabaseTuningModules/Knob';
import MetricData from './DatabaseTuningModules/Metric';
import WarningData from './DatabaseTuningModules/Warning';
import { getDatabaseTuningInterface, getKnobRecommendationSnapshotCount, getKnobRecommendationWarningsCount, getKnobRecommendationCount } from '../../api/databaseOptimization';

export default class DatabaseTuning extends Component {
  constructor(props) {
    super(props)
    this.state = {
      loading: false,
      showflag: true,
      knobData: {},
      warningData: {},
      metricData: {}
    }
  }
  async getDatabaseTuning (param) {
    this.setState({loading: true })
    const { success, data, msg } = await getDatabaseTuningInterface(param)
    if (success) {
      this.getKnobRecommendationSnapshotCount();
      this.getKnobRecommendationWarningsCount();
      this.getKnobRecommendationCount();
      this.setState({
        loading: false,
        showflag: false,
        metricData: data.metric_snapshot,
        warningData: data.warnings,
        knobData: data.details
      })
    } else {
      message.error(msg)
    }
  }
  async getKnobRecommendationSnapshotCount () {
    const { success, data, msg } = await getKnobRecommendationSnapshotCount()
    if (success) {
      let dataObj = this.state.metricData;
      dataObj['total'] = data;
      this.setState(() => ({
        metricData: dataObj
      }))
    } else {
      message.error(msg)
    }
  }
  async getKnobRecommendationWarningsCount () {
    const { success, data, msg } = await getKnobRecommendationWarningsCount()
    if (success) {
      let dataObj = this.state.warningData;
      dataObj['total'] = data;
      this.setState(() => ({
        warningData: dataObj
      }))
    } else {
      message.error(msg)
    }
  }
  async getKnobRecommendationCount () {
    const { success, data, msg } = await getKnobRecommendationCount()
    if (success) {
      let dataObj = this.state.knobData;
      dataObj['total'] = data;
      this.setState(() => ({
        knobData: dataObj
      }))
    } else {
      message.error(msg)
    }
  }
  componentDidMount () {
    this.getDatabaseTuning({
      metriccurrent: 1,
      metricpagesize: 10,
      warningcurrent: 1,
      warningpagesize: 10,
      knobcurrent: 1,
      knobpagesize: 10
    })
  }
  componentWillUnmount = () => {
      this.setState = () => {return}
  }
  render () {
    return (
      <div>
        {this.state.showflag ? <Spin style={{ margin: '260px 0 ' }} /> :
          <>
        <MetricData metricData={this.state.metricData} />
        <WarningData warningData={this.state.warningData} />
        <KnobData knobData={this.state.knobData} />
          </>
        }
      </div>
    )
  }
}
