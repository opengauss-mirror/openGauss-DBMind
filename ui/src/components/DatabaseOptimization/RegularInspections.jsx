import React, { Component } from 'react';
import RegularInspections from './RegularInspectionsModules/RegularInspections';
import MetricStatistics from './RegularInspectionsModules/MetricStatistics';
import { getMetricStatisticInterfaceCount } from '../../api/clusterInformation';
export default class RegularInspect extends Component {
  constructor(props) {
    super(props)
    this.state = {
      metricStatisticCount: {},
    }
  }
  async getMetricStatisticInterfaceCount () {
    const { success, data, msg } = await getMetricStatisticInterfaceCount()
    if (success) {
      let dataObj = this.state.metricStatisticCount;
      dataObj['total'] = data;
      this.setState(() => ({
        metricStatisticCount: dataObj
      }))
    } else {
      message.error(msg)
    }
  }
  componentDidMount () {
    this.getMetricStatisticInterfaceCount()
  }
  render () {
    return (
      <div>
        <RegularInspections />
        <MetricStatistics metricStatisticCount={this.state.metricStatisticCount} getMetricStatisticInterfaceCount={() => this.getMetricStatisticInterfaceCount()} />
      </div>
    )
  }
}
