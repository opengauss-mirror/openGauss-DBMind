import React, { Component } from 'react';
import RegularInspections from './RegularInspectionsModules/RegularInspections';
import MetricStatistics from './RegularInspectionsModules/MetricStatistics';
export default class RegularInspect extends Component {
  render () {
    return (
      <div>
        <RegularInspections />
        <MetricStatistics />
      </div>
    )
  }
}
