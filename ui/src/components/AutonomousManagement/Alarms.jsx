import React, { Component } from 'react';
import SelfhealingRecordsTable from './AlarmsModules/SelfhealingRecordsTable';
import HistoryAlarms from './AlarmsModules/HistoryAlarms';
import FutureAlarms from './AlarmsModules/FutureAlarms';

export default class Alarms extends Component {
  constructor() {
    super()
    this.state = {}
  }
  componentDidMount () { }
  render () {
    return (
      <div>
        <SelfhealingRecordsTable />
        <HistoryAlarms />
        <FutureAlarms />
      </div>
    )
  }
}
