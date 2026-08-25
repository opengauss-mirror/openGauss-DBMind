import React, { Component } from 'react';
import HistoryAlarms from './AlarmsModules/HistoryAlarms';
export default class Alarms extends Component {
  constructor() {
    super()
    this.state = {}
  }
  componentDidMount () { }
  render () {
    return (
      <div className="contentWrap">
        <HistoryAlarms />
      </div>
    )
  }
}
