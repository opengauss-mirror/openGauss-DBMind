import React, {Component} from 'react';
import KilledSlowQuery from './SlowTopQueryModules/KilledSlowQuery';
import RecentSlowQuery from './SlowTopQueryModules/RecentSlowQuery';
import TopQuery from './SlowTopQueryModules/TopQuery';

export default class SlowTopQuery extends Component {
  constructor() {
    super()
    this.state = {}
  }
  render () {
    return (
      <div>
        < KilledSlowQuery />
        < RecentSlowQuery />
        <TopQuery />
      </div>
    )
  }
}
