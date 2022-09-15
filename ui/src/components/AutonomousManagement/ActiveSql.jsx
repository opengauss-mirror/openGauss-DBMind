import React, {Component} from 'react';
import ActiveSQLStatements from './ActiveSQLStatementsModules/ActiveSQLStatements';
import LockingQuery from './ActiveSQLStatementsModules/LockingQuery';

export default class ActiveSql extends Component {
  constructor() {
    super()
    this.state = {}
  }
  render () {
    return (
      <div>
        <ActiveSQLStatements />
        <LockingQuery />
      </div>
    )
  }
}
