import React, { Component } from 'react';
import { Col, Row, Card } from 'antd';

import NodeEchartFormWork from '../../../NodeInformation/NodeModules/NodeEchartFormWork';


export default class DBPerformance extends Component {
  static propTypes = {
    dbPerformance: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props)
    this.state = {
      chartData1: {},
      chartData2: {},
      chartData3: {},
      chartData4: {},
      isShow: false
    }
  }

  divisionItem(arr1, arr2) {
    let newArr = arr1.map(function (item, index) {
      return item / arr2[index];
    });
    return newArr;
  }
  async getPerformanceData(data) {

    let result = []

    result.push(data['gaussdb_total_connection'] ?? [], data['gaussdb_active_connection'] ?? [], data['gaussdb_idle_connection'] ?? [], data['pg_sql_count_ddl'] ?? [], data['pg_sql_count_dml'] ?? [], data['pg_sql_count_dcl'] ?? [], data['statement_responsetime_percentile_p80'] ?? [], data['statement_responsetime_percentile_p95'] ?? [])
    if (result[0]) {
      let newResult = [], activeRateData = [], waitingRateData = [], xDataArray = [[], [], [], [], [], [], [], [], [], []], yDataArray = [[], [], [], [], [], [], [], [], [], []]
      activeRateData = JSON.parse(JSON.stringify(result[0]))
      waitingRateData = JSON.parse(JSON.stringify(result[1]))
      activeRateData[0].values = this.divisionItem(result[0][0].values, result[2][0].values);
      waitingRateData[0].values = this.divisionItem(result[1][0].values, result[2][0].values);
      newResult = [result[0], result[1], result[2], activeRateData, waitingRateData, result[3], result[4], result[5], result[6], result[7]]
      newResult.forEach((item, index) => {
        xDataArray[index] = item[0].timestamps
      });
      newResult.forEach((item, index) => {
        item[0].values.forEach((oitem) => {
          yDataArray[index].push(oitem)
        });
      });
      let data1 = { 'legend': [{ image: '', description: 'Sessions' }, { image: '', description: 'Active Session' }, { image: '', description: 'Waiting Session' }], 'xAxisData': xDataArray[0], 'seriesData': [{ data: yDataArray[0], description: 'Sessions', colors: '#5990FD' }, { data: yDataArray[1], description: 'Active Session', colors: '#EEBA18' }, { data: yDataArray[2], description: 'Waiting Session', colors: '#9185F0' }], 'flg': 0, 'legendFlg': 2, title: "Sessions/Active Sessions/Waiting Sessions", 'unit': '', 'fixedflg': 0 }
      let data2 = { 'legend': [{ image: '', description: 'Active' }, { image: '', description: 'Waiting' }], 'xAxisData': xDataArray[3], 'seriesData': [{ data: yDataArray[3], description: 'Active', colors: '#EEBA18' }, { data: yDataArray[4], description: 'Waiting', colors: '#9185F0' }], 'flg': 1, 'legendFlg': 2, title: 'Active Session Rate/Waiting Session Rate', 'unit': '%', 'fixedflg': 1 }
      let data3 = { 'legend': [{ image: '', description: 'DDL' }, { image: '', description: 'DML' }, { image: '', description: 'DCL' }], 'xAxisData': xDataArray[5], 'seriesData': [{ data: yDataArray[5], description: 'DDL', colors: '#EC6F1A' }, { data: yDataArray[6], description: 'DML', colors: '#9185F0' }, { data: yDataArray[7], description: 'DCL', colors: '#2DA769' }], 'flg': 0, 'legendFlg': 2, title: 'DDL/DML/DCL', 'unit': '', 'fixedflg': 0 }
      let data4 = { 'legend': [{ image: '', description: '80% SQL Response Time' }, { image: '', description: '95% SQL Response Time' }], 'xAxisData': xDataArray[8], 'seriesData': [{ data: yDataArray[8], description: '80% SQL Response Time', colors: '#2070F3' }, { data: yDataArray[9], description: '95% SQL Response Time', colors: '#9185F0' }], 'flg': 0, 'legendFlg': 2, title: 'SQL Response Time (/ms)', 'unit': '', 'fixedflg': 0 }
      this.setState({
        chartData1: data1,
        chartData2: data2,
        chartData3: data3,
        chartData4: data4,

      })
    }

  }
  UNSAFE_componentWillReceiveProps(nextProps) {
    this.getPerformanceData(nextProps.dbPerformance);
  }

  render() {
    return (
      <Card title="DB Performance Indicators" className="mb-10">
        <Row gutter={[10, 10]} className="mb-10">
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork echartData={this.state.chartData1} />
          </Col>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork echartData={this.state.chartData2} />
          </Col>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork echartData={this.state.chartData3} />
          </Col>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork echartData={this.state.chartData4} />
          </Col>
        </Row>

      </Card>
    )
  }
}
