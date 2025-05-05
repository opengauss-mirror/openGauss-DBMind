import React, { Component } from 'react';
import { Col, Row, Card } from 'antd';
import PropTypes from 'prop-types';
import ActiveConnectionsChart from './RegularInspectionsModules/ActiveConnectionsChart';
import TotalConnectionsChart from './RegularInspectionsModules/TotalConnectionsChart';
import ResponseTimeLineChart from './RegularInspectionsModules/ResponseTimeLineChart';
import TpsLineChart from './RegularInspectionsModules/TpsLineChart';
import SystemResourceChart from './RegularInspectionsModules/SystemResourceChart';
import DatabaseSizeChart from './RegularInspectionsModules/DatabaseSizeChart';
import TableSizeChart from './RegularInspectionsModules/TableSizeChart';
import HistoryAlarmChart from './RegularInspectionsModules/HistoryAlarmChart';
import InstanceSlowSqlChart from './RegularInspectionsModules/InstanceSlowSqlChart';
import TopkSql from './RegularInspectionsModules/TopkSql';
import RcaSql from './RegularInspectionsModules/RcaSql';
import DynamicMemoryChart from './RegularInspectionsModules/DynamicMemoryChart';
import RiskAnalysis from './RegularInspectionsModules/RiskAnalysis';

export default class RegularInspectionsWeek extends Component {
  static propTypes={
    regularInspectionsWeek:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      systemResourceChart: '',
      regularInspectionsWeek: {},
      tpsLineChartData: '',
      responseTimeLineChart: '',
      activeLineChart: '',
      totalLineChart: '',
      databaseSizeChart: '',
      tableSizeChart: '',
      historyAlarmChart: '',
      instanceSlowSqlChart: '',
      topkSqlData: '',
      rcaSqlData: '',
      dynamicMemoryChart: '',
      riskAnalysis: '',
    }
  }
  getRegularInspections (data) {
    if (data) {
        this.setState({
          systemResourceChart: data.rows[0][1].resource,
          tpsLineChartData: data.rows[0][1].performance,
          responseTimeLineChart: data.rows[0][1].performance,
          activeLineChart: data.rows[0][1].connection,
          totalLineChart: data.rows[0][1].connection,
          databaseSizeChart: data.rows[0][1].db_size,
          tableSizeChart: data.rows[0][1].table_size,
          historyAlarmChart: data.rows[0][1].history_alarm,
          instanceSlowSqlChart: data.rows[0][1].slow_sql_rca,
          topkSqlData: data.rows[0][1].slow_sql_rca.query_templates,
          rcaSqlData: data.rows[0][1].slow_sql_rca,
          dynamicMemoryChart: data.rows[0][1].dynamic_memory,
          riskAnalysis: data.rows[0][1].risks,
        })
    } else {
    }
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    this.getRegularInspections(nextProps.regularInspectionsWeek)
  }
  componentWillUnmount = () => {
      this.setState = () => {return}
  }
  render () {
    return (
      <div style={{ textAlign: 'center' }}>
          <SystemResourceChart systemResourceChart={this.state.systemResourceChart} />
          <Card title="Instance Performance And Workload" className="mb-20" style={{ textAlign: 'center' }}>
              <Row gutter={16}>
              <Col className="gutter-row" span={12}>
                  <div className="cardShow">
                    <TpsLineChart tpsLineChartData={this.state.tpsLineChartData} />
                  </div>
                </Col>
                <Col className="gutter-row" span={12}>
                  <div className="cardShow">
                    <ResponseTimeLineChart responseTimeLineChart={this.state.responseTimeLineChart} />
                  </div>
                </Col>
              </Row>
          </Card>
          <Card title="Instance Connection" className="mb-20" style={{ textAlign: 'center' }}>
              <Row gutter={16}>
              <Col className="gutter-row" span={12}>
                  <div className="cardShow">
                    <TotalConnectionsChart totalLineChart={this.state.totalLineChart} />
                  </div>
                </Col>
                <Col className="gutter-row" span={12}>
                  <div className="cardShow">
                    <ActiveConnectionsChart activeLineChart={this.state.activeLineChart} />
                  </div>
                </Col>
              </Row>
          </Card>
          <DatabaseSizeChart databaseSizeChart={this.state.databaseSizeChart} />
          <TableSizeChart tableSizeChart={this.state.tableSizeChart} />
          <Card title="Instance Alarm" className="mb-20" style={{ textAlign: 'center' }}>
              <Row gutter={16}>
                <Col className="gutter-row" span={12}>
                  <div className="cardShow">
                    <HistoryAlarmChart historyAlarmChart={this.state.historyAlarmChart} />
                  </div>
                </Col>
              </Row>
          </Card>
          <Card title="Instance Slow Sql" className="mb-20" style={{ textAlign: 'center' }}>
            <InstanceSlowSqlChart instanceSlowSqlChart={this.state.instanceSlowSqlChart} />
              <Row gutter={16}>
              <Col className="gutter-row" span={12}>
                  <div className="cardShow">
                    <TopkSql topkSqlData={this.state.topkSqlData} />
                  </div>
                </Col>
                <Col className="gutter-row" span={12}>
                  <div className="cardShow">
                    <RcaSql rcaSqlData={this.state.rcaSqlData} />
                  </div>
                </Col>
              </Row>
          </Card>
          <DynamicMemoryChart dynamicMemoryChart={this.state.dynamicMemoryChart} />
          <RiskAnalysis riskAnalysis={this.state.riskAnalysis} />
      </div>
    )
  }
}
