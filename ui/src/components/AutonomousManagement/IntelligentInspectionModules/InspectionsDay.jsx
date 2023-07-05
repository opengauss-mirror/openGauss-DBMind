import React, { Component } from "react";
import { Col, Row, Select, Card, Spin } from "antd";
import PropTypes from "prop-types";
import ActiveConnections from "./RegularInspectionsModules/ActiveConnections";
import TotalConnections from "./RegularInspectionsModules/TotalConnections";
import Tps from "./RegularInspectionsModules/Tps";
import ResponseTime from "./RegularInspectionsModules/ResponseTime";
import Dml from "./RegularInspectionsModules/Dml";
import DatabaseSize from "./RegularInspectionsModules/DatabaseSize";
import TableSize from "./RegularInspectionsModules/TableSize";
import HistoryAlarm from "./RegularInspectionsModules/HistoryAlarm";
import DistributionSlowSql from "./RegularInspectionsModules/DistributionSlowSql";
import DistributionRootCause from "./RegularInspectionsModules/DistributionRootCause";
import DynamicMemory from "./RegularInspectionsModules/DynamicMemory";

export default class RegularInspectionsDay extends Component {
  static propTypes = {
    regularInspectionsDay: PropTypes.object.isRequired
  }
  constructor(props) {
    super(props);
    this.state = {

      activeConnections: "",
      totalConnections: "",
      tpsData: "",
      responseTime: "",
      dmlData: "",
      databaseSize: "",
      tableSize: "",
      historyAlarm: "",
      distributionSlowSql: "",
      distributionRootCause: "",
      dynamicMemory: "",

    };
  }
  getRegularInspections(data) {
    if (data) {
      this.setState({

        activeConnections: data.rows[0][1].connection.active_connection,
        totalConnections: data.rows[0][1].connection.total_connection,
        tpsData: data.rows[0][1].performance.tps,
        responseTime: data.rows[0][1].performance.p95,
        dmlData: data.rows[0][1].dml,
        databaseSize: data.rows[0][1].db_size,
        tableSize: data.rows[0][1].table_size,
        historyAlarm: data.rows[0][1].history_alarm,
        distributionSlowSql:
          data.rows[0][1].slow_sql_rca.query_type_distribution,
        distributionRootCause:
          data.rows[0][1].slow_sql_rca.root_cause_distribution,
        dynamicMemory: data.rows[0][1].dynamic_memory,

      });
    } else {
    }
  }

  componentDidMount() {
    if (JSON.stringify(this.props.regularInspectionsDay) !== "{}") {
      this.getRegularInspections(this.props.regularInspectionsDay)
    }
  }
  render() {
    return (
      <div>
        <Row gutter={10} className="mb-10">
          <Col className="gutter-row" span={12}>
            <div className="cardShow">
              <TotalConnections
                totalConnections={this.state.totalConnections}
              />
            </div>
          </Col>
          <Col className="gutter-row" span={12}>
            <div className="cardShow">
              <ActiveConnections
                activeConnections={this.state.activeConnections}
              />
            </div>


          </Col>
        </Row>
        <Card title="Instance Performance And Workload" className="mb-10">
          <Row gutter={10}>
            <Col className="gutter-row" span={12}>
              <div className="cardShow">
                <Tps tpsData={this.state.tpsData} />
                <ResponseTime responseTime={this.state.responseTime} />
              </div>
            </Col>

            <Col className="gutter-row" span={12}>
              <div className="cardShow">
                <Dml dmlData={this.state.dmlData} />
              </div>
            </Col>
          </Row>
        </Card>
        <Card title="Instance Size" className="mb-10">
          <Row gutter={10}>
            <Col className="gutter-row" span={12}>
              <div className="cardShow">
                <DatabaseSize databaseSize={this.state.databaseSize} />
              </div>
            </Col>
            <Col className="gutter-row" span={12}>
              <div className="cardShow">
                <TableSize tableSize={this.state.tableSize} />
              </div>
            </Col>
          </Row>
        </Card>
        <Card title="Alarm Situation" className="mb-10">
          <HistoryAlarm historyAlarm={this.state.historyAlarm} />
        </Card>
        <Card title="SLOW SQL" className="mb-10">
          <Row gutter={10}>
            <Col className="gutter-row" span={12}>
              <div className="cardShow">
                <DistributionSlowSql
                  distributionSlowSql={this.state.distributionSlowSql}
                />
              </div>
            </Col>
            <Col className="gutter-row" span={12}>
              <div className="cardShow">
                <DistributionRootCause
                  distributionRootCause={this.state.distributionRootCause}
                />
              </div>
            </Col>
          </Row>
        </Card>
        <DynamicMemory dynamicMemory={this.state.dynamicMemory} />
      </div>
    )
  }
}
