import React, { Component } from "react";
import DBCapability from "./RealtimeInspectionModule/DBCapability";
import DBPerformance from "./RealtimeInspectionModule/DBPerformance";
import DBCacheInformation from "./RealtimeInspectionModule/DBCacheInformation";
import DBUsage from "./RealtimeInspectionModule/DBUsage";
import DBCapacityMetric from "./RealtimeInspectionModule/DBCapacityMetric";
import DBMemory from "./RealtimeInspectionModule/DBMemory";
import "../../../assets/css/main/IntelligentInspection.css";

export default class RealtimeDatabase extends Component {
  static propTypes = {
    DBrealtimeInspections: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      dbCapability: {},
      dbPerformance: {},
      dbCacheInformation: {},
      dbUsage: {},
      dbCapacityMetric: {},
      dbMemory: {},
      isShow: false,
    };
  }
  getRealtimeInspectionsDatabase(data) {
    this.setState({
      dbCapability: data.service,
      dbPerformance: data.perform,
      dbCacheInformation: data.cache,
      dbUsage: data.resource,
      dbCapacityMetric: data.capacity,
      dbMemory: data.memory,

    });
  }

  componentDidMount() {
    if (JSON.stringify(this.props.DBrealtimeInspections) !== "{}") {
      this.getRealtimeInspectionsDatabase(this.props.DBrealtimeInspections)
    }
  }
  componentWillUnmount = () => {
    this.setState = () => { return }
  }
  render() {
    return (
      <div className="RealtimeDatabase">
        <DBCapability dbCapability={this.state.dbCapability} />
        <DBPerformance dbPerformance={this.state.dbPerformance} />
        <DBCacheInformation
          dbCacheInformation={this.state.dbCacheInformation}
        />
        <DBUsage dbUsage={this.state.dbUsage} />
        <DBCapacityMetric dbCapacityMetric={this.state.dbCapacityMetric} />
        <DBMemory dbMemory={this.state.dbMemory} />

      </div>
    );
  }
}
