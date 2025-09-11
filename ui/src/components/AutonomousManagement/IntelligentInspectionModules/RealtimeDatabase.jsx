import React, { Component } from "react";
import DBCapability from "./RealtimeInspectionModule/DBCapability";
import DBPerformance from "./RealtimeInspectionModule/DBPerformance";
import DBCacheInformation from "./RealtimeInspectionModule/DBCacheInformation";
import DBUsage from "./RealtimeInspectionModule/DBUsage";
import DBCapacityMetric from "./RealtimeInspectionModule/DBCapacityMetric";
import DBMemory from "./RealtimeInspectionModule/DBMemory";
import PropTypes from "prop-types";
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
    // 绑定方法以避免this指向问题
    this.getRealtimeInspectionsDatabase = this.getRealtimeInspectionsDatabase.bind(this);
  }
  getRealtimeInspectionsDatabase(data) {
    // 添加数据验证，防止undefined错误
    if (!data || typeof data !== 'object') {
      console.warn('RealtimeDatabase: 无效的数据格式', data);
      return;
    }

    this.setState({
      dbCapability: data.service || {},
      dbPerformance: data.perform || {},
      dbCacheInformation: data.cache || {},
      dbUsage: data.resource || {},
      dbCapacityMetric: data.capacity || {},
      dbMemory: data.memory || {},
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
