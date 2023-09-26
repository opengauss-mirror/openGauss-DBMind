import React, { Component } from "react";
import SystemCpu from "./RealtimeInspectionModule/SystemCpu";
import SystemMemory from "./RealtimeInspectionModule/SystemMemory";
import SystemIO from "./RealtimeInspectionModule/SystemIO"
import SystemNetwork from "./RealtimeInspectionModule/SystemNetwork"
import PropTypes from "prop-types";
export default class RealtimeSystem extends Component {
  static propTypes = {
    realtimeInspections: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      systemCpu: {},
      systemMemory: {},
      systemIO: {},
      systemNetwork: {}
    };
  }

  getRealtimeInspections(data) {

    this.setState({
      systemCpu: data.cpu,
      systemMemory: data.memory,
      systemIO: data.io,
      systemNetwork: data.network,
    });
  }
  componentDidMount() {
    if (JSON.stringify(this.props.realtimeInspections) !== "{}") {
      this.getRealtimeInspections(this.props.realtimeInspections)
    }
  }
  render() {
    return (
      <div className="RealtimeSystem">
        <SystemCpu systemCpu={this.state.systemCpu} />
        <SystemMemory systemMemory={this.state.systemMemory} />
        <SystemIO systemIO={this.state.systemIO} />
        <SystemNetwork systemNetwork={this.state.systemNetwork} />
      </div>
    );
  }
}
