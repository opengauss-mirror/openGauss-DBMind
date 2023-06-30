import React, { Component } from "react";
import SystemResourceChart from "./RegularInspectionsModules/SystemResourceChart";

export default class RegularInspectionsWeek extends Component {
  static propTypes = {
    regularInspectionsWeek: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      systemResourceChart: "",
    };
  }
  getRegularInspections(data) {
    if (data) {
      this.setState({
        systemResourceChart: data.rows[0][1].resource,
      });
    } else {
    }
  }
  UNSAFE_componentWillReceiveProps(nextProps) {
    if (JSON.stringify(nextProps.regularInspectionsWeek) !== "{}") {
      this.getRegularInspections(nextProps.regularInspectionsWeek);
    }
  }

  render() {
    return (
      <div>
        <SystemResourceChart
          systemResourceChart={this.state.systemResourceChart}
        />
      </div>
    );
  }
}
