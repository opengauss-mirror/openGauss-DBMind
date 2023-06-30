import React, { Component } from "react";
import { Col, Row, Select, Card, Spin } from "antd";
import PropTypes from "prop-types";
import InstanceResource from "./RegularInspectionsModules/InstanceResource";


export default class RegularInspectionsDaySystem extends Component {
  static propTypes = {
    regularInspectionsDay: PropTypes.object.isRequired
  }
  constructor(props) {
    super(props);
    this.state = {
      instanceResource: "",
    };
  }
  getRegularInspections(data) {
    if (data) {
      this.setState({
        instanceResource: data.rows[0][1].resource,


      });
    } else {
    }
  }
  UNSAFE_componentWillReceiveProps(nextProps) {
    if (JSON.stringify(nextProps.regularInspectionsDay) !== "{}") {
      this.getRegularInspections(nextProps.regularInspectionsDay)
    }
  }

  render() {
    return (
      <div>
        <Row gutter={10} className="mb-10">
          <Col className="gutter-row" span={24}>
            <div className="cardShow">
              <InstanceResource
                instanceResource={this.state.instanceResource}
              />
            </div>
          </Col>

        </Row>

      </div>
    )
  }
}
