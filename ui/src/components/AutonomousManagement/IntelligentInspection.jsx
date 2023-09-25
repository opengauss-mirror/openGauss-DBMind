import React, { Component } from "react";
import InspectionTask from "./IntelligentInspectionModules/InspectionTask";
import InspectionRecords from "./IntelligentInspectionModules/InspectionRecords";
import "../../assets/css/main/IntelligentInspection.css";
import IntelligentInspectionDetail from "./IntelligentInspectionModules/IntelligentInspectionDetail";

export default class IntelligentInspection extends Component {
  constructor(props) {
    super(props);
    this.reportRef = React.createRef();
    this.state = {
      showDetails: false,
      inspectionMode: {},
      isShowBtn: false
    };
  }
  getData = (data, e, i) => {
    this.setState({
      showDetails: data,
      inspectionMode: e,
      isShowBtn: i
    });
  };
  getBack = (data) => {
    this.setState({
      showDetails: data,
    });
  }
  componentDidMount() { }
  render() {
    return (
      <>
        {this.state.showDetails ? (
          <IntelligentInspectionDetail
            inspectionMode={this.state.inspectionMode}
            isShowBtn={this.state.isShowBtn}
            getBack={this.getBack}
          />
        ) : (
          <div className="contentWrap IntelligentInspection">
            <InspectionTask />
            <InspectionRecords
              getData={this.getData}
            />
          </div>
        )}
      </>
    );
  }
}
