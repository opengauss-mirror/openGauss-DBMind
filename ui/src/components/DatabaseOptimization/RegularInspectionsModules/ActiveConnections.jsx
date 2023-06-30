import React, { Component } from "react";
import { Card, Table, Row, Col } from "antd";
import PropTypes from "prop-types";
import ResizeableTitle from "../../common/ResizeableTitle";
import "../../../assets/css/main/databaseOptimization.css";
import { formatTableTitle } from "../../../utils/function";

export default class ActiveConnections extends Component {
  static propTypes = {
    activeConnections: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props);
    this.state = {
      dataSource: [],
      
    };
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  handleTableData(data) {
 
    let activeArr = [];

    for (let i in data) {
      let activeObj = {
        name: i,
        value: data[i],
      };
      activeArr.push(activeObj);
    }
    this.setState(() => ({
      dataSource: activeArr,
     
    }));
  }
  handleResize = (index) => (e, { size }) => {
    this.setState(({ columns }) => {
      const nextColumns = [...columns];
      nextColumns[index] = {
        ...nextColumns[index],
        width: size.width,
      };
      return { columns: nextColumns };
    });
  };
  UNSAFE_componentWillReceiveProps(nextProps) {
    this.handleTableData(nextProps.activeConnections);
  }
  render() {
   
    return (
      <div className="tableDiv">
        <Card title="Active Connections" style={{ height: 129 }}>
          <ul style={{ width: "100%", display: "flex" }}>
           
            {this.state.dataSource.map((item) => {
              return (
                <li className="connectionTitle">
                  <p>{item.name}</p>
                  <h4>{item.value}</h4>
                </li>
              );
            })}
          </ul>
        </Card>
      </div>
    );
  }
}
