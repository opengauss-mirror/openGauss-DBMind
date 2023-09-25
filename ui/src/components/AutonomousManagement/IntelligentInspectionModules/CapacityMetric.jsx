import React, { Component } from "react";
import { Row, Col, Card, Badge, Table, Descriptions } from "antd";
import ResizeableTitle from "../../common/ResizeableTitle";
import { formatTableTitleToUpper } from "../../../utils/function";
import { responseInterceptor } from "http-proxy-middleware";

export default class CapacityMetric extends Component {
    constructor(props) {
        super(props);
        this.state = {
            dataSource: [],
            columns: [],
        };
    }
    components = {
        header: {
            cell: ResizeableTitle,
        },
    };
    handleTableData(header, rows) {
        let inspectionColumObj = {};
        let tableHeader = [];
        tableHeader.push({
            title: formatTableTitleToUpper(header[0]),
            dataIndex: header[0],
            key: header[0],
            ellipsis: true,
            width:"10%"
        
        });
        rows.forEach((item, index) => {
            inspectionColumObj = {
                title: item[0],
                dataIndex: item[0],
                key: item[0],
                ellipsis: true,
            };

            tableHeader.push(inspectionColumObj);
        });

        let  res = [];
        for (let i = 1; i < header.length; i++) {
            let obj = {}
            rows.forEach((item, index) => {       
                obj[tableHeader[index + 1].title] = item[i];
                obj[tableHeader[0].title] = formatTableTitleToUpper(header[i]);
            });
            res.push(obj);
        }
        this.setState(
            {
                dataSource: res,
                columns: tableHeader,
            }
        );
    }
    componentDidMount() {
        this.handleTableData(
            this.props.capacityMetric.header,
            this.props.capacityMetric.rows
        );
    }
    render() {
        return (
            <Card title="CapacityMetric" className="CapacityMetric mb-10">
                <Table
                    bordered
                    components={this.components}
                    columns={this.state.columns}
                    dataSource={this.state.dataSource}
                    size="small"
                    rowKey={(record) => record.key}
                    pagination={false}
                    style={{ height: 170, overflowY: "auto" }}
                    scroll={{ x: "100%" }}
                />
            </Card>
        );
    }
}
