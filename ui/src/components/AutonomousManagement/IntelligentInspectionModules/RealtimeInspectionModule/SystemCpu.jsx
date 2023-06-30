import React, { Component } from "react";
import { Col, Row, message, Card } from "antd";
import NodeEchartFormWork from "../../../NodeInformation/NodeModules/NodeEchartFormWork";

export default class SystemCpu extends Component {
    static propTypes = {
        systemCpu: PropTypes.object.isRequired,
    };
    constructor(props) {
        super(props);
        this.state = {
            chartData1: {},
            chartData2: {},
            chartData3: {},
            chartData4: {},
        };
    }

    getSystemCpu(data) {
        if (data) {

            let data1 = {
                "legend": [{ image: "", description: "System" }],
                "xAxisData": data['os_cpu_idle_usage'] ? data['os_cpu_idle_usage'][0].timestamps : [],
                "seriesData": [{ data: data['os_cpu_system_usage'] ? data['os_cpu_system_usage'][0].values : [], description: "System", colors: "#2DA769" }],
                "flg": 0,
                "legendFlg": 1,
                "unit": "%",
                "fixedflg": 1,
            },
                data2 = {
                    "legend": [{ image: "", description: "User" }],
                    "xAxisData": data['os_cpu_idle_usage'] ? data['os_cpu_idle_usage'][0].timestamps : [],
                    "seriesData": [{ data: data['os_cpu_user_usage'] ? data['os_cpu_user_usage'][0].values : [], description: "User", colors: "#5990FD" }],
                    "flg": 0,
                    "legendFlg": 1,
                    "unit": "%",
                    "fixedflg": 1,
                },
                data3 = {
                    "legend": [{ image: "", description: "Empty" }],
                    "xAxisData": data['os_cpu_idle_usage'] ? data['os_cpu_idle_usage'][0].timestamps : [],
                    "seriesData": [{ data: data['os_cpu_idle_usage'] ? data['os_cpu_idle_usage'][0].values : [], description: "Empty", colors: "#9185F0" }],
                    "flg": 0,
                    "legendFlg": 1,
                    "unit": "%",
                    "fixedflg": 1,
                },
                data4 = {
                    "legend": [{ image: "", description: "IO Wait" }],
                    "xAxisData": data['os_cpu_idle_usage'] ? data['os_cpu_idle_usage'][0].timestamps : [],
                    "seriesData": [
                        {
                            data: data['os_cpu_iowait_usage'] ? data['os_cpu_iowait_usage'][0].values : [],
                            description: "IO Wait",
                            colors: "#EC6F1A",
                        },
                    ],
                    "flg": 0,
                    "legendFlg": 1,
                    "unit": "%",
                    "fixedflg": 2,
                };
            this.setState({
                chartData1: data1,
                chartData2: data2,
                chartData3: data3,
                chartData4: data4,
            })
        }
    }
    UNSAFE_componentWillReceiveProps(nextProps) {
        this.getSystemCpu(nextProps.systemCpu);
    }

    render() {
        return (
            <Card title="CPU" className="mb-10">
                <Row gutter={[10, 10]} className="mb-10">
                    <Col className="gutter-row cpuborder" span={12}>
                        <NodeEchartFormWork echartData={this.state.chartData1} />
                    </Col>
                    <Col className="gutter-row cpuborder" span={12}>
                        <NodeEchartFormWork echartData={this.state.chartData2} />
                    </Col>
                    <Col className="gutter-row cpuborder" span={12}>
                        <NodeEchartFormWork echartData={this.state.chartData3} />
                    </Col>
                    <Col className="gutter-row cpuborder" span={12}>
                        <NodeEchartFormWork echartData={this.state.chartData4} />
                    </Col>
                </Row>
            </Card>
        );
    }
}
