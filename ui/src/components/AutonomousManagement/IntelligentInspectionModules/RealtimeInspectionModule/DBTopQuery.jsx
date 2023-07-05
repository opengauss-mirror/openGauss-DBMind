import React, { Component } from 'react';
import { Col, Row, Card, Collapse } from 'antd';
import NodeEchartFormWork from '../../../NodeInformation/NodeModules/NodeEchartFormWork';


const panelStyle = {
    marginBottom: 15,
    background: "#F6F6F6",
    borderRadius: '3px 3px 0 0',
};
const { Panel } = Collapse;
export default class DBTopQuery extends Component {
    constructor(props) {
        super(props)
        this.state = {

            primitiveData: [],
            serviceData: [],
            vectorKey: ''
        }
    }

    compare(property) {
        return function (a, b) {

            var value1 = a.labels[property];
            var value2 = b.labels[property];
            return value1 - value2;
        }
    }
    async getCacheInformationData() {
        let result = [
            [
                {
                    timestamps: [
                        1686190269661,
                        1686190299661,
                        1686190329661,
                        1686190359661,
                        1686190389661,
                        1686190419661,
                        1686190449661,
                        1686190479661,
                        1686190509661,
                        1686190539661,
                    ],
                    labels: { devices: 'vda' },
                    values: [
                        0.0082,
                        0.00819,
                        0.013343,
                        0.003243545,
                        0.0124324,
                        0.223445,
                        0.0124325435,
                        0.024354,
                        0.02343654,
                        0.0089,
                    ],
                },
                {
                    timestamps: [
                        1686190269661,
                        1686190299661,
                        1686190329661,
                        1686190359661,
                        1686190389661,
                        1686190419661,
                        1686190449661,
                        1686190479661,
                        1686190509661,
                        1686190539661,
                    ],
                    labels: { devices: 'vdb' },
                    values: [
                        0.0082,
                        0.00819,
                        0.013343,
                        0.003243545,
                        0.1124324,
                        0.023445,
                        0.0124325435,
                        0.024354,
                        0.02343654,
                        0.0089,
                    ],
                },
            ],
            [
                {
                    timestamps: [
                        1686190269661,
                        1686190299661,
                        1686190329661,
                        1686190359661,
                        1686190389661,
                        1686190419661,
                        1686190449661,
                        1686190479661,
                        1686190509661,
                        1686190539661,
                    ],
                    labels: { devices: 'vda' },
                    values: [
                        0.0082,
                        0.00819,
                        0.113343,
                        0.003243545,
                        0.0124324,
                        0.023445,
                        0.0124325435,
                        0.024354,
                        0.02343654,
                        0.0089,
                    ],
                },
                {
                    timestamps: [
                        1686190269661,
                        1686190299661,
                        1686190329661,
                        1686190359661,
                        1686190389661,
                        1686190419661,
                        1686190449661,
                        1686190479661,
                        1686190509661,
                        1686190539661,
                    ],
                    labels: { devices: 'vdb' },
                    values: [
                        0.0082,
                        0.00819,
                        0.013343,
                        0.003243545,
                        0.1124324,
                        0.023445,
                        0.0124325435,
                        0.024354,
                        0.02343654,
                        0.0089,
                    ],
                },
            ],
        ]
        if (result[0]) {
            result.forEach((item, index) => {
                item.sort(this.compare('datname'))
            });
            let primitiveData = [], serviceArray = []
            result[0].forEach((item, index) => {
                let DataItems = []
                result.forEach((oitem, oindex) => {

                    DataItems.push(oitem[index])
                });
                primitiveData.push(DataItems)
            });
            primitiveData.forEach((item, index) => {
                let chartData = []
                let data1 = { 'legend': [{ image: '', description: 'Disk Read/Write' }], 'xAxisData': item[0].timestamps, 'seriesData': [{ data: item[0].values, description: 'Disk Read/Write', colors: '#2DA769' }], 'flg': 0, 'legendFlg': 2, title: 'Disk Read/Write', 'unit': '', 'fixedflg': 0 }
                let data2 = { 'legend': [{ image: '', description: 'Cache Read/Write' }], 'xAxisData': item[1].timestamps, 'seriesData': [{ data: item[1].values, description: 'Cache Read/Write', colors: '#5990FD' }], 'flg': 0, 'legendFlg': 2, title: 'Cache Read/Write', 'unit': '', 'fixedflg': 0 }
                chartData.push(data1, data2)
                serviceArray.push(chartData)
            })
            this.setState(() => ({
                serviceData: serviceArray,
                primitiveData: primitiveData,
                vectorKey: 0,
            }), () => {
                this.onChange(0)
            })
        }

    }

    componentDidMount() {
        this.getCacheInformationData()
    }
    onChange = (key) => {
        this.setState({ vectorKey: key })
    };
    render() {
        return (
            <Card title="Top Query" className="mb-10">
                {
                    this.state.serviceData.length > 0 ? this.state.serviceData.map((item, index) => {
                        return (
                            <Collapse activeKey={this.state.vectorKey} onChange={(key) => { this.onChange(key) }} expandIconPosition='end' style={{
                                background: '#ffffffff', borderRadius: '3px 3px 0 0'
                            }}>
                                <Panel header={this.state.primitiveData[index][0].labels.devices} key={index} forceRender={true} style={panelStyle}>
                                    <Row gutter={10}>
                                        <Col className="gutter-row cpuborder" span={12}>
                                            <NodeEchartFormWork echartData={item[0]} />
                                        </Col>
                                        <Col className="gutter-row cpuborder" span={12}>
                                            <NodeEchartFormWork echartData={item[1]} />
                                        </Col>
                                    </Row>
                                </Panel>
                            </Collapse>
                        )

                    }) : ''
                }
            </Card>
        )
    }
}
