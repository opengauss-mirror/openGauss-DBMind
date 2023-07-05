import React, { Component } from 'react';
import { Col, Row, Collapse, message, Card } from 'antd';


import NodeEchartFormWork from '../../../NodeInformation/NodeModules/NodeEchartFormWork';

const panelStyle = {
    marginBottom: 15,
    background: "#F6F6F6",
    borderRadius: '3px 3px 0 0',
};
const { Panel } = Collapse;
export default class SystemIO extends Component {
    static propTypes = {
        systemNetwork: PropTypes.object.isRequired,
    };
    constructor(props) {
        super(props)
        this.state = {
            networkData: [],
            primitiveData: [],
            vectorKey: 0,
        }
    }

    compare(property) {
        return function (a, b) {

            var value1 = a.labels[property];
            var value2 = b.labels[property];
            return value1 - value2;
        }
    }
    getNetworkData(data) {
        let result = []

        result.push(data['os_network_receive_bytes'], data['os_network_transmit_bytes'], data['os_network_receive_drop'], data['os_network_transmit_drop'], data['os_network_receive_error'], data['os_network_transmit_error'])
        if (result[0]) {
            result.forEach((item, index) => {
                item.sort(this.compare('device'))
            });
            let primitiveData = [], networkArray = []
            result[0].forEach((item, index) => {
                let DataItems = []
                result.forEach((oitem, oindex) => {
                    DataItems.push(oitem[index])
                });
                primitiveData.push(DataItems)
            });
            primitiveData.forEach((item, index) => {
                let chartData = [], data1 = {}, data2 = {}, data3 = {}, data4 = {}
                data1 = { 'legend': [{ image: "", description: 'Current Receive Rate' }], 'xAxisData': item[0] ? item[0].timestamps : item[1].timestamps, 'seriesData': [{ data: item[0] ? item[0].values : [...item[1].values.fill(0)], description: 'Current Receive Rate', colors: '#2DA769' }], 'flg': 0, 'legendFlg': 1, 'unit': 'KB/s', 'fixedflg': 4 }
                data2 = { 'legend': [{ image: "", description: 'Current Sending Rate' }], 'xAxisData': item[1] ? item[1].timestamps : item[0].timestamps, 'seriesData': [{ data: item[1] ? item[1].values : [...item[0].values.fill(0)], description: 'Current Sending Rate', colors: '#5990FD' }], 'flg': 0, 'legendFlg': 1, 'unit': 'KB/s', 'fixedflg': 4 }
                data3 = { 'legend': [{ image: "", description: 'Receive Drop' }, { image: "", description: 'Transmit Drop' }], 'xAxisData': item[2] ? item[2].timestamps : item[3].timestamps, 'seriesData': [{ data: item[2] ? item[2].values : [...item[3].values.fill(0)], description: 'Receive Drop', colors: '#2DA769' }, { data: item[3] ? item[3].values : [...item[2].values.fill(0)], description: 'Transmit Drop', colors: '#EC6F1A' }], 'flg': 0, 'legendFlg': 1, 'unit': '', 'fixedflg': 4 }
                data4 = { 'legend': [{ image: "", description: 'Receive Error' }, { image: "", description: 'Transmit Error' }], 'xAxisData': item[4] ? item[4].timestamps : item[5].timestamps, 'seriesData': [{ data: item[4] ? item[4].values : [...item[5].values.fill(0)], description: 'Receive Error', colors: '#F43146' }, { data: item[5] ? item[5].values : [...item[4].values.fill(0)], description: 'Transmit Error', colors: '#9185F0' }], 'flg': 0, 'legendFlg': 1, 'unit': '', 'fixedflg': 4 }
                chartData.push(data1, data2, data3, data4)
                networkArray.push(chartData)
            })
            this.setState(() => ({
                networkData: networkArray,
                primitiveData: primitiveData,
                vectorKey: 0,
            }), () => {
                this.onChange(0)
            })
        }

    }

    UNSAFE_componentWillReceiveProps(nextProps) {
        this.getNetworkData(nextProps.systemNetwork)

    }

    onChange = (key) => {
        this.setState({ vectorKey: key })
    };
    render() {
        return (
            <Card title="Network" className="mb-10 nodeNetwork io">
                {
                    this.state.networkData.length > 0 ? this.state.networkData.map((item, index) => {
                        return (
                            <Collapse activeKey={this.state.vectorKey} onChange={(key) => { this.onChange(key) }} expandIconPosition='end' style={{
                                background: '#ffffffff', borderRadius: '3px 3px 0 0'
                            }}>
                                <Panel header={
                                    <Row gutter={[10, 10]}>
                                        <Col className="gutter-row panelDevice" span={2}>
                                            <span className='networkPanelheader'>{this.state.primitiveData[index][0].labels.device}</span>
                                        </Col>
                                        <Col className="gutter-row" span={3}>
                                            <span className='panelTitleSize' >status:</span>
                                            <span className='panelTitleBold' >Enable</span>
                                            <span className='panelCircle circleColorGreen'></span>
                                        </Col>
                                        <Col className="gutter-row" span={5}>
                                            <span className='panelTitleSize'>Current Recevice Rate:</span>
                                            <span className='panelTitleBold' >{(this.state.primitiveData[index][0].values[this.state.primitiveData[index][0].values.length - 1] * 100).toFixed(2)}KB/s</span>
                                            <span className='panelCircle circleColorPurple'></span>
                                        </Col>
                                        <Col className="gutter-row" span={5}>
                                            <span className='panelTitleSize'>Current Sending Rate</span>
                                            <span className='panelTitleBold' >{(this.state.primitiveData[index][1].values[this.state.primitiveData[index][1].values.length - 1] * 100).toFixed(2)}KB/s</span>
                                            <span className='panelCircle circleColorBlue'></span>
                                        </Col>
                                    </Row>
                                } key={index} forceRender={true} style={panelStyle}>
                                    <Row gutter={[10, 10]}>
                                        <Col className="gutter-row cpuborder" span={12}>
                                            <NodeEchartFormWork echartData={item[0]} />
                                        </Col>
                                        <Col className="gutter-row cpuborder" span={12}>
                                            <NodeEchartFormWork echartData={item[1]} />
                                        </Col>
                                        <Col className="gutter-row cpuborder" span={12}>
                                            <NodeEchartFormWork echartData={item[2]} />
                                        </Col>
                                        <Col className="gutter-row cpuborder" span={12}>
                                            <NodeEchartFormWork echartData={item[3]} />
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
