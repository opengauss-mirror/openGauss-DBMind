import React, { Component } from 'react';
import { Col, Row, Card } from 'antd';
import NodeEchartFormWork from '../../../NodeInformation/NodeModules/NodeEchartFormWork';

export default class DBUsage extends Component {
    static propTypes = {
        dbUsage: PropTypes.object.isRequired,
    };
    constructor(props) {
        super(props)
        this.state = {
            chartData1: {},
            chartData2: {},
        }
    }

    async getdbUsage(data) {
        let result = []
        Object.keys(data).forEach(item => {
            result.push(data[item])
        })
        if (result[0]) {
            let xDataArray = [[], [], []], yDataArray = [[], [], []]
            result.forEach((item, index) => {
                xDataArray[index] = item[0].timestamps
            });
            result.forEach((item, index) => {
                item[0].values.forEach((oitem) => {
                    yDataArray[index].push(oitem.toFixed(2))
                });
            });
            let data1 = { 'legend': [{ image: '', description: 'Cpu Time' }], 'xAxisData': xDataArray[0], 'seriesData': [{ data: yDataArray[0], description: 'Cpu Time', colors: '#EB6E19' }], 'flg': 0, 'legendFlg': 2, title: 'Cpu Time', 'unit': '', 'fixedflg': 0 }
            let data2 = { 'legend': [{ image: '', description: 'Phyblkrd' }, { image: '', description: 'Phyblkwrt' }], 'xAxisData': xDataArray[1], 'seriesData': [{ data: yDataArray[1], description: 'Phyblkrd', colors: '#9184F0' }, { data: yDataArray[2], description: 'Phyblkwrt', colors: '#5990FD' }], 'flg': 0, 'legendFlg': 2, title: 'Phyblkrd/Phyblkwrt', 'unit': '', 'fixedflg': 0 }
            this.setState({
                chartData1: data1,
                chartData2: data2,
            })
        }

    }
    UNSAFE_componentWillReceiveProps(nextProps) {
        this.getdbUsage(nextProps.dbUsage);
    }

    render() {
        return (
            <Card title="DB Resource Usage" className="mb-10">
                <Row gutter={[10, 10]} className="mb-10">
                    <Col className="gutter-row cpuborder" span={12}>
                        <NodeEchartFormWork echartData={this.state.chartData1} />
                    </Col>
                    <Col className="gutter-row cpuborder" span={12}>
                        <NodeEchartFormWork echartData={this.state.chartData2} />
                    </Col>

                </Row>
            </Card>
        )
    }
}
