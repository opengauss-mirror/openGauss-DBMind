import React, { Component } from 'react';
import { Col, Row, Card, Collapse } from 'antd';
import NodeEchartFormWork from '../../../NodeInformation/NodeModules/NodeEchartFormWork';

export default class DBMemory extends Component {
    static propTypes = {
        dbMemory: PropTypes.object.isRequired,
    };
    constructor(props) {
        super(props)
        this.state = {

            primitiveDataAll: [],
            serviceAllData: [],
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
    getdbMemory(data) {
        let result = []
        Object.keys(data).forEach(item => {
            result.push(data[item])
        })
        if (result[0]) {
            let newResult = [], xDataArray = [[], [], [], [], [], [], []], yDataArray = [[], [], [], [], [], [], []]
            newResult = [result[0], result[1], result[2], result[3], result[9], result[8], result[7]]
            newResult.forEach((item, index) => {
                xDataArray[index] = item[0].timestamps
            });
            newResult.forEach((item, index) => {
                item[0].values.forEach((oitem) => {
                    yDataArray[index].push(oitem)
                });
            });
            let data1 = { 'legend': [{ image: '', description: 'Used Memory' }, { image: '', description: 'peak_memory' }, { image: '', description: 'used_shrctx' }, { image: '', description: 'peak_shrctx' }], 'xAxisData': xDataArray[0], 'seriesData': [{ data: yDataArray[0], description: 'used_memory', colors: '#2DA769' }, { data: yDataArray[1], description: 'peak_memory', colors: '#EC6F1A' }, { data: yDataArray[2], description: 'used_shrctx', colors: '#EEBA18' }, { data: yDataArray[3], description: 'peak_shrctx', colors: '#5890FD' }], 'flg': 0, 'legendFlg': 2, title: ["Max Dynamic Memory", result[0][0].values[0] + 'MB'], 'unit': '', 'fixedflg': 0 }
            let data2 = { 'legend': [{ image: '', description: 'Shared Memory' }], 'xAxisData': xDataArray[4], 'seriesData': [{ data: yDataArray[4], description: 'Shared Memory', colors: '#EEBA18' }], 'flg': 0, 'legendFlg': 2, title: ['Max Shared Memory', result[1][0].values[0] + 'MB'], 'unit': '', 'fixedflg': 0 }
            let data3 = { 'legend': [{ image: '', description: 'Process Memory' }], 'xAxisData': xDataArray[5], 'seriesData': [{ data: yDataArray[5], description: 'Process Memory', colors: '#EC6F1A' }], 'flg': 0, 'legendFlg': 2, title: ['Max Process Memory', result[2][0].values[0] + 'MB'], 'unit': '', 'fixedflg': 0 }
            let data4 = { 'legend': [{ image: '', description: 'Used Memory' }], 'xAxisData': xDataArray[6], 'seriesData': [{ data: yDataArray[6], description: 'Used Memory', colors: '#2070F3' }], 'flg': 0, 'legendFlg': 2, title: 'Other Used Memory', 'unit': '', 'fixedflg': 0 }
            this.setState({
                chartData1: data1,
                chartData2: data2,
                chartData3: data3,
                chartData4: data4,
            })
        }

    }
    UNSAFE_componentWillReceiveProps(nextProps) {
        this.getdbMemory(nextProps.dbMemory);
    }

    onChange = (key) => {
        this.setState({ vectorKey: key })
    };
    render() {
        return (
            <Card title="Memory" className="mb-10">
                <Row gutter={[10, 10]}>

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
        )
    }
}
