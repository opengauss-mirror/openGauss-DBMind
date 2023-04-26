import React, { Component } from 'react';
import { Col, Row } from 'antd';
import EmptyImg from '../../assets/imgs/Empty.png';
import NodeEchartFormWork from './NodeModules/NodeEchartFormWork';

export default class RedundantIndexes extends Component {
  constructor(props) {
    super(props)
    this.state = {
      ifShow: false,
      chartData1: {},
      chartData2: {},
    }
  }

  async getResponseTime() {
    let data1 = {
      'legend': [{image: EmptyImg, description: 'Cpu time' }],
      'xAxisData': [
        '2023/02/08 17:47:00',
        '2023/02/08 17:57:00',
        '2023/02/08 18:07:00',
        '2023/02/08 18:17:00',
        '2023/02/08 18:27:00',
        '2023/02/08 18:37:00',
        '2023/02/08 18:47:00'
      ],
      'seriesData': [{ data: [6, 7, 5, 5, 6, 7, 7], description: 'Cpu time', colors: '#5990FD' }],'flg': 0, 'legendFlg':0
    }
    this.setState({
      chartData1: data1
    })
  }
  componentDidMount() {
    this.getResponseTime()
  }

  render() {
    return (
      <div>
        <Row gutter={[10,10]}>
        <Col className="gutter-row cpuborder" span={24}>
            <NodeEchartFormWork echartData={this.state.chartData1} />
          </Col>
        </Row>
      </div>
    )
  }
}
