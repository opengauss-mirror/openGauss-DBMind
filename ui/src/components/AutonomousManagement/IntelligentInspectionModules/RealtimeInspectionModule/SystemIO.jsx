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
    systemIO: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props)
    this.state = {
      primitiveDataAll: [],
      ioAllData: [],
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
  getIoData(data) {
    let datas = []
    datas.push(data['os_disk_io_read_bytes'], data['os_disk_io_write_bytes'], data['os_disk_io_read_delay'], data['os_disk_io_write_delay'], data['os_disk_iops'], data['os_disk_io_queue_length'], data['os_disk_ioutils'])
    if (datas[0]) {
      datas.forEach((item, index) => {
        item.sort(this.compare('device'))
      });
      let primitiveDataAll = [], ioAllArray = []
      datas[0].forEach((item, index) => {
        let DataItems = []
        datas.forEach((oitem, oindex) => {
          DataItems.push(oitem[index])
        });
        primitiveDataAll.push(DataItems)
      });

      Object.keys(datas).forEach(item => {

      })
      primitiveDataAll.forEach((item, index) => {
        let chartData = []
        let data1 = { 'legend': [{ image: "", description: 'Read Rate' }, { image: "", description: 'Write Rate' }], 'xAxisData': item[0].timestamps, 'seriesData': [{ data: item[0].values, description: 'Read Rate', colors: '#5990FD' }, { data: item[1].values, description: 'Write Rate', colors: '#2DA769' }], 'flg': 0, 'legendFlg': 1, 'unit': 'KB/s', 'fixedflg': 4 }
        let data2 = { 'legend': [{ image: "", description: 'Single Read Time' }, { image: "", description: 'Single Write Time' }], 'xAxisData': item[2].timestamps, 'seriesData': [{ data: item[2].values, description: 'Single Read Time', colors: '#2DA769' }, { data: item[3].values, description: 'Single Write Time', colors: '#5990FD' }], 'flg': 0, 'legendFlg': 1, 'unit': 'KB/ms', 'fixedflg': 4 }
        let data3 = { 'legend': [{ image: "", description: 'IOPS' }], 'xAxisData': item[4].timestamps, 'seriesData': [{ data: item[4].values, description: 'IOPS', colors: '#F43146' }], 'flg': 0, 'legendFlg': 1, 'unit': '/s', 'fixedflg': 4 }
        let data4 = { 'legend': [{ image: "", description: 'Average Queue Length' }], 'xAxisData': item[5].timestamps, 'seriesData': [{ data: item[5].values, description: 'Average Queue Length', colors: '#2DA769' }], 'flg': 0, 'legendFlg': 1, 'unit': '', 'fixedflg': 4 }
        let data5 = { 'legend': [{ image: "", description: 'IOUtils' }], 'xAxisData': item[6].timestamps, 'seriesData': [{ data: item[6].values, description: 'IOUtils', colors: '#9185F0' }], 'flg': 0, 'legendFlg': 1, 'unit': '%', 'fixedflg': 0 }
        chartData.push(data1, data2, data3, data4, data5)
        ioAllArray.push(chartData)
      })


      this.setState(() => ({
        ioAllData: ioAllArray,
        primitiveDataAll: primitiveDataAll,
        vectorKey: 0,
      }), () => {
        this.onChange(0)
      })
    }

  }

  UNSAFE_componentWillReceiveProps(nextProps) {
    this.getIoData(nextProps.systemIO)

  }

  onChange = (key) => {
    this.setState({ vectorKey: key })
  };
  render() {
    return (
      <Card title="IO" className="mb-10">
        {
          this.state.ioAllData.length > 0 ? this.state.ioAllData.map((item, index) => {
            return (
              <Collapse activeKey={this.state.vectorKey} onChange={(key) => { this.onChange(key) }} expandIconPosition='end' style={{
                background: '#ffffffff', borderRadius: '3px 3px 0 0'
              }}>
                <Panel header={this.state.primitiveDataAll[index][0].labels.device} key={index} forceRender={true} style={panelStyle}>
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
                    <Col className="gutter-row cpuborder" span={24}>
                      <NodeEchartFormWork echartData={item[4]} />
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
