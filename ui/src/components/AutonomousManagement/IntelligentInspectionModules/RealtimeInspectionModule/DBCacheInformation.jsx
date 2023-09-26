import React, { Component } from 'react';
import { Col, Row, Card, Collapse } from 'antd';
import NodeEchartFormWork from '../../../NodeInformation/NodeModules/NodeEchartFormWork';

const { Panel } = Collapse;
export default class DBCacheInformation extends Component {
  static propTypes = {
    dbCacheInformation: PropTypes.object.isRequired,
  };
  constructor(props) {
    super(props)
    this.state = {
      primitiveData: [],
      serviceData: [],
      vectorKey: '',
      isShow: false

    }
  }
  compare(property) {
    return function (a, b) {
      var value1 = a.labels[property];
      var value2 = b.labels[property];
      return value1 - value2;
    }
  }

  getdbCache(data) {
    let result = []

    result.push(data['pg_db_blks_read'] ?? [], data['pg_db_blks_hit'] ?? [], data['pg_db_blks_access'] ?? [])
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
        let data3 = { 'legend': [{ image: '', description: 'Hit Rate/Write' }], 'xAxisData': item[2].timestamps, 'seriesData': [{ data: item[2].values, description: 'Hit Rate/Write', colors: '#9185F0' }], 'flg': 0, 'legendFlg': 2, title: 'Hit Rate/Write', 'unit': '', 'fixedflg': 0 }
        chartData.push(data1, data2, data3)
        serviceArray.push(chartData)
      })
      this.setState(() => ({
        serviceData: serviceArray,
        primitiveData: primitiveData,
        vectorKey: 0,
        isShow: true
      }), () => {
        this.onChange(0)
      })
    }

  }
  UNSAFE_componentWillReceiveProps(nextProps) {
    this.getdbCache(nextProps.dbCacheInformation);
  }
  onChange = (key) => {
    this.setState({ vectorKey: key })
  };
  render() {
    return (
      <Card title="DB  Cache" className="mb-10">
        {
          this.state.isShow ? this.state.serviceData.map((item, index) => {
            return (
              <Collapse activeKey={this.state.vectorKey} onChange={(key) => { this.onChange(key) }} expandIconPosition='end' >
                <Panel header={this.state.primitiveData[index][0].labels.datname} key={index} forceRender={true} className='panelStyle'>
                  <Row gutter={10}>
                    <Col className="gutter-row cpuborder" span={12}>
                      <NodeEchartFormWork echartData={item[0]} />
                    </Col>
                    <Col className="gutter-row cpuborder" span={12}>
                      <NodeEchartFormWork echartData={item[1]} />
                    </Col>
                    <Col className="gutter-row cpuborder" span={24}>
                      <NodeEchartFormWork echartData={item[2]} />
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
