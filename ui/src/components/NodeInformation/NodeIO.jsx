import React, { Component } from 'react';
import { Col, Row, Collapse, message } from 'antd';
import AverageQueueLength from '../../assets/imgs/Average Queue Length.png';
import BandwidthUtilization from '../../assets/imgs/Bandwidth Utilization.png';
import Readrate from '../../assets/imgs/Read rate.png';
import SingleReadTime from '../../assets/imgs/Single Read Time.png';
import SingleWriteTime from '../../assets/imgs/Single Write Time.png';
import Tps from '../../assets/imgs/Tps.png';
import Writerate from '../../assets/imgs/Write rate.png';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import { commonMetricMethod } from '../../utils/function';

const metricData = ['os_disk_io_read_bytes', 'os_disk_io_write_bytes', 'os_disk_io_read_delay', 'os_disk_io_write_delay', 'os_disk_iops', 'os_disk_io_queue_length', 'os_disk_ioutils']
const { Panel } = Collapse;
export default class NodeIO extends Component {
  constructor(props) {
    super(props)
    this.state = {
      primitiveDataAll: [],
      ioAllData: [],
      vectorKey: ["0"],
      param: {
        instance: this.props.selValue,
        latest_minutes: this.props.selTimeValue ? this.props.selTimeValue : null,
        fetch_all: true,
        from_timestamp: this.props.startTime ? this.props.startTime : null,
        to_timestamp: this.props.endTime ? this.props.endTime : null
      }
    }
  }
  compare(property) {
    return function (a, b) {
      var value1 = a.labels[property];
      var value2 = b.labels[property];
      return value1 - value2;
    }
  }
  async getIoDataAll() {
    Promise.all([
      commonMetricMethod(this.state.param, { label: metricData[0] }),
      commonMetricMethod(this.state.param, { label: metricData[1] }),
      commonMetricMethod(this.state.param, { label: metricData[2] }),
      commonMetricMethod(this.state.param, { label: metricData[3] }),
      commonMetricMethod(this.state.param, { label: metricData[4] }),
      commonMetricMethod(this.state.param, { label: metricData[5] }),
      commonMetricMethod(this.state.param, { label: metricData[6] })
    ]).then((result) => {
      if (result[0]) {
        result.forEach((item, index) => {
          item.sort(this.compare('device'))
        });
        let primitiveDataAll = [], ioAllArray = []
        result[0].forEach((item, index) => {
          let DataItems = []
          result.forEach((oitem, oindex) => {
            DataItems.push(oitem[index])
          });
          primitiveDataAll.push(DataItems)
        });
        primitiveDataAll.forEach((item, index) => {
          let chartData = []
          let data1 = { 'legend': [{ image: Readrate, description: 'Read Rate' }, { image: Writerate, description: 'Write Rate' }], 'xAxisData': item[0].timestamps, 'seriesData': [{ data: item[0].values, description: 'Read Rate', colors: '#5990FD' }, { data: item[1].values, description: 'Write Rate', colors: '#2DA769' }], 'flg': 0, 'legendFlg': 1, 'unit': 'KB/s', 'fixedflg': 4, 'toolBox': true }
          let data2 = { 'legend': [{ image: SingleReadTime, description: 'Single Read Time' }, { image: SingleWriteTime, description: 'Single Write Time' }], 'xAxisData': item[2].timestamps, 'seriesData': [{ data: item[2].values, description: 'Single Read Time', colors: '#2DA769' }, { data: item[3].values, description: 'Single Write Time', colors: '#5990FD' }], 'flg': 0, 'legendFlg': 1, 'unit': 'KB/ms', 'fixedflg': 4, 'toolBox': true }
          let data3 = { 'legend': [{ image: Tps, description: 'IOPS' }], 'xAxisData': item[4].timestamps, 'seriesData': [{ data: item[4].values, description: 'IOPS', colors: '#F43146' }], 'flg': 0, 'legendFlg': 1, 'unit': '/s', 'fixedflg': 4, 'toolBox': true }
          let data4 = { 'legend': [{ image: AverageQueueLength, description: 'Average Queue Length' }], 'xAxisData': item[5].timestamps, 'seriesData': [{ data: item[5].values, description: 'Average Queue Length', colors: '#2DA769' }], 'flg': 0, 'legendFlg': 1, 'unit': '', 'fixedflg': 4, 'toolBox': true }
          let data5 = { 'legend': [{ image: BandwidthUtilization, description: 'IOUtils' }], 'xAxisData': item[6].timestamps, 'seriesData': [{ data: item[6].values, description: 'IOUtils', colors: '#9185F0' }], 'flg': 0, 'legendFlg': 1, 'unit': '%', 'fixedflg': 0, 'toolBox': true }
          chartData.push(data1, data2, data3, data4, data5)
          ioAllArray.push(chartData)
        })
        this.setState(() => ({
          ioAllData: ioAllArray,
          primitiveDataAll: primitiveDataAll,
        }), () => {
          this.onChange(this.state.vectorKey)
        })
      }
    }).catch((error) => {
      console.log('error', error)
    })
  }
  componentDidUpdate(prevProps) {
    if (prevProps.selValue !== this.props.selValue || prevProps.selTimeValue !== this.props.selTimeValue || prevProps.startTime !== this.props.startTime || prevProps.endTime !== this.props.endTime || prevProps.tabkey !== this.props.tabkey) {
      this.setState(() => ({
        param: Object.assign(this.state.param, { instance: this.props.selValue, latest_minutes: this.props.selTimeValue ? this.props.selTimeValue : null, from_timestamp: this.props.startTime, to_timestamp: this.props.endTime })
      }), () => {
        if (this.props.tabkey === "2") {
          this.getIoDataAll()
        }
      })
    }
  }
  componentDidMount() {
    this.getIoDataAll()
  }
  onChange = (key) => {
    this.setState({ vectorKey: key }, () => {
      this.forceUpdate();
    })
  };
  render() {
    return (
      <div>
        {
          this.state.ioAllData.length > 0 ? this.state.ioAllData.map((item, index) => {
            return (
              <Collapse activeKey={this.state.vectorKey} onChange={(key) => { this.onChange(key) }} expandIconPosition='end'  >
                <Panel header={this.state.primitiveDataAll[index][0].labels.device} key={index} forceRender={true} className='panelStyle'>
                  <Row gutter={[10, 10]}>
                    <Col className="gutter-row cpuborder" span={12}>
                      {this.state.vectorKey.indexOf(index.toString()) !== -1 ? <NodeEchartFormWork echartData={item[0]} /> : ''}
                    </Col>
                    <Col className="gutter-row cpuborder" span={12}>
                      {this.state.vectorKey.indexOf(index.toString()) !== -1 ? <NodeEchartFormWork echartData={item[1]} /> : ''}
                    </Col>
                    <Col className="gutter-row cpuborder" span={12}>
                      {this.state.vectorKey.indexOf(index.toString()) !== -1 ? <NodeEchartFormWork echartData={item[2]} /> : ''}
                    </Col>
                    <Col className="gutter-row cpuborder" span={12}>
                      {this.state.vectorKey.indexOf(index.toString()) !== -1 ? <NodeEchartFormWork echartData={item[3]} /> : ''}
                    </Col>
                    <Col className="gutter-row cpuborder" span={24}>
                      {this.state.vectorKey.indexOf(index.toString()) !== -1 ? <NodeEchartFormWork echartData={item[4]} /> : ''}
                    </Col>
                  </Row>
                </Panel>
              </Collapse>
            )

          }) : ''
        }

      </div>
    )
  }
}
