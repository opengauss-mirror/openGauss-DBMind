import React, { Component } from 'react';
import { Col, Row, Collapse, message } from 'antd';
import CurrentReceiveRate from '../../assets/imgs/Current Receive Rate.png';
import CurrentSendingRate from '../../assets/imgs/Current Sending Rate.png';
import ReceiveDrop from '../../assets/imgs/Receive_drop.png';
import TransmitDrop from '../../assets/imgs/Transmit_drop.png';
import TransmitError from '../../assets/imgs/Transmit_error.png';
import ReceiveError from '../../assets/imgs/Receive_error.png';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import { commonMetricMethod } from '../../utils/function';

const { Panel } = Collapse;
const metricData = ['os_network_receive_bytes', 'os_network_transmit_bytes', 'os_network_receive_drop', 'os_network_transmit_drop', 'os_network_receive_error', 'os_network_transmit_error']
export default class NodeNetwork extends Component {
  constructor(props) {
    super(props)
    this.state = {
      primitiveDataAll: [],
      networkAllData: [],
      vectorKey: ["0"],
      param: {
        instance: this.props.selValue,
        latest_minutes: this.props.selTimeValue ? this.props.selTimeValue : null,
        regex: true,
        fetch_all: true,
        from_timestamp: this.props.startTime ? this.props.startTime : null,
        to_timestamp: this.props.endTime ? this.props.endTime : null
      }
    }
  }
  compare(property) {
    // 字符串字段排序，避免使用数值减法导致 NaN
    return function (a, b) {
      const v1 = (a && a.labels && a.labels[property]) || ''
      const v2 = (b && b.labels && b.labels[property]) || ''
      return String(v1).localeCompare(String(v2))
    }
  }
  async getNetworkDataAll() {
    Promise.all([
      commonMetricMethod(this.state.param, { label: metricData[0] }),
      commonMetricMethod(this.state.param, { label: metricData[1] }),
      commonMetricMethod(this.state.param, { label: metricData[2] }),
      commonMetricMethod(this.state.param, { label: metricData[3] }),
      commonMetricMethod(this.state.param, { label: metricData[4] }),
      commonMetricMethod(this.state.param, { label: metricData[5] })
    ]).then((result) => {
      const arrays = (result || []).map(arr => Array.isArray(arr) ? arr : [])
      // 排序（按 device 字符串）
      arrays.forEach(arr => arr.sort(this.compare('device')))

      // 选择一个非空数组作为设备基准
      const baseIdx = arrays.findIndex(arr => Array.isArray(arr) && arr.length > 0)
      if (baseIdx === -1) {
        this.setState({ networkAllData: [], primitiveDataAll: [] })
        return
      }
      const baseArr = arrays[baseIdx]
      const devices = baseArr.map(s => (s.labels && s.labels.device) || '')

      const primitiveDataAll = []
      const networkAllArray = []
      devices.forEach((dev) => {
        // 按设备名在各指标数组中查找对应序列
        const group = arrays.map(arr => arr.find(s => s && s.labels && s.labels.device === dev))
        primitiveDataAll.push(group)
        const ts = (group[0] && group[0].timestamps) || (group[1] && group[1].timestamps) || []
        const recvVals = (group[0] && group[0].values) || (group[1] ? Array(group[1].values.length).fill(0) : [])
        const sendVals = (group[1] && group[1].values) || (group[0] ? Array(group[0].values.length).fill(0) : [])
        const rdropVals = (group[2] && group[2].values) || (group[3] ? Array(group[3].values.length).fill(0) : [])
        const tdropVals = (group[3] && group[3].values) || (group[2] ? Array(group[2].values.length).fill(0) : [])
        const rerrVals = (group[4] && group[4].values) || (group[5] ? Array(group[5].values.length).fill(0) : [])
        const terrVals = (group[5] && group[5].values) || (group[4] ? Array(group[4].values.length).fill(0) : [])

        const data1 = { 'legend': [{ image: CurrentReceiveRate, description: 'Current Receive Rate' }], 'xAxisData': ts, 'seriesData': [{ data: recvVals, description: 'Current Receive Rate', colors: '#2DA769' }], 'flg': 0, 'legendFlg': 1, 'unit': 'MB/s', 'fixedflg': 4, 'toolBox': true }
        const data2 = { 'legend': [{ image: CurrentSendingRate, description: 'Current Sending Rate' }], 'xAxisData': ts, 'seriesData': [{ data: sendVals, description: 'Current Sending Rate', colors: '#5990FD' }], 'flg': 0, 'legendFlg': 1, 'unit': 'MB/s', 'fixedflg': 4, 'toolBox': true }
        const data3 = { 'legend': [{ image: ReceiveDrop, description: 'Receive Drop' }, { image: TransmitDrop, description: 'Transmit Drop' }], 'xAxisData': ts, 'seriesData': [{ data: rdropVals, description: 'Receive Drop', colors: '#2DA769' }, { data: tdropVals, description: 'Transmit Drop', colors: '#EC6F1A' }], 'flg': 0, 'legendFlg': 1, 'unit': '', 'fixedflg': 4, 'toolBox': true }
        const data4 = { 'legend': [{ image: ReceiveError, description: 'Receive Error' }, { image: TransmitError, description: 'Transmit Error' }], 'xAxisData': ts, 'seriesData': [{ data: rerrVals, description: 'Receive Error', colors: '#F43146' }, { data: terrVals, description: 'Transmit Error', colors: '#9185F0' }], 'flg': 0, 'legendFlg': 1, 'unit': '', 'fixedflg': 4, 'toolBox': true }
        networkAllArray.push([data1, data2, data3, data4])
      })

      this.setState(() => ({
        networkAllData: networkAllArray,
        primitiveDataAll: primitiveDataAll,
      }), () => {
        this.onChange(this.state.vectorKey)
      })
    }).catch((error) => {
      console.log('error', error)
    })
  }
  componentDidUpdate(prevProps) {
    if (prevProps.selValue !== this.props.selValue || prevProps.selTimeValue !== this.props.selTimeValue || prevProps.startTime !== this.props.startTime || prevProps.endTime !== this.props.endTime || prevProps.tabkey !== this.props.tabkey) {
      this.setState(() => ({
        param: Object.assign(this.state.param, { instance: this.props.selValue, latest_minutes: this.props.selTimeValue ? this.props.selTimeValue : null, from_timestamp: this.props.startTime, to_timestamp: this.props.endTime, regex: true })
      }), () => {
        if (this.props.tabkey === "4") {
          this.getNetworkDataAll()
        }
      })
    }
  }
  componentDidMount() {
    this.getNetworkDataAll()
  }
  onChange = (key) => {
    this.setState({ vectorKey: key }, () => {
      this.forceUpdate();
    })
  };
  render() {
    return (
      <div className='nodeNetwork'>

        {
          this.state.networkAllData.length > 0 ? this.state.networkAllData.map((item, index) => {
            return (
              <Collapse activeKey={this.state.vectorKey} onChange={(key) => { this.onChange(key) }} expandIconPosition='end' >
                <Panel header={
                  <Row gutter={[0, 12]}>
                    <Col className="gutter-row" span={3}>
                      <span className='networkPanelheader'>{this.state.primitiveDataAll[index][0] ? this.state.primitiveDataAll[index][0].labels.device : 'Default'}</span>
                    </Col>
                    <Col className="gutter-row" span={3}>
                      <span className='panelTitleSize' >status:</span>
                      <span className='panelTitleBold' >Enable</span>
                      <span className='panelCircle circleColorGreen'></span>
                    </Col>
                    <Col className="gutter-row" span={5}>
                      <span className='panelTitleSize'>Current Recevice Rate:</span>
                      <span className='panelTitleBold' >{this.state.primitiveDataAll[index][0] ? (this.state.primitiveDataAll[index][0].values[this.state.primitiveDataAll[index][0].values.length - 1] * 100).toFixed(2) : 0}MB/s</span>
                      <span className='panelCircle circleColorPurple'></span>
                    </Col>
                    <Col className="gutter-row" span={5}>
                      <span className='panelTitleSize'>Current Sending Rate</span>
                      <span className='panelTitleBold' >{this.state.primitiveDataAll[index][1] ? (this.state.primitiveDataAll[index][1].values[this.state.primitiveDataAll[index][1].values.length - 1] * 100).toFixed(2) : 0}MB/s</span>
                      <span className='panelCircle circleColorBlue'></span>
                    </Col>
                  </Row>
                } key={index} forceRender={true} className='panelStyle'>
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
