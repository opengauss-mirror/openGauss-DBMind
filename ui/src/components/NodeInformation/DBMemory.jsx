import React, { Component } from 'react';
import { Col, Row } from 'antd';
import { Empty, message } from 'antd';
import icon9 from '../../assets/imgs/icon9.png';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import { getDBMemoryData } from '../../api/autonomousManagement';
import { commonMetricMethod } from '../../utils/function';

const metricData = ['type=max_dynamic_memory', 'type=max_shared_memory', 'type=max_process_memory',
  'type=dynamic_used_memory', 'type=dynamic_peak_memory', 'type=dynamic_used_shrctx', 'type=dynamic_peak_shrctx',
  'type=shared_used_memory', 'type=process_used_memory', 'type=other_used_memory']
export default class DBMemory extends Component {
  constructor(props) {
    super(props)
    this.state = {
      chartData1: {},
      chartData2: {},
      chartData3: {},
      chartData4: {},
      param: {
        instance: this.props.selValue,
        fetch_all: false,
        regex: false,
        from_timestamp: this.props.startTime ? this.props.startTime : null,
        to_timestamp: this.props.endTime ? this.props.endTime : null
      },
      selTimeValue: this.props.selTimeValue ? this.props.selTimeValue : null
    }
  }
  divisionItem(arr1, arr2) {
    let newArr = arr1.map(function (item, index) {
      return item / arr2[index];
    });
    return newArr;
  }
  async getDBMemoryDataAll() {
    Promise.all([
      commonMetricMethod(this.state.param, { labels: metricData[0] }, getDBMemoryData),
      commonMetricMethod(this.state.param, { labels: metricData[1] }, getDBMemoryData),
      commonMetricMethod(this.state.param, { labels: metricData[2] }, getDBMemoryData),
      commonMetricMethod(this.state.param, { latest_minutes: this.state.selTimeValue, labels: metricData[3] }, getDBMemoryData),
      commonMetricMethod(this.state.param, { latest_minutes: this.state.selTimeValue, labels: metricData[4] }, getDBMemoryData),
      commonMetricMethod(this.state.param, { latest_minutes: this.state.selTimeValue, labels: metricData[5] }, getDBMemoryData),
      commonMetricMethod(this.state.param, { latest_minutes: this.state.selTimeValue, labels: metricData[6] }, getDBMemoryData),
      commonMetricMethod(this.state.param, { latest_minutes: this.state.selTimeValue, labels: metricData[7] }, getDBMemoryData),
      commonMetricMethod(this.state.param, { latest_minutes: this.state.selTimeValue, labels: metricData[8] }, getDBMemoryData),
      commonMetricMethod(this.state.param, { latest_minutes: this.state.selTimeValue, labels: metricData[9] }, getDBMemoryData)
    ]).then((result) => {
      if (result[0]) {
        let newResult = [], xDataArray = [[], [], [], [], [], [], []], yDataArray = [[], [], [], [], [], [], []]
        newResult = [result[3], result[4], result[5], result[6], result[7], result[8], result[9]]
        newResult.forEach((item, index) => {
          xDataArray[index] = item[0].timestamps
        });
        newResult.forEach((item, index) => {
          item[0].values.forEach((oitem) => {
            yDataArray[index].push(oitem)
          });
        });
        let data1 = { 'legend': [{ image: '', description: 'Used Memory' }, { image: '', description: 'peak_memory' }, { image: '', description: 'used_shrctx' }, { image: '', description: 'peak_shrctx' }], 'xAxisData': xDataArray[0], 'seriesData': [{ data: yDataArray[0], description: 'used_memory', colors: '#2DA769' }, { data: yDataArray[1], description: 'peak_memory', colors: '#EC6F1A' }, { data: yDataArray[2], description: 'used_shrctx', colors: '#EEBA18' }, { data: yDataArray[3], description: 'peak_shrctx', colors: '#5890FD' }], 'flg': 0, 'legendFlg': 2, title: ["Max Dynamic Memory", result[0][0].values[0] + 'MB'], 'unit': '', 'fixedflg': 0, 'toolBox': true }
        let data2 = { 'legend': [{ image: '', description: 'Shared Memory' }], 'xAxisData': xDataArray[4], 'seriesData': [{ data: yDataArray[4], description: 'Shared Memory', colors: '#EEBA18' }], 'flg': 0, 'legendFlg': 2, title: ['Max Shared Memory', result[1][0].values[0] + 'MB'], 'unit': '', 'fixedflg': 0, 'toolBox': true }
        let data3 = { 'legend': [{ image: '', description: 'Process Memory' }], 'xAxisData': xDataArray[5], 'seriesData': [{ data: yDataArray[5], description: 'Process Memory', colors: '#EC6F1A' }], 'flg': 0, 'legendFlg': 2, title: ['Max Process Memory', result[2][0].values[0] + 'MB'], 'unit': '', 'fixedflg': 0, 'toolBox': true }
        let data4 = { 'legend': [{ image: '', description: 'Used Memory' }], 'xAxisData': xDataArray[6], 'seriesData': [{ data: yDataArray[6], description: 'Used Memory', colors: '#2070F3' }], 'flg': 0, 'legendFlg': 2, title: 'Other Used Memory', 'unit': '', 'fixedflg': 0, 'toolBox': true }
        this.setState({
          chartData1: data1,
          chartData2: data2,
          chartData3: data3,
          chartData4: data4,
        })
      }
    }).catch((error) => {
      console.log('error', error)
    })
  }
  componentDidUpdate(prevProps) {
    if (prevProps.selValue !== this.props.selValue || prevProps.selTimeValue !== this.props.selTimeValue || prevProps.startTime !== this.props.startTime || prevProps.endTime !== this.props.endTime || prevProps.tabkey !== this.props.tabkey) {
      this.setState(() => ({
        param: Object.assign(this.state.param, { instance: this.props.selValue, from_timestamp: this.props.startTime, to_timestamp: this.props.endTime }),
        selTimeValue: this.props.selTimeValue ? this.props.selTimeValue : null
      }), () => {
        if (this.props.tabkey === "7") {
          this.getDBMemoryDataAll()
        }
      })
    }
  }
  componentDidMount() {
    this.getDBMemoryDataAll()
  }
  render() {
    return (
      <div>
        <Row gutter={[10, 10]}>
          <Col className="gutter-row cpuborder" style={{ display: 'block', textAlign: 'right' }} span={24}>
            <span><img src={icon9} alt="" className='iconstyle'></img></span>
          </Col>
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
      </div>
    )
  }
}
