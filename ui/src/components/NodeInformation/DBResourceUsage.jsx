import React, { Component } from 'react';
import { Col, Row, message } from 'antd';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import { commonMetricMethod } from '../../utils/function';

const metricData = ['gaussdb_cpu_time', 'pg_summary_file_iostat_total_phyblkrd', 'pg_summary_file_iostat_total_phyblkwrt']
export default class DBResourceUsage extends Component {
  constructor(props) {
    super(props)
    this.state = {
      chartData1: {},
      chartData2: {},
      param: {
        instance: this.props.selValue,
        latest_minutes: this.props.selTimeValue ? this.props.selTimeValue : null,
        fetch_all: false,
        regex: false,
        from_timestamp: this.props.startTime ? this.props.startTime : null,
        to_timestamp: this.props.endTime ? this.props.endTime : null
      }
    }
  }
  async getCpuDataAll() {
    Promise.all([
      commonMetricMethod(this.state.param, { label: metricData[0] }),
      commonMetricMethod(this.state.param, { label: metricData[1] }),
      commonMetricMethod(this.state.param, { label: metricData[2] })
    ]).then((result) => {
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
        let data1 = { 'legend': [{ image: '', description: 'Cpu Time' }], 'xAxisData': xDataArray[0], 'seriesData': [{ data: yDataArray[0], description: 'Cpu Time', colors: '#EB6E19' }], 'flg': 0, 'legendFlg': 2, title: 'Cpu Time', 'unit': '', 'fixedflg': 0, 'toolBox': true }
        let data2 = { 'legend': [{ image: '', description: 'Phyblkrd' }, { image: '', description: 'Phyblkwrt' }], 'xAxisData': xDataArray[1], 'seriesData': [{ data: yDataArray[1], description: 'Phyblkrd', colors: '#9184F0' }, { data: yDataArray[2], description: 'Phyblkwrt', colors: '#5990FD' }], 'flg': 0, 'legendFlg': 2, title: 'Phyblkrd/Phyblkwrt', 'unit': '', 'fixedflg': 0, 'toolBox': true }
        this.setState({
          chartData1: data1,
          chartData2: data2,
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
        if (this.props.tabkey === "4") {
          this.getCpuDataAll()
        }
      })
    }
  }
  componentDidMount() {
    this.getCpuDataAll()
  }
  render() {
    return (
      <div>
        <Row gutter={[10, 10]}>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork echartData={this.state.chartData1} />
          </Col>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork echartData={this.state.chartData2} />
          </Col>
        </Row>
      </div>
    )
  }
}
