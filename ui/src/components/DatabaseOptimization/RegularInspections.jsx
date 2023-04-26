import React, { Component } from 'react';
import { Col, message, Row, Spin, Select, DatePicker } from 'antd';
import '../../assets/css/common.css';
import moment from 'moment';
import RegularInspectionsDay from './RegularInspectionsDay';
import RegularInspectionsWeek from './RegularInspectionsWeek';
import { formatTimestamp } from '../../utils/function';
import { getRegularInspectionsInterface } from '../../api/clusterInformation';

const { Option } = Select;
export default class RegularInspections extends Component {
  constructor(props) {
    super(props)
    this.state = {
      checkType: 'daily_check',
      typenewval: '',
      typeOptionsFilter: ['daily_check', 'weekly_check', 'monthly_check'],
      metricStatisticCount: {},
      showflag: true,
      startTime: '',
      endTime: '',
      reportVal: '',
      conclusionVal: '',
      instanceResource: '',
      activeConnections: '',
      totalConnections: '',
      tpsData: '',
      responseTime: '',
      dmlData: '',
      databaseSize: '',
      tableSize: '',
      historyAlarm: '',
      futureAlarm: '',
      distributionSlowSql: '',
      distributionRootCause: '',
      dynamicMemory: '',
      regularInspectionsDay:{},
      regularInspectionsWeek:{},
    }
  }
  changeTypeVal (value) {
    this.setState({
      checkType: value,
      showflag: true
      }, () => this.getRegularInspections(value))
  }
  async getRegularInspections (value) {
    let params = { inspection_type: value ? value : this.state.checkType }
    const { success, data, msg } = await getRegularInspectionsInterface(params)
    if (success) {
        this.setState({showflag: false})
        let sTime = formatTimestamp(data.rows[0][2])
        let eTime = formatTimestamp(data.rows[0][3])
        if(this.state.checkType === 'daily_check'){
          this.setState({
            startTime: sTime,
            endTime: eTime,
            regularInspectionsDay: data,
          })
        } else if (this.state.checkType === 'weekly_check'){
          this.setState({
            startTime: sTime,
            endTime: eTime,
            regularInspectionsWeek: data
          })
        } else {
          this.setState({
            startTime: sTime,
            endTime: eTime,
            regularInspectionsWeek: data
          })
        }
    } else {
      message.error(msg)
    }
  }
  componentDidMount () {
    this.props.onRef && this.props.onRef(this);
    this.getRegularInspections()
  }
  handleRefresh () {
      this.setState({showflag: true}, () => {
      this.getRegularInspections()
    })
  }
  componentWillUnmount = () => {
      this.setState = () => {return}
  }
  render () {
    return (
      <div className="contentWrap">
        <div style={{ textAlign: 'center' }}>
                <Row style={{ width: '60%' }} justify="space-around">
                  <Col>
                    <span>type: </span>
                    <Select value={this.state.checkType} onChange={(val) => { this.changeTypeVal(val) }}
                    style={{ width: 200 }} className="mb-20">
                      {
                        this.state.typeOptionsFilter.map((item, index) => {
                          return (
                            <Option value={item} key={index}>{item}</Option>
                          )
                        })
                      }
                    </Select>
                  </Col>
                  <Col>
                    <span>start: </span>
                    <DatePicker format="YYYY-MM-DD HH:mm:ss" showTime value={moment(this.state.startTime)} disabled style={{ width: 200 }} />
                  </Col>
                  <Col>
                    <span>end: </span>
                    <DatePicker format="YYYY-MM-DD HH:mm:ss" showTime value={moment(this.state.endTime)} disabled style={{ width: 200 }} />
                  </Col>
                </Row>
          {this.state.showflag ? <Spin style={{ margin: '260px 0 ' }} /> : 
          (this.state.checkType === 'daily_check' ? <RegularInspectionsDay regularInspectionsDay={this.state.regularInspectionsDay} /> : 
          (this.state.checkType === 'weekly_check' ? <RegularInspectionsWeek regularInspectionsWeek={this.state.regularInspectionsWeek} />:
          <RegularInspectionsWeek regularInspectionsWeek={this.state.regularInspectionsWeek} />))
          }
        </div>
      </div>
    )
  }
}