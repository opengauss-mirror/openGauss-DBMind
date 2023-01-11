import React from 'react';
import { ReloadOutlined } from '@ant-design/icons';
import { Card, Row, Col, Input, message, Select, DatePicker, Button, Spin } from 'antd';
import moment from 'moment';
import { getRegularInspectionsInterface } from '../../../api/clusterInformation';
import { formatTimestamp } from '../../../utils/function';
const { Option } = Select;
const { TextArea } = Input;
export default class RegularInspection extends React.PureComponent {
  constructor() {
    super()
    this.state = {
      checkType: 'daily check',
      typenewval: '',
      typeOptionsFilter: ['daily check', 'weekly check', 'monthly check'],
      startTime: '',
      endTime: '',
      reportVal: '',
      conclusionVal: '',
      showFlag: 0,
    }
  }
  changeTypeVal (value) {
    this.setState({checkType: value})
  }
  async getRegularInspections () {
    this.setState({ showFlag: 1 })
    let params = { inspection_type: this.state.checkType }
    const { success, data, msg } = await getRegularInspectionsInterface(params)
    if (success) {
      if (data.rows.length > 0) {
        let obj = {}
        data.header.forEach((item, index) => {
          obj[item] = data.rows[0][index]
        })
        let sTime = formatTimestamp(obj.start)
        let eTime = formatTimestamp(obj.end)
        this.setState({
          showFlag: 0,
          startTime: sTime,
          endTime: eTime,
          reportVal: obj.report,
          conclusionVal: obj.conclusion
        })
      } else {
        this.setState({
          showFlag: 0,
          startTime: '',
          endTime: '',
          reportVal: '',
          conclusionVal: ''
        })
      }
    } else {
      message.error(msg)
    }
  }
  componentDidMount () {
    this.getRegularInspections()
  }
  render () {
    return (
      <div className="mb-20">
        <Card style={{ height: 1000 }} title="Regular Inspections" extra={<ReloadOutlined className="more_link" onClick={() => { this.getRegularInspections() }} />} >
          {
            this.state.showFlag === 0 ?
              <>
                <Row style={{ marginBottom: 20, width: '60%' }} justify="space-around">
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
                  <Col>
                    <Button type="primary" onClick={() => this.getRegularInspections()}>Search</Button>
                  </Col>
                </Row>
                <Row style={{ marginBottom: 20 }} justify="space-around">
                  <Col span={1}><span>report: </span></Col>
                  <Col span={22}>
                    <TextArea rows={22} value={this.state.reportVal} style={{ display: 'inline-block' }} />
                  </Col>
                </Row>
                <Row style={{ marginBottom: 20 }} justify="space-around">
                  <Col span={1}><span>conclusion: </span></Col>
                  <Col span={22}>
                    <TextArea rows={12} value={this.state.conclusionVal} style={{ display: 'inline-block' }} />
                  </Col>
                </Row>
              </> : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '300px auto' }} /> </div>}
        </Card>
      </div>
    )
  }
}
