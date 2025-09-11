import React, { Component } from 'react';
import { Button, Input, message, Select, Card, Col, Row, Table, Form, DatePicker } from 'antd';
import { getIntelligentSqlAnalysisInterface, getItemListInterface } from '../../api/aiTool';
import moment from 'moment';
import '../../assets/css/common.css'
import '../../assets/css/main/aiToolkit.css';

const { Option } = Select;
const { TextArea } = Input;
const { RangePicker } = DatePicker;

export default class IntelligentSqlAnalysis extends Component {
  constructor() {
    super()
    this.state = {
      dataSource: [],
      columns: [
        {
          title: 'root cause',
          dataIndex: 'root_cause',
          key: 'root_cause',
        },
        {
          title: 'suggestion',
          dataIndex: 'suggestion',
          key: 'suggestion',
        }
      ],
      loading: false,
      options: [],
      selValue: '',
    }
  }
  timestampToTime (timestamp) {
    const date = new Date(timestamp);
    const Y = date.getFullYear() + '-';
    const M = (date.getMonth() + 1 < 10 ? '0' + (date.getMonth() + 1) : date.getMonth() + 1) + '-';
    const D = date.getDate() + ' ';
    const h = (date.getHours() < 10 ? '0' + (date.getHours()) : date.getHours()) + ':';
    const m = (date.getMinutes() < 10 ? '0' + (date.getMinutes()) : date.getMinutes()) + ':';
    const s = (date.getSeconds() < 10 ? '0' + (date.getSeconds()) : date.getSeconds());
    const standardTime = new Date(Y + M + D + h + m + s)
    const finaTime = Date.parse(standardTime)
    return finaTime;
  }
  onFinish = (values) => {
    const newData = Object.assign(values)
    const stime = newData.timePeriod ? this.timestampToTime(newData.timePeriod[0]._d) : 0
    const etime = newData.timePeriod ? this.timestampToTime(newData.timePeriod[1]._d) : 0
    const schemaname = values.schemaname ? values.schemaname : 'public'
    const paramsVal = {
      query: newData.sql,
      db_name: newData.database,
      schemaname: schemaname,
      start_time: stime,
      end_time: etime,
    }
    this.getIntelligentSqlAnalysis(paramsVal)
  }
  onFinishFailed = () => {};
  changeSelVal (value) {
    this.setState({selValue: value})
  }
  async getItemList () {
    const { success, data, msg } = await getItemListInterface()
    if (success) {
      const defaultVal = this.state.selValue || (Array.isArray(data) && data.length > 0 ? data[0] : '')
      this.setState({ options: data, selValue: defaultVal }, () => {
        if (defaultVal && this.FormRef && this.FormRef.setFieldsValue) {
          this.FormRef.setFieldsValue({ database: defaultVal })
        }
      })
    } else {
      message.error(msg)
    }
  }
  async getIntelligentSqlAnalysis (params) {
    this.setState({ loading: true })
    const { success, msg, data } = await getIntelligentSqlAnalysisInterface(params)
    if (success) {
      const res = []
      data[1][0][0].forEach((it, idx) => {
        const obj = {
          key: idx,
          root_cause: data[1][0][0][idx],
          suggestion: data[1][1][0][idx]
        }
        res.push(obj)
      })
      this.setState({
        loading: false,
        dataSource: res
      })
    } else {
      this.setState({loading: false}, () => {
        message.error(msg)
      })
    }
  }
  componentDidMount () {
    this.getItemList()
  }
  render () {
    return (
      <div className='contentWrap intelligent'>
        <Card title="Intelligent SQL Analysis" style={{ minHeight: 800 }}>
          <Form
            ref={(e) => { this.FormRef = e }}
            name="basic"
            initialValues={{
              schemaname: this.state.schemaname
            }}
            onFinish={this.onFinish}
            onFinishFailed={this.onFinishFailed}
            autoComplete="off"
          >
            <Row>
              <Col span={24} className="errorinvalid">
                <Form.Item
                  label="Database List"
                  name="database"
                  rules={[
                    {
                      required: true,
                      message: 'Please select an option!',
                    }
                  ]}
                >
                  <Select value={this.state.selValue} onChange={(val) => { this.changeSelVal(val) }} showSearch
                    optionFilterProp="children" filterOption={(input, option) =>
                      option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 260 }}>
                    {
                      this.state.options.map(item => {
                        return (
                          <Option value={item} key={item}>{item}</Option>
                        )
                      })
                    }
                  </Select>
                </Form.Item>
              </Col>
              <Col span={6}>
                <Form.Item
                  label="Schema"
                  name="schemaname"
                >
                  <Input placeholder="public" style={{ width: 260 }} />
                </Form.Item>
              </Col>
              <Col span={7}>
                <Form.Item
                  label=""
                  name="timePeriod"
                >
                  <RangePicker 
                  style={{ width: 420 }}
                  placement='topRight'
                  format="YYYY-MM-DD HH:mm:ss"
                  showTime={{
                    defaultValue: moment('00:00:00', 'HH:mm:ss'),
                  }}
                  />
                </Form.Item>
              </Col>
            </Row>
            <Row>
              <Col span={24}>
                <Form.Item
                  label="SQL Statements"
                  name="sql"
                  rules={[
                    {
                      required: true,
                      message: 'Please input SQL!',
                    },
                  ]}
                >
                  <TextArea rows={8} placeholder={`# Please type the slow SQL statement as the following, then the Intelligent SQL Analysis will return the root causes.
# SELECT * FROM t1 WHERE t1.id > 100`} />
                </Form.Item>
              </Col>
            </Row>
            <Row>
              <Col span={12} offset={2}>
                <Form.Item>
                  <Button type="primary" htmlType="submit">
                    Analysis
                  </Button>
                </Form.Item>
              </Col>
            </Row>
          </Form>
          <Table size='small' bordered dataSource={this.state.dataSource} columns={this.state.columns} rowKey={record => record.key} loading={this.state.loading} />
        </Card>
      </div>
    )
  }
}
