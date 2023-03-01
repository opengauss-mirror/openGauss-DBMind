import React, { Component } from 'react';
import { Button, Input, message, Select, Card, Col, Row, Table, Form, DatePicker } from 'antd';
import { getIntelligentSqlAnalysisInterface } from '../../api/aiTool';
import { getItemListInterface } from '../../api/aiTool';
import moment from 'moment';

const { Option } = Select;
const { TextArea } = Input;
export default class Sqlanalysis extends Component {
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
    let date = new Date(timestamp);
    let Y = date.getFullYear() + '-';
    let M = (date.getMonth() + 1 < 10 ? '0' + (date.getMonth() + 1) : date.getMonth() + 1) + '-';
    let D = date.getDate() + ' ';
    let h = (date.getHours() < 10 ? '0' + (date.getHours()) : date.getHours()) + ':';
    let m = (date.getMinutes() < 10 ? '0' + (date.getMinutes()) : date.getMinutes()) + ':';
    let s = (date.getSeconds() < 10 ? '0' + (date.getSeconds()) : date.getSeconds());
    let standardTime = new Date(Y + M + D + h + m + s)
    let finaTime = Date.parse(standardTime)
    return finaTime;
  }
  onFinish = (values) => {
    let newData = Object.assign(values)
    let stime = newData.starttime ? this.timestampToTime(newData.starttime._d) : ''
    let etime = newData.endtime ? this.timestampToTime(newData.endtime._d) : ''
    let schemaname = values.schemaname ? values.schemaname : 'public'
    let paramsVal = {
      sql: newData.sql,
      database: newData.database,
      schemaname: schemaname,
      start_time: stime,
      end_time: etime,
      wdr: '',
    }
    this.getIntelligentSqlAnalysis(paramsVal)
  }
  onFinishFailed = (errorInfo) => {
    console.log('Failed:', errorInfo);
  };
  changeSelVal (value) {
    this.setState({selValue: value})
  }
  async getItemList () {
    const { success, data, msg } = await getItemListInterface()
    if (success) {
      this.setState({options: data})
    } else {
      message.error(msg)
    }
  }
  async getIntelligentSqlAnalysis (params) {
    this.setState({ loading: true })
    const { success, msg, data } = await getIntelligentSqlAnalysisInterface(params)
    if (success) {
      let res = []
      data[0][0].forEach((it, idx) => {
        let obj = {
          key: idx,
          root_cause: data[0][0][idx],
          suggestion: data[1][0][idx]
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
      <div>
        <Card title="Intelligent SQL Analysis" style={{ minHeight: 800 }}>
          <Form
            name="basic"
            initialValues={{
              schemaname: this.state.schemaname
            }}
            onFinish={this.onFinish}
            onFinishFailed={this.onFinishFailed}
            autoComplete="off"
          >
            <Row justify="space-between">
              <Col span={6} className="errorinvalid">
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
              <Col span={6}>
                <Form.Item
                  label="Start Time"
                  name="starttime"
                >
                  <DatePicker
                    style={{ width: 260 }}
                    format="YYYY-MM-DD HH:mm:ss"
                    showTime={{
                      defaultValue: moment('00:00:00', 'HH:mm:ss'),
                    }}
                  />
                </Form.Item>
              </Col>
              <Col span={6}>
                <Form.Item
                  label="End Time"
                  name="endtime"
                >
                  <DatePicker
                    style={{ width: 260 }}
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
          <Table bordered dataSource={this.state.dataSource} columns={this.state.columns} rowKey={record => record.key} loading={this.state.loading} />
        </Card>
      </div>
    )
  }
}
