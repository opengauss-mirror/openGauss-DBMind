import React, { Component } from 'react';
import { Button, Input, message, Select, Card, Col, Row, Table, Form, DatePicker, Modal, InputNumber } from 'antd';
import { SettingFilled, InfoCircleFilled } from '@ant-design/icons';
import { getIntelligentSqlAnalysisInterface, getItemListInterface } from '../../api/aiTool';
import { getSettingDefaults, getSettingCurrentValue, updateSetting } from "../../api/dbmindSettings";
import moment from 'moment';
import '../../assets/css/common.css'
import '../../assets/css/main/aiToolkit.css';

const { Option } = Select;
const { TextArea } = Input;
const { RangePicker } = DatePicker;
const labelStyle = {width:194,float:'left',textAlign:'right',lineHeight:'32px'}
const inputStyle = {marginLeft:20,marginRight:20}
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
      isSettingVisible:false,
      nestloop_rows_threshold: '',
      plan_height_threshold: '',
      complex_operator_threshold: '',
      large_in_list_threshold: '',
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
    let stime = newData.timePeriod ? this.timestampToTime(newData.timePeriod[0]._d) : 0
    let etime = newData.timePeriod ? this.timestampToTime(newData.timePeriod[1]._d) : 0
    let schemaname = values.schemaname ? values.schemaname : 'public'
    let paramsVal = {
      query: newData.sql,
      db_name: newData.database,
      schemaname: schemaname,
      start_time: stime,
      end_time: etime,
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
      data[1][0][0].forEach((it, idx) => {
        let obj = {
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
  async handleSetting(){
    const { success,data, msg } = await getSettingCurrentValue({configname: "slow_sql_threshold"});
    if (success) {
      this.setState({
        nestloop_rows_threshold: data.nestloop_rows_threshold[0],
        plan_height_threshold: data.plan_height_threshold[0],
        complex_operator_threshold: data.complex_operator_threshold[0],
        large_in_list_threshold: data.large_in_list_threshold[0],
        isSettingVisible: true,
      })
    } else {
      message.error(msg);
    }
  }
  async handleSettingOk(){
    if(this.state.nestloop_rows_threshold && this.state.plan_height_threshold && this.state.complex_operator_threshold && this.state.large_in_list_threshold){
      let params = {
        configname:"slow_sql_threshold",
        config_dict:{nestloop_rows_threshold:this.state.nestloop_rows_threshold,plan_height_threshold:this.state.plan_height_threshold,
          complex_operator_threshold:this.state.complex_operator_threshold,large_in_list_threshold:this.state.large_in_list_threshold}
      };
      const { success, msg } = await updateSetting(params);
      if (success) {
        this.setState({
          isSettingVisible: false
        });
        message.success("SAVE SUCCESS");
      } else {
        message.error(msg);
      }
    } else {
      message.warning('The input value is a positive integer greater than 0')
    }
  }
  async handleSettingReset(){
    const { success,data, msg } = await getSettingDefaults({configname: "slow_sql_threshold"});
    if (success) {
      this.setState({
        nestloop_rows_threshold: data.nestloop_rows_threshold[0].toString(),
        plan_height_threshold: data.plan_height_threshold[0].toString(),
        complex_operator_threshold: data.complex_operator_threshold[0].toString(),
        large_in_list_threshold: data.large_in_list_threshold[0].toString(),
      })
    } else {
      message.error(msg);
    }
  }
  handleSettingCancel(){
    this.setState({
      isSettingVisible: false,
    })
  }
  handleChange = (e,flg) => {
    if(e){
      if(flg === 1){
        this.setState({nestloop_rows_threshold: e})
      } else if(flg === 2){
        this.setState({plan_height_threshold: e})
      } else if(flg === 3){
        this.setState({complex_operator_threshold: e})
      } else {
        this.setState({large_in_list_threshold: e})
      }
    } else {
      message.warning('The input value is a positive integer greater than 0')
    }
  }
  componentDidMount () {
    this.getItemList()
  }
  render () {
    return (
      <div className='contentWrap intelligent'>
        <Card title="Intelligent SQL Analysis" extra={<SettingFilled className="more_link" onClick={() => { this.handleSetting() }} />} style={{ minHeight: 800 }}>
          <Form
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
        <Modal title="Setting"  style={{minWidth:840}} footer={<div style={{textAlign:'center'}}><Button key="submit" type="primary" onClick={() => this.handleSettingOk()}>Save</Button><Button key="back" onClick={() => this.handleSettingReset()}>Reset</Button></div>}
         destroyOnClose='true' visible={this.state.isSettingVisible} maskClosable = {false} onOk={() => this.handleSettingOk()}  onCancel={() => this.handleSettingCancel()}>
          <p style={{minWidth:780}}><label style={labelStyle}>Nestloop_rows_threshold: </label><InputNumber style={inputStyle} min={0} onChange={(e) => this.handleChange(e,1)} stringMode value={this.state.nestloop_rows_threshold} /><label style={{color:'#ADA6ED'}}><InfoCircleFilled /> The maximum number of tuples suitable for the nested loop operator</label></p>
          <p style={{minWidth:780}}><label style={labelStyle}>Plan_height_threshold: </label><InputNumber style={inputStyle} min={1} onChange={(e) => this.handleChange(e,2)} stringMode value={this.state.plan_height_threshold} /><label style={{color:'#ADA6ED'}}><InfoCircleFilled /> The threshold of execution plan</label></p>
          <p style={{minWidth:780}}><label style={labelStyle}>Complex_operator_threshold: </label><InputNumber style={inputStyle} min={1} onChange={(e) => this.handleChange(e,3)} stringMode value={this.state.complex_operator_threshold} /><label style={{color:'#ADA6ED'}}><InfoCircleFilled /> The thresold of complex operator, now it refers to the join operator</label></p>
          <p style={{minWidth:780}}><label style={labelStyle}>Large_in_list_threshold: </label><InputNumber style={inputStyle} min={1} onChange={(e) => this.handleChange(e,4)} stringMode value={this.state.large_in_list_threshold} /><label style={{color:'#ADA6ED'}}><InfoCircleFilled /> The threshold of the number of elements in the in-clause</label></p>
        </Modal>
      </div>
    )
  }
}
