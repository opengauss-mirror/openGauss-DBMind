import React, { Component } from 'react';
import { Button, Input, message, Select, Card, Col, Row, Table, Form, DatePicker, Checkbox, Modal, InputNumber,Radio } from 'antd';
import { getItemListInterface, getUserItemListInterface } from '../../api/aiTool';
import { getIntelligentSqlCondition } from '../../api/databaseOptimization';
import ReactEcharts from 'echarts-for-react';
import moment from 'moment';
import Export from '../../assets/imgs/Export.png';
import Create from '../../assets/imgs/Create.png';
import '../../assets/css/common.css'
import '../../assets/css/main/databaseOptimization.css'
import { formatTableTitle, formatTimestamp } from '../../utils/function';
import SlowSqlDiagnosis from '../DatabaseOptimization/SlowSqlDiagnosis';

const { Option } = Select;
const { RangePicker } = DatePicker;
export default class IntelligentSqlCondition extends Component {
  constructor() {
    super()
    this.state = {
      dataSource: [],
      columns: [],
      loading: false,
      options: [],
      userDatas: [],
      optionsSource: ['pg_stat_activity','dbe_perf.statement_history','asp'],
      selValue: 'pg_stat_activity',
      isCreateVisible:false,
      routeTo:0,
      sqlText:'',
      database:'',
      formData:{},
      isSlowSqlDiagnosis:false
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
    this.setState({formData: values})
    let newData = Object.assign(values)
    let stime = newData.timePeriod ? this.timestampToTime(newData.timePeriod[0]._d) : 0
    let etime = newData.timePeriod ? this.timestampToTime(newData.timePeriod[1]._d) : 0
    let paramsVal = {
      data_source: newData.dataSource ? newData.dataSource:'',
      databases:  newData.database ? newData.database:'',
      db_users: newData.users,
      sql_types: newData.types.join(","),
      start_time: stime,
      end_time: etime,
      duration: newData.duration ? Number(newData.duration):0,
      schemas: newData.schemas ? newData.schemas:null,
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
  async getUserItemList () {
    const { success, data, msg } = await getUserItemListInterface()
    if (success) {
      this.setState({userDatas: data})
    } else {
      message.error(msg)
    }
  }
  async getIntelligentSqlAnalysis (params) {
    this.setState({ loading: true })
    const { success, msg, data } = await getIntelligentSqlCondition(params)
    if (success) {
      if (data.rows.length > 0) {
        let historyColumObj = {}
        let tableHeader = []
        let hostOptionsFilterArr = []
        data.header.forEach(item => {
          historyColumObj = {
            title: formatTableTitle(item),
            dataIndex: item,
            width: 180,
            key: item,
            ellipsis: true,
            sorter: (a, b) => {
              let aVal = a[item]
              let bVal = b[item]
              let c = isFinite(aVal),
                d = isFinite(bVal);
              return (c !== d && d - c) || (c && d ? aVal - bVal : aVal.localeCompare(bVal));
            }
          }
          tableHeader.push(historyColumObj)
        })
        let res = [],textArr = []
        data.rows.forEach((item, index) => {
          let tabledata = {}
          for (let i = 0; i < data.header.length; i++) {
            tabledata[data.header[i]] = item[i]
            tabledata['key'] = index
            if ((data.header[i] && data.header[i] === 'start_time') || (data.header[i] && data.header[i] === 'finish_time')) {
              tabledata[data.header[i]] = item[i].split(".")[0]
            }
            if ((data.header[i] && data.header[i] === 'query')) {
              textArr.push(item[i])
            }
          }
          res.push(tabledata)
        });
        this.setState(() => ({
          loading: false,
          dataSource: res,
          columns: tableHeader,
          current: this.state.current,
          sqlText:textArr.join('\n'),
          database:params.databases
        }))
      } else {
        this.setState({
          loading: false,
          dataSource: [],
          columns: [],
        })
      }
    } else {
      this.setState({
        loading: false,
        dataSource: [],
        columns: [],
      })
      message.error(msg)
    }
  }
  handleDownload () {
    const type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;charset=utf-8'
    let blob = new window.Blob([this.state.sqlText], { type: type })
    let requestUrl = window.URL.createObjectURL(blob)
    let link = document.createElement('a')
    link.style.display = 'none'
    link.href = requestUrl
    link.setAttribute('download', 'result.txt')
    document.body.appendChild(link)
    link.click()
    link.remove()
  }
  handleCreate(){
    if(this.state.dataSource.length){
      this.setState({
        isCreateVisible: true,
      })
    } else {
      message.warning('Collect data first')
    }
  }
  handleCreateCancel(){
    this.setState({
      isCreateVisible: false,
    })
  }
  getBack = (a,b,c) => {
    this.setState({
      isSlowSqlDiagnosis: false,
      isCreateVisible: false,
      selValue:c.dataSource
    },() => 
    this.FormRef.setFieldsValue(c)
  )
  }
  handleCreateOk(){
    let address = '',params = {}
    if(this.state.routeTo === 1){
      address = '/Aitoolkit/indexadvisor'
      params = { sqltext: this.state.sqlText,database: this.state.database }
    } else if(this.state.routeTo === 2){
      this.setState({
        isSlowSqlDiagnosis: true,
      })
    } else if(this.state.routeTo === 3){
      address = ''
    } else if(this.state.routeTo === 4){
      address = ''
    } else {
      message.warning('Please select an item')
      return false
    }
    this.props.history.push({ pathname: address, state: params })
  }
  handleRouteTo(flg){
    this.setState(() => ({routeTo: flg}))
  }
  componentDidMount () {
    this.getItemList();
    this.getUserItemList();
  }
  render () {
    return (
      <div className='contentWrap IntelligentSqlCondition'>
        {this.state.isSlowSqlDiagnosis ? <SlowSqlDiagnosis getBack={this.getBack} tableData={this.state.dataSource} tableHeader={this.state.columns} formData={this.state.formData} /> :
        <>
          <Card title="Query Conditions" style={{ minHeight: 400 }} className='mb-10'>
            <Form
              ref={(e) => {this.FormRef = e}}
              name="basic"
              onFinish={this.onFinish}
              onFinishFailed={this.onFinishFailed}
              autoComplete="off"
            >
              <Row justify="space-between">
                <Col span={24} className="errorinvalid">
                  <Form.Item
                    label="Data Source"
                    name="dataSource"
                    rules={[
                      {
                        required: true,
                        message: 'Please select an option!',
                      }
                    ]}
                    initialValue={this.state.selValue}
                  >
                    <Select value={this.state.selValue} onChange={(val) => { this.changeSelVal(val) }} showSearch
                      optionFilterProp="children" filterOption={(input, option) =>
                        option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 260 }}>
                      {
                        this.state.optionsSource.map(item => {
                          return (
                            <Option value={item} key={item}>{item}</Option>
                          )
                        })
                      }
                    </Select>
                  </Form.Item>
                </Col>
                {this.state.selValue !== 'pg_stat_activity' && <Col span={24} className='timeblue'>
                  <Form.Item
                    label="Time Limit"
                    name="timePeriod"
                    rules={[
                      {
                        required: true,
                        message: 'The value cannot be empty',
                      }
                    ]}
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
                </Col>}
                {this.state.selValue !== 'asp' && <Col span={24}>
                  <Form.Item
                    label="Execution Duration"
                    name="duration"
                    rules={[
                      {
                        required: true,
                        message: 'The value cannot be empty',
                      }
                    ]}
                  >
                    <InputNumber  min={0} placeholder="10ms" style={{ width: 260 }} />
                  </Form.Item>
                </Col>}
                <Col span={24} className="errorinvalid">
                  <Form.Item
                    label="Database"
                    name="database"
                    rules={[
                      {
                        required: true,
                        message: 'Please select an option!',
                      }
                    ]}
                  >
                    <Select showSearch optionFilterProp="children" filterOption={(input, option) =>
                        option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 600 }}>
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
                <Col span={24}>
                  <Form.Item
                    label="User"
                    name="users"
                    rules={[
                      {
                        required: true,
                        message: 'The value cannot be empty',
                      }
                    ]}
                  >
                    {/* <Input placeholder="user" style={{ width: 600 }} /> */}
                    <Select showSearch optionFilterProp="children" filterOption={(input, option) =>
                        option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 600 }}>
                      {
                        this.state.userDatas.map(item => {
                          return (
                            <Option value={item} key={item}>{item}</Option>
                          )
                        })
                      }
                    </Select>
                  </Form.Item>
                </Col>
                {this.state.selValue === 'dbe_perf.statement_history' && <Col span={24}>
                  <Form.Item
                    label="Schemas"
                    name="schemas"
                  >
                    <Input placeholder="schemas" style={{ width: 600 }} />
                  </Form.Item>
                </Col>}
                <Col span={24}>
                <Form.Item name="types" label="SQL Type" rules={[{ required: true,message: 'Please select at least one item'}]}>
                  <Checkbox.Group>
                  <Checkbox value="SELECT" style={{lineHeight: '32px',}} >SELECT</Checkbox>
                  <Checkbox value="INSERT" style={{lineHeight: '32px',}} >INSERT</Checkbox>
                  <Checkbox value="UPDATE" style={{lineHeight: '32px',}} >UPDATE</Checkbox>
                  <Checkbox value="DELETE" style={{lineHeight: '32px',}} >DELETE</Checkbox>
                  </Checkbox.Group>
                </Form.Item>
                </Col>
              </Row>
              <Row>
                <Col span={12} offset={2}>
                  <Form.Item>
                    <Button style={{marginLeft: 10,backgroundColor:'#5990FD',borderColor:'#5990FD'}} type="primary" size='small' htmlType="submit">
                    Inquire
                    </Button>
                  </Form.Item>
                </Col>
              </Row>
            </Form>
          </Card>
          <Card title="Log List" style={{ minHeight: 570 }} extra={<div><img src={Export} title='Export' disabled alt="" style={{marginRight:6}} onClick={() => this.handleDownload()} ></img><img src={Create} title='Analyze' alt="" onClick={() => this.handleCreate()} ></img></div>} >
            <Table size="small" bordered dataSource={this.state.dataSource} columns={this.state.columns} rowKey={record => record.key} loading={this.state.loading} scroll={{ x: '100%' }} />
          </Card>
          <Modal title="Create Analysis Tasks" width="20vw" footer={<div style={{textAlign:'center'}}><Button style={{backgroundColor:'#5990FD',borderColor:'#5990FD'}} size='small' key="submit" type="primary" onClick={() => this.handleCreateOk()}>Ensure</Button> </div>}
            destroyOnClose='true' visible={this.state.isCreateVisible} maskClosable = {false} onOk={() => this.handleCreateOk()} onCancel={() => this.handleCreateCancel()} >
            <Radio.Group style={{textAlign:'center'}}>
              <Row gutter={[0,10]}>
                <Col span={14}  offset={5}>
                <Radio.Button style={{width:'100%'}} block value={1} onClick={() => this.handleRouteTo(1)} >Index Advisor</Radio.Button>
                </Col>
                <Col span={14} offset={5}>
                <Radio.Button style={{width:'100%'}} block value={2} onClick={() => this.handleRouteTo(2)} >Slow SQL Diagnosis</Radio.Button>
                </Col>
                <Col span={14} offset={5}>
                <Radio.Button style={{width:'100%'}} block value={3} onClick={() => this.handleRouteTo(3)} disabled>SQL Rewriter</Radio.Button>
                </Col>
                <Col span={14} offset={5}>
                <Radio.Button style={{width:'100%'}} block value={4} onClick={() => this.handleRouteTo(4)} disabled>Workload forecast</Radio.Button>
                </Col>
              </Row>
            </Radio.Group>
          </Modal>
        </>
      }
      </div>
    )
  }
}
