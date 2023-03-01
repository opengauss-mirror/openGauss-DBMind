import React, { Component } from 'react';
import { Button, Card, Checkbox, Col, Input, message, Row, Select } from 'antd';
import { DownloadOutlined } from '@ant-design/icons';
import { getItemListInterface, getQueryTuningInterface } from '../../api/aiTool';

const { TextArea } = Input;
const { Option } = Select;
export default class QueryTuning extends Component {
  constructor() {
    super()
    this.state = {
      sqlstatement_treaVal: '',
      result_treaVal: '',
      selValue: '',
      use_rewrite: true,
      use_hinter: false,
      use_materialized: false,
      checkedValues: [],
      options: []
    }
  }
  handleChange1 = (event) => {
    this.setState({ sqlstatement_treaVal: event.target.value });
  }
  handleTune = () => {
    if (this.state.selValue === '') {
      message.warning('Please choose a database. ')
    } else if (this.state.sqlstatement_treaVal === '') {
      message.warning('please enter the content of textarea. ')
    } else {
      this.getQueryTuning()
    }  
  }
  changeVal (e) {
    this.setState({[e.target.name]: e.target.value})
  }
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
  async getQueryTuning () {
    let params = {
      database: this.state.selValue,
      sql: encodeURIComponent(this.state.sqlstatement_treaVal),
      use_rewrite: this.state.use_rewrite,
      use_hinter: this.state.use_hinter,
      use_materialized: this.state.use_materialized
    }
    const { success, data, msg } = await getQueryTuningInterface(params)
    if (success) {
      this.setState({result_treaVal: data })
    } else {
      message.error(msg)
    }
  }
  handleDownload () {
    const type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;charset=utf-8'
    let blob = new window.Blob([this.state.result_treaVal], { type: type })
    let requestUrl = window.URL.createObjectURL(blob)
    let link = document.createElement('a')
    link.style.display = 'none'
    link.href = requestUrl
    link.setAttribute('download', 'result.txt')
    document.body.appendChild(link)
    link.click()
    link.remove()
  }
  componentDidMount () {
    this.getItemList()
  }
  render () {
    return (
      <div>
        <Card title="Query Tuning" bordered={false} style={{ width: '100%', minHeight: 800, position: 'relative' }}>
          <Row style={{ width: '100%', marginBottom: 20 }} >
              <Col span={3}>
                <span>Tuning Methods:</span>
              </Col>
              <Col span={6}>
                <Checkbox defaultChecked={this.state.use_rewrite} disabled>Use Rewrite</Checkbox>
              </Col>
              <Col span={6}>
                <Checkbox defaultChecked={this.state.use_hinter} disabled>Use Hinter</Checkbox>
              </Col>
              <Col span={6}>
                <Checkbox defaultChecked={this.state.use_materialized} disabled>Use Materialized</Checkbox>
              </Col>
          </Row>
          <div className="btngroup">
            <div className="flexbox">
              <div className="flextitle">Database List：</div>
              <Select value={this.state.selValue} onChange={(val) => { this.changeSelVal(val) }} showSearch
                optionFilterProp="children" filterOption={(input, option) =>
                  option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 180, marginRight: 20 }} className="mb-20">
                {
                  this.state.options.map(item => {
                    return (
                      <Option value={item} key={item}>{item}</Option>
                    )
                  })
                }
              </Select>
              <Button type="primary" className="mb-20" onClick={this.handleTune} style={{ marginRight: 40 }}>Tune</Button>
            </div>
          </div>
          <TextArea className="mb-20" rows={10} placeholder="# Please type the SQL statement you want to rewrite here." value={this.state.sqlstatement_treaVal} onChange={this.handleChange1} />
          <TextArea className="mb-20" rows={10} disabled value={this.state.result_treaVal} />
          <Button type="primary" icon={<DownloadOutlined />} size="small" style={{ position: 'absolute', bottom: 170, right: 50 }} onClick={() => { this.handleDownload() }} />
        </Card>
      </div >
    )
  }
}
