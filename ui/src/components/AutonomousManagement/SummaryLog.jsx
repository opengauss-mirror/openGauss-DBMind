import React, { Component } from 'react';
import { Card, Input, message, Spin } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { getLogSummaryTnterface } from '../../api/autonormousMangemant';

const { TextArea } = Input;

export default class LogInformation extends Component {
  constructor() {
    super()
    this.state = {
      summaryLogTextareaVal: '',
      loadingFlag: 1
    }
  }
  async getLogSummary () {
    this.setState({loadingFlag: 1 })
    const { success, data, msg } = await getLogSummaryTnterface()
    if (success) {
      this.setState({
        summaryLogTextareaVal: data,
        loadingFlag: 2
      })
    } else {
      this.setState({loadingFlag: 2})
      message.error(msg)
    }
  }
  componentDidMount () {
    this.getLogSummary()
  }
  render () {
    return (
      <div >
        <Card title="Summary Log" style={{ height: 800 }} extra={<ReloadOutlined className="more_link" onClick={() => { this.getLogSummary() }} />}>
          {this.state.loadingFlag === 2 ? <TextArea rows={26} value={this.state.summaryLogTextareaVal} /> : <div style={{ width: '100%', textAlign: 'center' }}> <Spin style={{ margin: '200px 0', }} /></div>}
        </Card>
      </div>
    )
  }
}
