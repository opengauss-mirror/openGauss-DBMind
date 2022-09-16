import React, { Component } from 'react';
import { Button, Card, Checkbox, Col, DatePicker, Form, Input, InputNumber, message, Row, Table } from 'antd';
import moment from 'moment';
import { ReloadOutlined } from '@ant-design/icons';
import ResizeableTitle from '../../common/ResizeableTitle';
import { getRecentSlowQueryInterface } from '../../../api/autonormousMangemant';
import { formatTableTitle, formatTimestamp } from '../../../utils/function';

const { RangePicker } = DatePicker;
export default class SlowTopQuery extends Component {
  constructor() {
    super()
    this.state = {
      dataSource: [],
      columns: [],
      pagination: {
        total: 0,
        defaultCurrent: 1
      },
      recentSearchForm: {
        query: '',
        end: '',
        start: '',
        group: true
      },
      loadingRecent: false,
      startTime: '',
      endTime: '',
      checkedGroup: true,
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  onRecentFinish = (values) => {
    let newData = Object.assign(values, {
      group: this.state.checkedGroup,
    })
    let paramsVal = {
      start: this.state.startTime,
      end: this.state.endTime,
      query: encodeURIComponent(newData.query),
      limit: newData.limit,
      group: newData.group
    }
    for (let item in paramsVal) {
      if (paramsVal[item] === '') {
        paramsVal[item] = null
      }
    }
    this.getRecentSlowQuery(paramsVal)
  }
  async getRecentSlowQuery (params) {
    let paramData = {
      start: params.start ? params.start : null,
      end: params.end ? params.end : null,
      query: params.query ? params.query : null,
      limit: params.limit ? params.limit : null,
      group: params.group
    }
    this.setState({ loadingRecent: true })
    const { success, data, msg } = await getRecentSlowQueryInterface(paramData)
    if (success) {
      if (data.header.length > 0) {
        let historyColumObj = {}
        let tableHeader = []
        data.header.forEach((item) => {
          historyColumObj = {
            title: formatTableTitle(item),
            dataIndex: item,
            key: item,
            width: 180,
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
        let res = []
        data.rows.forEach((item, index) => {
          let tabledata = {}
          for (let i = 0; i < data.header.length; i++) {
            tabledata[data.header[i]] = item[i]
            tabledata['key'] = index
            if ((data.header[i] && data.header[i] === 'start_at') || (data.header[i] && data.header[i] === 'end_at')) {
              tabledata[data.header[i]] = formatTimestamp(item[i])
            }
          }
          res.unshift(tabledata)
        });
        this.setState(() => ({
          loadingRecent: false,
          dataSource: res,
          columns: tableHeader,
          pagination: {
            total: res.length,
            defaultCurrent: 1
          }
        }))
      } else {
        this.setState({
          loadingRecent: false,
          dataSource: [],
          columns: [],
        })
      }
    } else {
      this.setState({
        loadingRecent: false,
        dataSource: [],
        columns: [],
      })
      message.error(msg)
    }
  }
  // 时间框
  onChangeTimes = (dates, dateStrings) => {
    this.setState(() => ({
      startTime: new Date(dateStrings[0]).getTime(),
      endTime: new Date(dateStrings[1]).getTime()
    }))
  }
  onChangeCheckbox (e) {
    this.setState({checkedGroup: e.target.checked})
  }
  handleResize = index => (e, { size }) => {
    this.setState(({ columns }) => {
      const nextColumns = [...columns];
      nextColumns[index] = {
        ...nextColumns[index],
        width: size.width,
      };
      return { columns: nextColumns };
    });
  }
  handleRefresh(){
    this.setState({checkedGroup: true,},()=>{
      this.recentFormRef.resetFields()
      this.getRecentSlowQuery(this.state.recentSearchForm)
    }) 
  }
  componentDidMount () {
    this.getRecentSlowQuery(this.state.recentSearchForm)
  }
  render () {
    const columns = this.state.columns.map((col, index) => ({
      ...col,
      onHeaderCell: column => ({
        width: column.width,
        onResize: this.handleResize(index)
      })
    }))
    return (
      <div>
        <Card title="Recent Slow Query" extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />} className="mb-20">
          <Row style={{ marginBottom: 20 }}>
            <Col span={24}>
              <Form
                layout="inline"
                initialValues={{ ...this.state.recentSearchForm }}
                onFinish={this.onRecentFinish}
                ref={(e) => {
                  this.recentFormRef = e
                }}
              >
                <Form.Item name="query" label="query">
                  <Input placeholder="Input query" allowClear />
                </Form.Item>
                <Form.Item name="time" label="time">
                  <RangePicker
                    ranges={{
                      Today: [moment(), moment()],
                      'This Month': [moment().startOf('month'), moment().endOf('month')],
                    }}
                    showTime
                    style={{ width: 400 }}
                    format="YYYY/MM/DD HH:mm:ss"
                    onChange={this.onChangeTimes}
                  />
                </Form.Item>
                <Form.Item name="limit" label="limit">
                  <InputNumber min={1} />
                </Form.Item>
                <Form.Item name="group" label="group">
                  <Checkbox checked={this.state.checkedGroup} onChange={(e) => { this.onChangeCheckbox(e) }}></Checkbox>
                </Form.Item>
                <Form.Item >
                  <Button type="primary" htmlType="submit">Search</Button>
                </Form.Item>
              </Form>
            </Col>
          </Row>
          <Table bordered showSorterTooltip={false} components={this.components} columns={columns} dataSource={this.state.dataSource} rowKey={record => record.key} pagination={this.state.pagination} loading={this.state.loadingRecent} scroll={{ x: '100%' }} />
        </Card>
      </div>
    )
  }
}
