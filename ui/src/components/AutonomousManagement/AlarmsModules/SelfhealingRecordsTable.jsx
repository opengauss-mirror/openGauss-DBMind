import React, { Component } from 'react';
import { Card, Row, Col, DatePicker, Table, Button, message, Select } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import ResizeableTitle from '../../common/ResizeableTitle';
import { formatTableTime, formatTableTitle, formatTimestamp } from '../../../utils/function';
import { getSelfHealingRecordsInterface } from '../../../api/autonormousMangemant';

const { Option } = Select;
export default class SelfhealingRecordsTable extends Component {
  constructor() {
    super()
    this.state = {
      dataSource: [],
      columns: [],
      pagination: {
        total: 0,
        defaultCurrent: 1
      },
      loading: false,
      actionOptionsFilter: [],
      successOptionsFilter: [{
        key: 'true',
        value: 'true'
      }, {
        key: 'false',
        value: 'false'
      }],
      action: '',
      actionnewVal: '',
      success: '',
      min_occurrence: '',
      timekey:''
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  handleDataDeduplicate = (value) => {
    let newArr = []
    for (let i = 0; i < value.length; i++) {
      if (newArr.indexOf(value[i]) === -1 && value[i]) {
        newArr.push(value[i])
      }
    }
    return newArr
  }
  async getSelfHealingRecordsData () {
    let params = {
      action: this.state.action === '' ? null : this.state.action,
      success: this.state.success === '' ? null : this.state.success,
      min_occurrence: this.state.min_occurrence === '' ? null : this.state.min_occurrence,
    }
    this.setState({ loading: true });
    const { success, data, msg } = await getSelfHealingRecordsInterface(params)
    if (success) {
      if (data.header.length > 0) {
        let historyColumObj = {}
        let tableHeader = []
        let actionOptionsFilterArr = []
        data.header.forEach((item) => {
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
        let res = []
        data.rows.forEach((item, index) => {
          let tabledata = {}
          for (let i = 0; i < data.header.length; i++) {
            tabledata[data.header[i]] = item[i] + ''
            tabledata['key'] = index
            if (data.header[i] && data.header[i] === 'occurrence_at') {
              tabledata[data.header[i]] = formatTimestamp(item[i] + '')
            }
          }
          res.push(tabledata)
        });
        formatTableTime(res)
        res.forEach((item) => {
          actionOptionsFilterArr.push(item.action)
        })
        let actionTypeOptions = this.handleDataDeduplicate(actionOptionsFilterArr)
        this.setState(() => ({
          actionOptionsFilter: actionTypeOptions,
          loading: false,
          dataSource: res,
          columns: tableHeader,
          pagination: {
            total: res.length,
            defaultCurrent: 1
          }
        }))
      } else {
        this.setState({
          loading: false,
          dataSource: [],
          columns: [],
        })
      }
    } else {
      this.setState({loading: false})
      message.error(msg)
    }
  }
  onSearch = (value) => {
    if (value) {
      this.setState({
        action: value,
        actionnewVal: value
      })
    }
  };
  onBlurSelect = () => {
    const value = this.state.actionnewVal
    if (value) {
      this.changeSelActionVal(value)
      this.setState({
        actionnewVal: ''
      })
    }
  }
  changeSelActionVal = (e) => {
    this.setState({
      actionnewVal: e,
      action: e
    })
  }
  changeSelSuccessVal (value) {
    this.setState({success: value})
  }
  onChangeData = (date, dateString) => {
    if (dateString) {
      this.setState({min_occurrence: new Date(dateString).getTime()})
    } else {
      this.setState({ min_occurrence: ''})
    }
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
  };
  handleSearch () {
    this.getSelfHealingRecordsData()
  }
  handleRefresh(){
    this.setState({
      action: '',
      success: '',
      min_occurrence: '',
      timekey:new Date(),
      group: true,
    },()=>{
      this.getSelfHealingRecordsData()
    })
  }
  componentDidMount () {
    this.getSelfHealingRecordsData()
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
        <Card title="Self-healing Records" extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />} className="mb-20">
          <Row style={{ marginBottom: 20 }}>
            <Col span={4}>
              <span>action: </span>
              <Select value={this.state.action} name="action" onChange={(ev) => { this.changeSelActionVal(ev) }} onSearch={(e) => { this.onSearch(e) }} onBlur={() => this.onBlurSelect()} showSearch allowClear
                optionFilterProp="children" filterOption={(input, option) =>
                  option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 180 }} className="mb-20">
                {
                  this.state.actionOptionsFilter.map((item, index) => {
                    return (
                      <Option value={item} key={index}>{item}</Option>
                    )
                  })
                }
              </Select>
            </Col>
            <Col span={4}>
              <span>success: </span>
              <Select value={this.state.success} name="success" onChange={(val) => { this.changeSelSuccessVal(val) }} allowClear
                style={{ width: 180 }} className="mb-20">
                {
                  this.state.successOptionsFilter.map((item) => {
                    return (
                      <Option value={item.key} key={item.key}>{item.value}</Option>
                    )
                  })
                }
              </Select>
            </Col>
            <Col span={5}>
              <span>min occurrence: </span>
              <DatePicker key={this.state.timekey} showTime onChange={(date, dateString) => this.onChangeData(date, dateString)} />
            </Col>
            <Col span={4}>
              <Button type="primary" onClick={() => this.handleSearch()}>Search</Button>
            </Col>
          </Row>
          <Table bordered showSorterTooltip={false} components={this.components} columns={columns} dataSource={this.state.dataSource} rowKey={record => record.key} pagination={this.state.pagination} loading={this.state.loading} scroll={{ x: '100%' }} />
        </Card>
      </div>
    )
  }
}
