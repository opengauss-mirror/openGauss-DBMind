import React, { Component } from 'react';
import { Button, Card, Checkbox, Col, DatePicker, message, Row, Select, Table } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import ResizeableTitle from '../../common/ResizeableTitle';
import { getFutureAlarmsInterface, getSearchMetricInterface } from '../../../api/autonormousMangemant';
import { formatTableTitle, formatTimestamp } from '../../../utils/function';

const { Option } = Select;
export default class Alarms extends Component {
  constructor() {
    super()
    this.state = {
      futureTableSource: [],
      columns: [],
      futurePagination: {
        total: 0,
        defaultCurrent: 1
      },
      metric_name: '',
      metricnewname: '',
      instance: '',
      instancenewname: '',
      start: '',
      group: true,
      options: [],
      loadingFuture: false,
      instanceOptionsFilter: [],
      futerInstanceOptionFilter: [],
      timekey:''
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  // 下拉框数据
  async getSearchMetric () {
    const { success, data, msg } = await getSearchMetricInterface()
    if (success) {
      this.setState({options: data})
    } else {
      message.error(msg)
    }
  }
  async getFutureAlarms () {
    let params = {
      metric_name: this.state.metric_name === '' ? null : this.state.metric_name,
      instance: this.state.instance === '' ? null : this.state.instance,
      start: this.state.start === '' ? null : this.state.start,
      group: this.state.group,
    }
    this.setState({ loadingFuture: true });
    const { success, data, msg } = await getFutureAlarmsInterface(params)
    if (success) {
      if (data.header.length > 0) {
        let historyColumObj = {}
        let tableHeader = []
        let instanceOptionsFilterArr = []
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
          res.push(tabledata)
        });
        res.forEach((item) => {
          instanceOptionsFilterArr.push(item.instance.replace(/(\s*$)/g, ''))
        })
        let instanceOptions = this.handleDataDeduplicate(instanceOptionsFilterArr)
        this.setState(() => ({
          futerInstanceOptionFilter: instanceOptions,
          loadingFuture: false,
          futureTableSource: res,
          columns: tableHeader,
          futurePagination: {
            total: res.length,
            defaultCurrent: 1
          }
        }))
      } else {
        this.setState({
          loadingFuture: false,
          futureTableSource: [],
          columns: [],
        })
      }
    } else {
      this.setState({
        loadingFuture: false,
        futureTableSource: [],
        columns: [],
      })
      message.error(msg)
    }
  }
  handleDataDeduplicate = (value) => {
    let newArr = []
    for (let i = 0; i < value.length; i++) {
      if (newArr.indexOf(value[i]) === -1 && value[i]) {
        newArr.push(value[i])
      }
    }
    return newArr
  }
  onSearch () { }
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
  // metric_name
  onChange1 (e) {
    this.setState({ metric_name: e})
  }
  onSearch1 = (value) => {
    if (value) {
      this.setState({
        metric_name: value,
        metricnewname: value
      })
    }
  };
  onBlurSelect1 = () => {
    const value = this.state.metricnewname
    if (value) {
      this.onChange1(value)
      this.setState({
        metricnewname: ''
      })
    }
  }
  // instance
  onChange2 (e) {
    this.setState({instance: e})
  }
  onSearch2 = (value) => {
    if (value) {
      this.setState({
        instance: value,
        instancenewname: value
      })
    }
  };
  onBlurSelect2 = () => {
    const value = this.state.instancenewname
    if (value) {
      this.onChange2(value)
      this.setState({instancenewname: ''})
    }
  }
  // start
  onChangeData = (date, dateString) => {
    if (dateString) {
      this.setState({
        start: new Date(dateString).getTime()
      })
    } else {
      this.setState({start: ''})
    }
  }
  // group
  onChangeGroupCheckbox (e) {
    this.setState({
      group: e.target.checked
    })
  }
  handleSearch () {
    this.getFutureAlarms()
  }
  handleRefresh(){
    this.setState({
      metric_name: '',
      instance: '',
      start: '',
      timekey:new Date(),
      group: true,
    },()=>{
      this.getFutureAlarms()
    })
  }
  componentDidMount () {
    this.getSearchMetric()
    this.getFutureAlarms()
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
        <Card title="Future Alarms" extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />}>
          <Row style={{ marginBottom: 20, width: '60%' }} justify="space-around">
            <Col>
              <span>metric name: </span>
              <Select placeholder="search metric" value={this.state.metric_name} showSearch allowClear onChange={(ev) => { this.onChange1(ev) }}
                onSearch={(e) => { this.onSearch1(e) }} onBlur={() => this.onBlurSelect1()}
                optionFilterProp="children" filterOption={(input, option) =>
                  option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0
                } style={{ width: 200 }}>
                {
                  this.state.options.map((item, index) => {
                    return (
                      <Option value={item} key={index}>{item}</Option>
                    )
                  })
                }
              </Select>
            </Col>
            <Col>
              <span>instance: </span>
              <Select showSearch allowClear value={this.state.instance} onChange={(ev) => { this.onChange2(ev) }}
                onSearch={(e) => { this.onSearch2(e) }} onBlur={() => this.onBlurSelect2()} optionFilterProp="children" filterOption={(input, option) =>
                  option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0
                } style={{ width: 200 }}>
                {
                  this.state.futerInstanceOptionFilter.map((item, index) => {
                    return (
                      <Option value={item} key={index}>{item}</Option>
                    )
                  })
                }
              </Select>
            </Col>
            <Col>
              <span>start: </span>
              <DatePicker key={this.state.timekey} showTime onChange={(date, dateString) => this.onChangeData(date, dateString)} />
            </Col>
            <Col style={{paddingTop:4}}>
              <span>group: </span>
              <Checkbox checked={this.state.group} onChange={(e) => { this.onChangeGroupCheckbox(e) }}></Checkbox>
            </Col>
            <Col>
              <Button type="primary" onClick={() => this.handleSearch()}>Search</Button>
            </Col>
          </Row>
          <Table bordered showSorterTooltip={false} components={this.components} columns={columns} dataSource={this.state.futureTableSource} rowKey={record => record.key} pagination={this.state.futurePagination} loading={this.state.loadingFuture} scroll={{ x: '100%' }} />
        </Card>
      </div>
    )
  }
}
