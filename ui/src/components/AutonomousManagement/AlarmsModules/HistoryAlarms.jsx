import React, { Component } from 'react';
import { Button, Card, Checkbox, Col, message, Row, Select, Table } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import ResizeableTitle from '../../common/ResizeableTitle';
import { getHistoryAlarmsInterface } from '../../../api/autonormousMangemant';
import { formatTableTime, formatTableTitle, formatTimestamp } from '../../../utils/function';

const { Option } = Select;
export default class Alarms extends Component {
  constructor() {
    super()
    this.state = {
      dataSource: [],
      columns: [],
      pagination: {
        total: 0,
        defaultCurrent: 1
      },
      loadingHistory: false,
      hostOptionsFilter: [],
      alarmOptionsFilter: [],
      alarmLevelOptionsFilter: [],
      host: '',
      hostnewval: '',
      alarm_type: '',
      typenewval: '',
      alarm_level: '',
      levelnewval: '',
      checkedGroup: true,
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  async getHistoryAlarms () {
    let params = {
      host: this.state.host === '' ? null : this.state.host,
      alarm_type: this.state.alarm_type === '' ? null : this.state.alarm_type,
      alarm_level: this.state.alarm_level === '' ? null : this.state.alarm_level,
      group: this.state.checkedGroup
    }
    this.setState({ loadingHistory: true });
    const { success, data, msg } = await getHistoryAlarmsInterface(params)
    if (success) {
      if (data.header.length > 0) {
        let historyColumObj = {}
        let tableHeader = []
        let hostOptionsFilterArr = []
        let alarmOptionsFilterArr = []
        let alarmLevelOptionsFilterArr = []
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
            tabledata[data.header[i]] = item[i]
            tabledata['key'] = index
            if (data.header[i] && data.header[i] === 'occurrence_at') {
              tabledata[data.header[i]] = formatTimestamp(item[i] + '')
            }
          }
          res.push(tabledata)
        });
        formatTableTime(res)
        res.forEach((item) => {
          hostOptionsFilterArr.push(item.host.replace(/(\s*$)/g, ''))
          alarmOptionsFilterArr.push(item.alarm_type)
          alarmLevelOptionsFilterArr.push(item.alarm_level)
        })
        let hostOptions = this.handleDataDeduplicate(hostOptionsFilterArr)
        let alarmTypeOptions = this.handleDataDeduplicate(alarmOptionsFilterArr)
        let alarmLevelOptions = this.handleDataDeduplicate(alarmLevelOptionsFilterArr)
        this.setState(() => ({
          hostOptionsFilter: hostOptions,
          alarmOptionsFilter: alarmTypeOptions,
          alarmLevelOptionsFilter: alarmLevelOptions,
          loadingHistory: false,
          dataSource: res,
          columns: tableHeader,
          pagination: {
            total: res.length,
            defaultCurrent: 1
          }
        }))
      } else {
        this.setState({
          loadingHistory: false,
          dataSource: [],
          columns: [],
        })
      }
    } else {
      this.setState({
        loadingHistory: false,
        dataSource: [],
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
  // host
  changeSelHostVal (value) {
    this.setState({
      host: value
    })
  }
  onSearchHost = (value) => {
    if (value) {
      this.setState({
        host: value,
        hostnewval: value
      })
    }
  };
  onBlurSelectHost = () => {
    const value = this.state.hostnewval
    if (value) {
      this.changeSelHostVal(value)
      this.setState({
        hostnewval: ''
      })
    }
  }
  // alarm_type
  changeSelTypeVal (value) {
    this.setState({
      alarm_type: value
    })
  }
  onSearchType = (value) => {
    if (value) {
      this.setState({
        alarm_type: value,
        typenewval: value
      })
    }
  };
  onBlurTypeSelect = () => {
    const value = this.state.typenewval
    if (value) {
      this.changeSelTypeVal(value)
      this.setState({typenewval: ''})
    }
  }
  // alarm_level
  changeSelLevelVal (value) {
    this.setState({alarm_level: value})
  }
  onSearchLevel = (value) => {
    if (value) {
      this.setState({
        alarm_level: value,
        levelnewval: value
      })
    }
  };
  onBlurLevelSelect = () => {
    const value = this.state.levelnewval
    if (value) {
      this.changeSelLevelVal(value)
      this.setState({levelnewval: ''})
    }
  }
  //group
  onChangeCheckbox (e) {
    this.setState({checkedGroup: e.target.checked})
  }
  handleSearch () {
    this.getHistoryAlarms()
  }
  handleRefresh(){
    this.setState({
      host: '',
      alarm_type: '',
      alarm_level: '',
      checkedGroup: true,
    },()=>{
      this.getHistoryAlarms()
    })
  }
  componentDidMount () {
    this.getHistoryAlarms()
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
      <div className="mb-20">
        <Card title="History Alarms" extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />} className="mb-20">
          <Row style={{ marginBottom: 20, width: '60%' }} justify="space-around">
            <Col>
              <span>host: </span>
              <Select value={this.state.host} onChange={(val) => { this.changeSelHostVal(val) }} showSearch allowClear
                onSearch={(e) => { this.onSearchHost(e) }} onBlur={() => this.onBlurSelectHost()}
                optionFilterProp="children" filterOption={(input, option) =>
                  option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 180 }} className="mb-20">
                {
                  this.state.hostOptionsFilter.map((item, index) => {
                    return (
                      <Option value={item} key={index}>{item}</Option>
                    )
                  })
                }
              </Select>
            </Col>
            <Col>
              <span>alarm type: </span>
              <Select value={this.state.alarm_type} onChange={(val) => { this.changeSelTypeVal(val) }} showSearch allowClear
                onSearch={(e) => { this.onSearchType(e) }} onBlur={() => this.onBlurTypeSelect()}
                optionFilterProp="children" filterOption={(input, option) =>
                  option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 180 }} className="mb-20">
                {
                  this.state.alarmOptionsFilter.map((item, index) => {
                    return (
                      <Option value={item} key={index}>{item}</Option>
                    )
                  })
                }
              </Select>
            </Col>
            <Col>
              <span>alarm level: </span>
              <Select value={this.state.alarm_level} onChange={(val) => { this.changeSelLevelVal(val) }} showSearch allowClear
                onSearch={(e) => { this.onSearchLevel(e) }} onBlur={() => this.onBlurLevelSelect()}
                optionFilterProp="children" filterOption={(input, option) =>
                  option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 180 }} className="mb-20">
                {
                  this.state.alarmLevelOptionsFilter.map((item, index) => {
                    return (
                      <Option value={item} key={index}>{item}</Option>
                    )
                  })
                }
              </Select>
            </Col>
            <Col style={{paddingTop:4}}> <span>group: </span> <Checkbox checked={this.state.checkedGroup} onChange={(e) => { this.onChangeCheckbox(e) }}></Checkbox></Col>
            <Col>
              <Button type="primary" onClick={() => this.handleSearch()}>Search</Button>
            </Col>
          </Row>
          <Table bordered showSorterTooltip={false} components={this.components} columns={columns} dataSource={this.state.dataSource} rowKey={record => record.key} pagination={this.state.pagination} loading={this.state.loadingHistory} scroll={{ x: '100%' }} />
        </Card>
      </div>
    )
  }
}
