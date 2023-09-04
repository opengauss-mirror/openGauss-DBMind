import React, { Component } from 'react';
import { Button, Card, Checkbox, Col, message, Row, Select, Table, Modal, DatePicker,Input } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import ResizeableTitle from '../../common/ResizeableTitle';
import MetricChart from './MetricChart';
import { getHistoryAlarmsInterface, getHistoryAlarmsInterfaceCount } from '../../../api/autonomousManagement';
import { formatTableTitle, formatTimestamp } from '../../../utils/function';
const { Option } = Select;
const { RangePicker } = DatePicker;
export default class Alarms extends Component {
  constructor() {
    super()
    this.state = {
      dataSource: [],
      columns: [],
      pageSize: 10,
      current: 1,
      total: 0,
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
      isModalVisible: false,
      metric_name: '',
      start_time: '',
      end_time: '',
      startTime: '',
      endTime: '',
      metricName: '',
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  async getHistoryAlarms (pageParams) {
    let params = {
      instance: this.state.host === '' ? null : this.state.host,
      alarm_type: this.state.alarm_type === '' ? null : this.state.alarm_type,
      alarm_level: this.state.alarm_level === '' ? null : this.state.alarm_level,
      metric_name: this.state.metricName === '' ? null : this.state.metricName,
      group: this.state.checkedGroup,
      current: pageParams ? pageParams.current : this.state.current,
      pagesize:pageParams ? pageParams.pagesize : this.state.pageSize,
      start_at:this.state.startTime ? this.state.startTime : null,
      end_at:this.state.endTime ? this.state.endTime : null
    }
    this.setState({ loadingHistory: true });
    const { success, data, msg } = await getHistoryAlarmsInterface(params)
    if (success) {
      if (data.header.length > 0) {
        let historyColumObj = {}
        let operationColumObj = {}
        let tableHeader = []
        let hostOptionsFilterArr = []
        let alarmOptionsFilterArr = []
        let alarmLevelOptionsFilterArr = []
        if(data.header.length > 9){
          data.header.push('operation')
        }
        data.header.forEach((item) => {
          operationColumObj = {
            sorter: (a, b) => {
              if(Object.keys(a).includes(item)){
                let aVal = a[item]
                let bVal = b[item]
                let c = isFinite(aVal),
                  d = isFinite(bVal);
                return (c !== d && d - c) || (c && d ? aVal - bVal : aVal.localeCompare(bVal));
              }
            }
          }
          historyColumObj = {
            title: formatTableTitle(item),
            dataIndex: item,
            width: 180,
            key: item,
            ellipsis: true,
            align:item === 'operation' ? 'center' : 'left',
            fixed:item === 'operation' ? 'right' : 'false',
            render: (row, record) => {
              if(item === 'operation'){
                return <Button type="primary" disabled={(record.anomaly_type === "Threshold" || record.anomaly_type === "GRADIENT")} onClick={() => this.isModal(row, record)}>analyze</Button>
              } else {
                return row
              }
            },
            ...(item !== 'operation' ? operationColumObj: '')
          }
            tableHeader.push(historyColumObj)
        })
        let res = []
        data.rows.forEach((item, index) => {
          let tabledata = {}
          for (let i = 0; i < data.header.length; i++) {
              tabledata[data.header[i]] = item[i]
              tabledata['key'] = index
            if (data.header[i] && (data.header[i] === 'start_at' || data.header[i] === 'end_at')) {
              tabledata[data.header[i]] = formatTimestamp(item[i] + '')
            }
            if (data.header[i] && data.header[i] === 'alarm_level') {
                switch (item[i]) {
                  case 50:
                      tabledata[data.header[i]] = "CRITICAL"
                      break;
                  case 40:
                      tabledata[data.header[i]] = "ERROR"
                      break;
                  case 30:
                      tabledata[data.header[i]] = "WARNING"
                      break;
                  case 20:
                      tabledata[data.header[i]] = "INFO"
                      break;
                  case 10:
                      tabledata[data.header[i]] = "DEBUG"
                      break;
                  case 0:
                      tabledata[data.header[i]] = "NOTSET"
                      break;
                  default:
                      tabledata[data.header[i]] = item[i]
                      break;
              }
            }
          }
          res.push(tabledata)
        });
        res.forEach((item) => {
          hostOptionsFilterArr.push(item.instance.replace(/(\s*$)/g, ''))
          alarmOptionsFilterArr.push(item.alarm_type)
          alarmLevelOptionsFilterArr.push(item.alarm_level)
        })
        let hostOptions = this.handleDataDeduplicate(hostOptionsFilterArr)
        let alarmTypeOptions = this.handleDataDeduplicate(alarmOptionsFilterArr)
        let alarmLevelOptions = this.handleDataConversion(this.handleDataDeduplicate(alarmLevelOptionsFilterArr))
        this.setState(() => ({
          hostOptionsFilter: hostOptions,
          alarmOptionsFilter: alarmTypeOptions,
          alarmLevelOptionsFilter: alarmLevelOptions,
          loadingHistory: false,
          dataSource: res,
          columns: tableHeader,
          pageSize: this.state.pageSize,
          current: this.state.current
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
  async getHistoryAlarmsCount () {
    let params = {
      instance: this.state.host === '' ? null : this.state.host,
      alarm_type: this.state.alarm_type === '' ? null : this.state.alarm_type,
      alarm_level: this.state.alarm_level === '' ? null : this.state.alarm_level,
      metric_name: this.state.metricName === '' ? null : this.state.metricName,
      group: this.state.checkedGroup,
      start_at:this.state.startTime ? this.state.startTime : null,
      end_at:this.state.endTime ? this.state.endTime : null
    }
    const { success, data, msg } = await getHistoryAlarmsInterfaceCount(params)
    if (success) {
      this.setState(() => ({
        total: data
      }))
    } else {
      message.error(msg)
    }
  }
  handleCancel = () => {
    this.setState({
      isModalVisible: false,
      metric_name: '',
      host: '',
      start_time: '',
      end_time: ''
    })
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
  handleDataConversion = (value) => {
    let newArr = []
    for (let i = 0; i < value.length; i++) {
      switch (value[i]) {
        case "CRITICAL":
          newArr.push({key:value[i],value:50})
            break;
        case "ERROR":
          newArr.push({key:value[i],value:40})
            break;
        case "WARNING":
          newArr.push({key:value[i],value:30})
            break;
        case "INFO":
          newArr.push({key:value[i],value:20})
            break;
        case "DEBUG":
          newArr.push({key:value[i],value:10})
            break;
        case "NOTSET":
          newArr.push({key:value[i],value:0})
            break;
        default:
          newArr.push({key:value[i],value:value[i]})
            break;
    }
    }
    return newArr
  }
  isModal(row, record) {
    this.setState({
      metric_name: record.metric_name,
      host:record.instance.replace(/(\s*$)/g, ''),
      start_time:new Date(record.start_at).getTime(),
      end_time:new Date(record.end_at).getTime(),
      isModalVisible: true,
      metric_filter:record.metric_filter
    })
  }
  // 回调函数，切换下一页
  changePage(current,pageSize){
    let pageParams = {
      current: current,
      pagesize: pageSize,
    };
    this.setState({
      current: current,
    });
    this.getHistoryAlarms(pageParams);
  }
    // 回调函数,每页显示多少条
  changePageSize(pageSize,current){
    // 将当前改变的每页条数存到state中
    this.setState({
      pageSize: pageSize
    });
    let pageParams = {
      current: current,
      pagesize: pageSize,
    };
    this.getHistoryAlarms(pageParams);
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
  changeMetricVal (e) {
    this.setState({metricName: e.target.value})
  }
  onBlurMetricInput = (e) => {
    this.changeMetricVal(e)
  }
  //group
  onChangeCheckbox (e) {
    this.setState({checkedGroup: e.target.checked})
    if(e.target.checked){
      this.setState({
        startTime: '',
        endTime: '',
      })
    };
  }
  handleSearch () {
    this.getHistoryAlarms().then(() => {
      this.getHistoryAlarmsCount()
    })
  }
  handleRefresh(){
    this.setState({
      host: '',
      alarm_type: '',
      alarm_level: '',
      metricName: '',
      checkedGroup: true,
      pageSize: 10,
      current: 1
    },()=>{
      this.getHistoryAlarms().then(() => {
        this.getHistoryAlarmsCount()
      })
    })
  }
  setDates = (dates, dateStrings) => {
    this.setState({
      startTime: new Date(dateStrings[0]).getTime(),
      endTime: new Date(dateStrings[1]).getTime(),
    });
  };
  componentDidMount () {
    this.getHistoryAlarms().then(() => {
      this.getHistoryAlarmsCount()
    })
  }
  render () {
    const columns = this.state.columns.map((col, index) => ({
      ...col,
      onHeaderCell: column => ({
        width: column.width,
        onResize: this.handleResize(index)
      })
    }))
    const paginationProps = {
      showSizeChanger: true,
      showQuickJumper: true,
      showTotal: () => `Total ${this.state.total} items`,
      pageSize: this.state.pageSize,
      current: this.state.current,
      total: this.state.total,
      onShowSizeChange: (current,pageSize) => this.changePageSize(pageSize,current),
      onChange: (current,pageSize) => this.changePage(current,pageSize)
    };
    return (
      <div className="mb-20">
        <Card title="History Alarms" extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />} className="mb-20">
          <Row style={{ marginBottom: 20, width: this.state.checkedGroup ? '60%' : '100%'}} justify="space-around">
          <Col style={{paddingTop:4}}> <span>group: </span> <Checkbox checked={this.state.checkedGroup} onChange={(e) => { this.onChangeCheckbox(e) }}></Checkbox></Col>
            <Col>
              <span>host: </span>
              <Select value={this.state.host} onChange={(val) => { this.changeSelHostVal(val) }} showSearch allowClear={true}
                onSearch={(e) => { this.onSearchHost(e) }} onBlur={() => this.onBlurSelectHost()}
                optionFilterProp="children" filterOption={(input, option) =>
                  option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 160 }} className="mb-10">
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
              <Select value={this.state.alarm_type} onChange={(val) => { this.changeSelTypeVal(val) }} showSearch allowClear={true}
                onSearch={(e) => { this.onSearchType(e) }} onBlur={() => this.onBlurTypeSelect()}
                optionFilterProp="children" filterOption={(input, option) =>
                  option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 160 }} className="mb-10">
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
              <Select value={this.state.alarm_level} onChange={(val) => { this.changeSelLevelVal(val) }} showSearch allowClear={true}
                onSearch={(e) => { this.onSearchLevel(e) }} onBlur={() => this.onBlurLevelSelect()}
                optionFilterProp="children" filterOption={(input, option) =>
                  option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 160 }} className="mb-10">
                {
                  this.state.alarmLevelOptionsFilter.map((item, index) => {
                    return (
                      <Option value={item.value} key={index}>{item.key}</Option>
                    )
                  })
                }
              </Select>
            </Col>
            {!this.state.checkedGroup && <Col>
              <span>metric name: </span>
              <Input onChange={(e) => this.changeMetricVal(e)} onBlur={(e) => this.onBlurMetricInput(e)} value={this.state.metricName} style={{ width:160 }} className="mb-10" />
            </Col>}
            {!this.state.checkedGroup && <Col>
              <RangePicker 
                style={{ width:340 }}
                placement='topRight'
                format="YYYY-MM-DD HH:mm:ss"
                onChange={this.setDates}
                showTime
              />
            </Col>}
            <Col>
              <Button type="primary" onClick={() => this.handleSearch()}>Search</Button>
            </Col>
          </Row>
          <Table bordered showSorterTooltip={false} components={this.components} columns={columns.filter((item) => item.dataIndex !== 'history_alarm_id' && item.dataIndex !== 'metric_filter')} dataSource={this.state.dataSource} rowKey={record => record.key} pagination={paginationProps} loading={this.state.loadingHistory} scroll={{ x: '100%' }} />
        </Card>
        <Modal title="Abnormal Root Cause Analysis" style={{maxWidth: "80vw"}} bodyStyle={{overflowY: "auto",height: "80vh", background: '#f1f1f1'}} width="80vw" okButtonProps={{ style: { display: 'none' } }} 
         destroyOnClose='true' visible={this.state.isModalVisible} maskClosable = {false} centered='true' onCancel={() => this.handleCancel()}>
          <MetricChart metric_name={this.state.metric_name}  metric_filter={this.state.metric_filter} host={this.state.host} start_time={this.state.start_time} end_time={this.state.end_time}/>
        </Modal>
      </div>
    )
  }
}
