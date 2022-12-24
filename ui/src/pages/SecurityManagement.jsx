import React, { Component } from 'react';
import { Button, Card, Col, message, Row, Select, Table } from 'antd';
import ResizeableTitle from '../components/common/ResizeableTitle';
import { ReloadOutlined } from '@ant-design/icons';
import { getDetectedRiskInterface } from '../api/securityManagement';
import { formatTableTitle, formatTimestamp } from '../utils/function';

const { Option } = Select;
export default class SecurityManagement extends Component {
  constructor(props) {
    super(props)
    this.state = {
      dataSource: [],
      columns: [],
      pagination: {
        total: 0,
        defaultCurrent: 1
      },
      SearchForm: '',
      loading: false,
      selValue: '',
      instancenewname: '',
      optionsFilter: [],
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
  async getDetectedRisk () {
    let params = {
      instance: this.state.selValue === '' ? null : this.state.selValue
    }
    this.setState({
      loading: true
    })
    const { success, data, msg } = await getDetectedRiskInterface(params)
    if (success) {
      if (data.header.length > 0) {
        let historyColumObj = {}
        let tableHeader = []
        data.header.forEach(item => {
          historyColumObj = {
            title: formatTableTitle(item),
            dataIndex: item,
            key: item,
            ellipsis: true,
            width: 180
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
        let optionsArr = []
        res.forEach((item) => {
          optionsArr.push(item.instance.replace(/(\s*$)/g, ''))
        })
        let instanceOptions = this.handleDataDeduplicate(optionsArr)
        this.setState(() => ({
          loading: false,
          dataSource: res,
          columns: tableHeader,
          optionsFilter: instanceOptions,
          futurePagination: {
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
      this.setState({
        loading: false,
        dataSource: [],
        columns: [],
      })
      message.error(msg)
    }
  }
  changeSelInstanceVal (value) {
    this.setState({
      selValue: value
    })
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
    this.getDetectedRisk()
  }
  onSearch = (value) => {
    if (value) {
      this.setState({
        selValue: value,
        instancenewname: value
      })
    }
  };
  onBlurSelect = () => {
    const value = this.state.instancenewname
    if (value) {
      this.changeSelInstanceVal(value)
      this.setState({instancenewname: ''})
    }
  }
  handleRefresh(){
    this.setState({
      selValue:''
    },()=>{
      this.getDetectedRisk()
    })
  }
  componentDidMount () {
    this.getDetectedRisk()
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
      <div className="contentWrap">
        <Card title="Detected Risks" extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />} style={{ minHeight: 860 }}>
          <Row style={{ marginBottom: 10, width: '20%' }} justify="space-around">
            <Col>
              <span>instance: </span>
              <Select value={this.state.selValue} onChange={(val) => { this.changeSelInstanceVal(val) }} showSearch allowClear
                optionFilterProp="children" onSearch={(e) => { this.onSearch(e) }} onBlur={() => this.onBlurSelect()}
                filterOption={(input, option) =>
                  option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 180 }} className="mb-20">
                {
                  this.state.optionsFilter.map(item => {
                    return (
                      <Option value={item} key={item}>{item}</Option>
                    )
                  })
                }
              </Select>
            </Col>
            <Col>
              <Button type="primary" onClick={() => this.handleSearch()}>Search</Button>
            </Col>
          </Row>
          <Table bordered components={this.components} columns={columns} dataSource={this.state.dataSource} pagination={this.state.pagination} loading={this.state.loading} scroll={{ x: '100%'}}/>
        </Card>
      </div>
    )
  }
}