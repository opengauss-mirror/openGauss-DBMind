import React, { Component } from 'react';
import { DownloadOutlined } from '@ant-design/icons';
import { Button, Card, Input, message, Select, Table, Upload } from 'antd';
import { getItemListInterface, getListIndexAdvisorInterface } from '../../api/aitook';
import { formatTableTitle } from '../../utils/function';

const { TextArea } = Input;
const { Option } = Select;
export default class IndexAdvisor extends Component {
  constructor(props) {
    super(props)
    this.state = {
      columns: [],
      data: [],
      pagination: {
        total: 0,
        defaultCurrent: 1
      },
      selValue: '',
      textareaVal: '',
      options: [],
      fileList: []
    }
  }
  async getItemList () {
    const { success, data, msg } = await getItemListInterface()
    if (success) {
      this.setState({options: data})
    } else {
      message.error(msg)
    }
  }
  async getListIndexAdvisor (arrs) {
    let params = {
      database: this.state.selValue,
      textareaVal: arrs
    }
    const { success, data, msg } = await getListIndexAdvisorInterface(params)
    if (success) {
      let historyColumObj = {}
      let tableHeader = []
      if (data.header) {
        data.header.forEach((item) => {
          historyColumObj = {
            title: formatTableTitle(item),
            dataIndex: item,
            key: item,
            ellipsis: true,
          }
          tableHeader.push(historyColumObj)
        })
        let res = []
        data.rows.forEach((item, index) => {
          let tabledata = {}
          for (let i = 0; i < data.header.length; i++) {
            tabledata[data.header[i]] = item[i]
            tabledata['key'] = index
          }
          res.push(tabledata)
        });
        this.setState(() => ({
          data: res,
          columns: tableHeader,
          pagination: {
            total: res.length,
            defaultCurrent: 1
          }
        }))
      } else {
        message.warning('No data.')
      }
    } else {
      message.error(msg)
    }
  }
  addTableData = (type) => {
    if (this.state.selValue === '') {
      message.warning('Please choose a database.')
    } else if (this.state.textareaVal === '') {
      message.warning('Please enter the content of SQL statements.')
    } else {
      let arr = this.state.textareaVal.split(/\n+/)
      if (type === 'type1') {
        this.getListIndexAdvisor(arr)
      } else if (type === 'type2') {
        let paramArr = [...arr, ...this.state.fileList]
        this.getListIndexAdvisor(paramArr)
      }
    }
  }
  changeVal (e) {
    this.setState({[e.target.name]: e.target.value})
  }
  changeSelVal (value) {
    this.setState({selValue: value})
  }
  beforeUpload = (file) => {
    const reader = new FileReader()
    reader.readAsText(file)
    reader.onload = (result) => {
      let targetNum = result.target.result
      let array = targetNum.split(/\s+/)
      this.setState({fileList: array}, () => {
        this.addTableData('type2')
      })
    }
    return false
  }
  componentDidMount () {
    this.getItemList()
  }
  render () {
    return (
      <div>
        <Card className="mb-20" title="Smart Index Recommendation" bordered={false} style={{ width: '100%', height: 380 }}>
          <div className="flexbox">

            <div className="flextitle1">Database List：</div>
            <Select value={this.state.selValue} onChange={(val) => { this.changeSelVal(val) }} showSearch
              optionFilterProp="children" filterOption={(input, option) =>
                option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 200 }} className="mb-20">
              {
                this.state.options.map(item => {
                  return (
                    <Option value={item} key={item}>{item}</Option>
                  )
                })
              }
            </Select>
          </div>
          <div className="flexbox">
            <div className="flextitle2">SQL Statements：</div>
            <TextArea rows={8} name="textareaVal" onChange={(ev) => { this.changeVal(ev) }} placeholder={`# Please enter the SQL statement as the following, then the index advisor will return suggestions.
# SELECT * FROM t1 WHERE t1.id > 100`} />
          </div>
        </Card>
        <Card title="Recommended Set" bordered={false} style={{ width: '100%', height: 400 }}>
          <Button type="primary" size="small" style={{ margin: '0 10px 20px 0' }} onClick={() => this.addTableData('type1')}>Adivse Index</Button>
          <Upload
            name="file"
            maxCount={1}
            beforeUpload={(file) => this.beforeUpload(file)}
          >
            <Button size="small" icon={<DownloadOutlined />}>Upload</Button>
          </Upload>
          <Table columns={this.state.columns} dataSource={this.state.data} rowKey={record => record.key} pagination={this.state.pagination} />
        </Card>
      </div>
    )
  }
}
