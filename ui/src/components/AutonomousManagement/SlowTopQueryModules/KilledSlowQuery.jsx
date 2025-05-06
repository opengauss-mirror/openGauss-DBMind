import React, { Component } from 'react';
import { Button, Card, Col, DatePicker, Form, Input, InputNumber, message, Row, Table } from 'antd';
import moment from 'moment';
import { ReloadOutlined } from '@ant-design/icons';
import ResizeableTitle from '../../common/ResizeableTitle';
import { getKillSlowQueryInterface, getKillSlowQueryInterfaceCount} from '../../../api/autonomousManagement';
import { formatTableTitle, formatTimestamp } from '../../../utils/function';

const { RangePicker } = DatePicker;
export default class SlowTopQuery extends Component {
  constructor() {
    super()
    this.state = {
      dataSource2: [],
      columns: [],
      pageSize: 10,
      current: 1,
      total: 0,
      loadingKill: false,
      startTime: '',
      endTime: '',
      startTime1: '',
      endTime1: '',
      checkedGroup: true,
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  onKillFinish = (values) => {
    let paramVal = {
      start: this.state.startTime ? this.state.startTime : null,
      end: this.state.endTime ? this.state.endTime : null,
      query: values.query ? encodeURIComponent(values.query) : null,
      limit: values.limit === '' ? null : values.limit,
      current: this.state.current,
      pagesize:this.state.pageSize
    }
    this.getKillSlowQuery(paramVal).then(() => {
      this.getKillSlowQueryCount(values)
    })
  }
  async getKillSlowQuery (params) {
    this.setState({loadingKill: true})
    const { success, data, msg } = await getKillSlowQueryInterface(params)
    if (success) {
      if (data.header.length > 0) {
        let historyColumObj = {}
        let tableHeader = []
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
            if ((data.header[i] && data.header[i] === 'killed_time')) {
              tabledata[data.header[i]] = formatTimestamp(item[i])
            }
          }
          res.unshift(tabledata)
        });
        this.setState(() => ({
          loadingKill: false,
          dataSource2: res,
          columns: tableHeader,
          pageSize: this.state.pageSize,
          current: this.state.current
        }))
      } else {
        this.setState({
          loadingKill: false,
          dataSource2: [],
          columns: [],
        })
      }
    } else {
      this.setState({
        loadingKill: false,
        dataSource2: [],
        columns: [],
      })
      message.error(msg)
    }
  }
  async getKillSlowQueryCount (values) {
    let params = {
      start: this.state.startTime ? this.state.startTime : null,
      end: this.state.endTime ? this.state.endTime : null,
      query: (values && values.query) ? encodeURIComponent(values.query) : null,
      limit: (values && values.limit) ? values.limit : null
    }
    const { success, data, msg } = await getKillSlowQueryInterfaceCount(params)
    if (success) {
      this.setState(() => ({
        total: data
      }))
    } else {
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
  onChangeTimes1 = (dates, dateStrings) => {
    this.setState(() => ({
      startTime1: new Date(dateStrings[0]).getTime(),
      endTime1: new Date(dateStrings[1]).getTime()
    }))
  }
  onChangeCheckbox (e) {
    this.setState({checkedGroup: e.target.checked})
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
    this.getKillSlowQuery(pageParams);
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
    this.getKillSlowQuery(pageParams);
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
    this.setState({
      pageSize: 10,
      current: 1,
    },()=>{
      this.killFormRef.resetFields()
      this.getKillSlowQuery({current: 1,pagesize: 10}).then(() => {
        this.getKillSlowQueryCount()
      })
    })
  }
  componentDidMount () {
    this.getKillSlowQuery({current: 1,pagesize: 10}).then(() => {
      this.getKillSlowQueryCount()
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
      <div>
        <Card title="Killed Slow Query" extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />} className="mb-20" style={{ marginBottom: 20 }}>
          <Row style={{ marginBottom: 20 }}>
            <Col span={24}>
              <Form
                layout="inline"
                onFinish={this.onKillFinish}
                ref={(e) => {
                  this.killFormRef = e
                }}
              >
                <Form.Item name="query" label="query">
                  <Input placeholder="Input query" allowClear={true} />
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
                <Form.Item >
                  <Button type="primary" htmlType="submit">Search</Button>
                </Form.Item>
              </Form>
            </Col>
          </Row>
          <Table bordered showSorterTooltip={false} components={this.components} columns={columns} dataSource={this.state.dataSource2} rowKey={record => record.key} pagination={paginationProps} loading={this.state.loadingKill} scroll={{ x: '100%' }} />
        </Card>
      </div>
    )
  }
}
