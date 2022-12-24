import React from 'react';
import { ReloadOutlined } from '@ant-design/icons';
import { Card, message, Table } from 'antd';
import { getMetricStatisticInterface } from '../../../api/clusterInformation';
import { formatTableTitle, formatTimestamp } from '../../../utils/function';
export default class Statistics extends React.PureComponent {
  constructor() {
    super()
    this.state = {
      dataSource: [],
      columns: [],
      pagination: {
        total: 0,
        defaultCurrent: 1
      },
      SearchForm: '',
      loading: false,
    }
  }
  async getMetricStatistic () {
    this.setState({ loading: true })
    const { success, data, msg } = await getMetricStatisticInterface()
    if (success) {
      if (data.header.length > 0) {
        let historyColumObj = {}
        let tableHeader = []
        data.header.forEach((item, index) => {
          if (index > 0) {
            historyColumObj = {
              title: formatTableTitle(item),
              dataIndex: item,
              key: item,
              ellipsis: true,
              width: 180
            }
            tableHeader.push(historyColumObj)
          }
        })
        let res = []
        data.rows.forEach((item, index) => {
          if (index > 0) {
            let tabledata = {}
            for (let i = 0; i < data.header.length; i++) {
              tabledata[data.header[i]] = item[i]
              tabledata['key'] = index
              if (data.header[i] && data.header[i] === 'date') {
                tabledata[data.header[i]] = formatTimestamp(item[i] + '')
              }
            }
            res.push(tabledata)
          }
        });
        let optionsArr = []
        res.forEach((item) => {
          optionsArr.push(item.instance.replace(/(\s*$)/g, ''))
        })
        this.setState(() => ({
          loading: false,
          dataSource: res,
          columns: tableHeader,
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
  componentDidMount () {
    this.getMetricStatistic()
  }
  render () {
    return (
      <div>
        <Card style={{ height: 780 }} title="Metric Statistics" extra={<ReloadOutlined className="more_link" onClick={() => { this.getMetricStatistic() }} />} >
          <Table bordered components={this.components} columns={this.state.columns} dataSource={this.state.dataSource} pagination={this.state.pagination} loading={this.state.loading} scroll={{ x: '100%'}}/>
        </Card>
      </div>
    )
  }
}
