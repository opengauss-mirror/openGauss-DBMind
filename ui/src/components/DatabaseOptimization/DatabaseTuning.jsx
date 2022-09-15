import React, {Component} from 'react';
import {Card, message, Table} from 'antd';
import {formatTableTitle} from '../../utils/function';
import {getDatabaseTuningInterface} from '../../api/databaseOptimaztion';

export default class DatabaseTuning extends Component {
  constructor(props) {
    super(props)
    this.state = {
      loading: false,
      metricDataSource: [],
      metricColumns: [],
      metricPagination: {
        total: 0,
        defaultCurrent: 1
      },
      warningDataSource: [],
      warningColumns: [],
      warningPagination: {
        total: 0,
        defaultCurrent: 1
      },
      knobDataSource: [],
      knobColumns: [],
      knobPagination: {
        total: 0,
        defaultCurrent: 1
      }
    }
  }
  async getDatabaseTuning () {
    this.setState({loading: true })
    const { success, data, msg } = await getDatabaseTuningInterface()
    if (success) {
      let detailsColumObj = {}
      let detailsTableHeader = []
      data.details.header.forEach((item) => {
        detailsColumObj = {
          title: formatTableTitle(item),
          dataIndex: item,
          key: item,
          ellipsis: true,
        }
        detailsTableHeader.push(detailsColumObj)
      })
      let detailsRes = []
      data.details.rows.forEach((item, index) => {
        let tabledata = {}
        for (let i = 0; i < data.details.header.length; i++) {
          tabledata[data.details.header[i]] = item[i]
          tabledata['key'] = index + ''
        }
        detailsRes.push(tabledata)
      });
      let metricColumObj = {}
      let metricTableHeader = []
      data.metric_snapshot.header.forEach((item) => {
        metricColumObj = {
          title: formatTableTitle(item),
          dataIndex: item,
          key: item,
          ellipsis: true,
        }
        metricTableHeader.push(metricColumObj)
      })
      let metricRes = []
      data.metric_snapshot.rows.forEach((item, index) => {
        let tabledata = {}
        for (let i = 0; i < data.metric_snapshot.header.length; i++) {
          tabledata[data.metric_snapshot.header[i]] = item[i]
          tabledata['key'] = index + ''
        }
        metricRes.push(tabledata)
      });
      let warningColumObj = {}
      let warningTableHeader = []
      data.warnings.header.forEach(item => {
        warningColumObj = {
          title: formatTableTitle(item),
          dataIndex: item,
          key: item,
          ellipsis: true,
          width: 180
        }
        warningTableHeader.push(warningColumObj)
      })
      let warningRes = []
      data.warnings.rows.forEach((item, index) => {
        let tabledata = {}
        for (let i = 0; i < data.warnings.header.length; i++) {
          tabledata[data.warnings.header[i]] = item[i]
          tabledata['key'] = index + ''
        }
        warningRes.push(tabledata)
      });
      this.setState(() => ({
        loading: false,
        metricDataSource: metricRes,
        metricColumns: metricTableHeader,
        metricPagination: {
          total: metricRes.length,
          defaultCurrent: 1
        },
        warningDataSource: warningRes,
        warningColumns: warningTableHeader,
        warningPagination: {
          total: warningRes.length,
          defaultCurrent: 1
        },
        knobDataSource: detailsRes,
        knobColumns: detailsTableHeader,
        knobPagination: {
          total: detailsRes.length,
          defaultCurrent: 1
        }
      }))
    } else {
      message.error(msg)
    }
  }
  componentDidMount () {
    this.getDatabaseTuning()
  }
  componentWillUnmount = () => {
      this.setState = () => {return}
  }
  render () {
    return (
      <div>
        <Card title="Metric Snapshot" className="mb-20 formlabel-160">
          <Table bordered dataSource={this.state.metricDataSource} columns={this.state.metricColumns} rowKey={record => record.key} pagination={this.state.metricPagination} loading={this.state.loading} scroll={{ x: '100%'}}/>
        </Card>
        <Card title="Warning" className="mb-20 formlabel-160">
          <Table bordered dataSource={this.state.warningDataSource} columns={this.state.warningColumns} rowKey={record => record.key} pagination={this.state.warningPagination} loading={this.state.loading} scroll={{ x: '100%'}}/>
        </Card>
        <Card title="Knob Recommendation" className="formlabel-160">
          <Table bordered dataSource={this.state.knobDataSource} columns={this.state.knobColumns} rowKey={record => record.key} pagination={this.state.knobPagination} loading={this.state.loading} scroll={{ x: '100%'}}/>
        </Card>
      </div>
    )
  }
}
