import React, { Component } from 'react';
import { Card, Table, message } from 'antd';
import PropTypes from 'prop-types'
import ResizeableTitle from '../../common/ResizeableTitle';
import { formatTableTitle } from '../../../utils/function';
import { getKnobRecommendationWarnings } from '../../../api/databaseOptimization'

export default class WarningData extends Component {
  static propTypes={
    warningData:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      dataSource: [],
      columns: [],
      pageSize: 10,
      current: 1,
      total: 0,
      loading: false
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  handleTableData (header, rows, total) {
    this.setState({loading: true})
    if (header.length > 0) {
      let historyColumObj = {}
      let tableHeader = []
      header.forEach(item => {
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
      rows.forEach((item, index) => {
        let tabledata = {}
        for (let i = 0; i < header.length; i++) {
          tabledata[header[i]] = item[i]
        }
        tabledata['key'] = index + ''
        res.push(tabledata)
      });
      this.setState(() => ({
        loading: false,
        dataSource: res,
        columns: tableHeader,
        pageSize: this.state.pageSize,
        current: this.state.current,
        total: total
      }))
    }
  }
  async getKnobRecommendationWarnings (params) {
    const { success, data, msg } = await getKnobRecommendationWarnings(params)
    if (success) {
      this.handleTableData(data.header, data.rows, this.state.total);
    } else {
      message.error(msg)
    }
  }
  // 回调函数，切换下一页
  changePage(current,pageSize){
    let params = {
      current: current,
      pagesize: pageSize,
    };
    this.setState({
      current: current,
    });
    this.getKnobRecommendationWarnings(params);
  }
    // 回调函数,每页显示多少条
  changePageSize(pageSize,current){
    // 将当前改变的每页条数存到state中
    this.setState({
      pageSize: pageSize
    });
    let params = {
      current: current,
      pagesize: pageSize,
    };
    this.getKnobRecommendationWarnings(params);
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
  UNSAFE_componentWillReceiveProps (nextProps) {
    this.handleTableData(nextProps.warningData.header, nextProps.warningData.rows, nextProps.warningData.total)
  }
  render () {
    const columns = this.state.columns.map((col, index) => ({
      ...col,
      onHeaderCell: column => ({
        width: column.width,
        onResize: this.handleResize(index)
      })
    }))
    const warningProps = {
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
        <Card title="Warning" className="mb-20 formlabel-160">
          <Table bordered components={this.components} dataSource={this.state.dataSource} columns={columns} rowKey={record => record.key} pagination={warningProps} loading={this.state.loading} scroll={{ x: '100%'}}/>
        </Card>
      </div>
    )
  }
}
