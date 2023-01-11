import React from 'react';
import { ReloadOutlined } from '@ant-design/icons';
import { Card, message, Table } from 'antd';
import { getMetricStatisticInterface } from '../../../api/clusterInformation';
import { formatTableTitle, formatTimestamp } from '../../../utils/function';
export default class Statistics extends React.PureComponent {
  static propTypes={
    metricStatisticCount:PropTypes.object.isRequired
  }
  constructor() {
    super()
    this.state = {
      dataSource: [],
      columns: [],
      pageSize: 10,
      current: 1,
      total: 0,
      SearchForm: '',
      loading: false,
    }
  }
  async getMetricStatistic (params) {
    this.setState({ loading: true })
    const { success, data, msg } = await getMetricStatisticInterface(params)
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
          pageSize: this.state.pageSize,
          current: this.state.current
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
  // 回调函数，切换下一页
  changePage(current,pageSize){
    let params = {
      current: current,
      pagesize: pageSize,
    };
    this.setState({
      current: current,
    });
    this.getMetricStatistic(params);
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
    this.getMetricStatistic(params);
  }
  handleRefresh(){
    this.setState({
      pageSize: 10,
      current: 1,
    },()=>{
      this.props.getMetricStatisticInterfaceCount();
      this.getMetricStatistic({current: 1,pagesize: 10})
    })
  }
  componentDidMount () {
    this.getMetricStatistic({current: 1,pagesize: 10})
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    this.setState({total: nextProps.metricStatisticCount.total})
  }
  render () {
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
        <Card style={{ height: 780 }} title="Metric Statistics" extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />} >
          <Table bordered components={this.components} columns={this.state.columns} dataSource={this.state.dataSource} pagination={paginationProps} loading={this.state.loading} scroll={{ x: '100%'}}/>
        </Card>
      </div>
    )
  }
}
