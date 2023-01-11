import React, { Component } from 'react';
import { Table, Card, message, Empty, Spin } from 'antd';
import { ReloadOutlined } from '@ant-design/icons'; 
import ReactEcharts from 'echarts-for-react';
import { getRegularInspections } from '../../../api/autonormousMangemant';
import { formatTableTitle, formatTimestamp } from '../../../utils/function';

const columnsTable = ['name','address','corr','delay'];
export default class ThroughputChart extends Component {
  constructor(props) {
    super(props)
    this.state = {
      showFlag: 0,
      allDataRegular:[],
      columns:[],
      dataSource: [],
      rootCausecolumns:[{
        title: 'root cause',
        dataIndex: 'root_cause',
        key: 'root_cause',
      },
      {
        title: 'suggestion',
        dataIndex: 'suggestion',
        key: 'suggestion',
      }],
      rootCauseDataSource: [],
      loading: false
    }
  }
  async getQps () {
    this.setState({ loading: true })
    let params = {
      metric_name: this.props.metric_name,
      host: this.props.host,
      start_time: this.props.start_time,
      end_time: this.props.end_time
    }
    const { success, data, msg } = await getRegularInspections(params)
    if (success) {
      if (data && data[`${Object.keys(data)[0]}`].length > 0) {
        let arrayData = [],dataSourceData = [],columnsArr = [], colors = ['#5470c6', '#91cc75', '#fac858']
        columnsTable.forEach(item => {
          let obj = {
            title: formatTableTitle(item),
            dataIndex: item,
            key: item,
            ellipsis: true,
            width: item === 'name' ? 360 : 180
          }
          columnsArr.push(obj)
        })
        data[`${params.metric_name} from ${params.host}`].forEach((item, index) => {
          let dataSourceArray = {
            'name':item[0].replace(/(\s*$)/g, '').split("from")[0],'address':item[0].replace(/(\s*$)/g, '').split("from")[1],'corr':item[1],'delay':item[2]
          }
          if(index){
            dataSourceData.push(dataSourceArray);
          }
          // 处理X轴
          let formatTimeData = [];
          item[4].forEach(ele => {
            formatTimeData.push(formatTimestamp(ele));
          });
          // 处理Y轴
          let seriesItem = {
            data: item[3],
            type: 'line',
            smooth: true,
            name: item[0].replace(/(\s*$)/g, '').split("from")[1],
            symbol: 'none',
            color: colors[0],
          }
          let param = {
            xdata:formatTimeData,
            seriesData:seriesItem,
            yname:item[0]
          }
          arrayData.push(param);
        })
        this.setState(() => ({
          showFlag: 0,
          loading: false,
          allDataRegular: [...arrayData],
          columns: columnsArr,
          dataSource: dataSourceData
        }), () => {
          this.echartsElement.resize();
        })
      } else {
        this.setState({showFlag: 1,loading: false})
      }
    } else {
      this.setState({showFlag: 1,loading: false})
      message.error(msg);
    }
  }
  getOption = (item) => {
    return {
      title: {
        text: item.yname,
        left: 'center',
        textStyle:{
          color: '#314b71',
          fontSize: '12'
        }
      },
      grid: {
        containLabel: true,
        width: '100%',
        left: '0%',
        top: '15%',
        right: '0%'
      },
      xAxis: {
        axisLine: {
          lineStyle: {
            width: 0,
          }
        },
        axisLabel: {
          padding: [0, 0, 0, 80],
          textStyle: {
            color: '#314b71',
            fontSize: '10'
          }
        },
        type: 'category',
        data: item.xdata
      },
      yAxis: {
        type: 'value',
        nameLocation: 'end',
        nameTextStyle: {
          padding: [0, 0, 8, 120],
          color: '#314b71',
        },
        nameGap: 15,
        axisLabel: {
          textStyle: {
            color: '#314b71',
            fontSize: '12'
          }
        }
      },
      tooltip: {trigger: 'axis'},
      dataZoom: {
        start: 0,
        end: 100,
        show: true,
        type: 'slider',
        handleSize: '100%',
        left: '0%',
        right: '0.3%',
        height: 15
      },
      series: item.seriesData
    }
  }
  handleRefresh () {
    this.setState({showFlag: 2}, () => {
      this.getQps()
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
  componentDidMount () {
    this.setState({showFlag: 2}, () => {
      this.getQps()
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
    return (
      <div>
        <Card className="mb-20" style={{ height: '100%' }} title="">
          <Table bordered components={this.components} columns={columns} dataSource={this.state.dataSource} rowKey={record => record.key} loading={this.state.loading} pagination={false} scroll={{ x: '100%' }} />
        </Card>
        <Card title={`${this.props.metric_name} from ${this.props.host}`} style={{ height: '100%' }} extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />}>
          {this.state.showFlag === 0 ? this.state.allDataRegular.map((item) => {
                return (
                    <ReactEcharts
                    ref={(e) => {
                      this.echartsElement = e
                    }}
                    option={this.getOption(item)}
                    style={{ width: '100%', height: 240 }}
                    lazyUpdate={true}
                  >
                  </ReactEcharts>
                )
              })
            : this.state.showFlag === 1 ? <Empty description={false} style={{ paddingTop: 50 }} /> : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>}
            <Table bordered dataSource={this.state.rootCauseDataSource} columns={this.state.rootCausecolumns} rowKey={record => record.key} loading={this.state.loading} />
        </Card>
      </div>
    )
  }
}
