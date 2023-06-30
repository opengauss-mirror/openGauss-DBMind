import React, { Component } from 'react';
import { Input, message, Card, Col, Row, Table, Tabs, DatePicker, Modal, Radio, Drawer } from 'antd';
import { getIntelligentSqlAnalysisInterface } from '../../api/aiTool';
import { getLabelData, getCollect } from '../../api/databaseOptimization';
import { getExecutionPlan } from '../../api/autonomousManagement';
import ReactEcharts from 'echarts-for-react';
import moment from 'moment';
import Analyze from '../../assets/imgs/Analyze.png';
import Detail from '../../assets/imgs/Detail.png';
import GetBack from '../../assets/imgs/getback.png';
import ExecutionPlan from '../../assets/imgs/ExecutionPlan.png';
import PropTypes from 'prop-types';
import '../../assets/css/common.css'
import '../../assets/css/main/databaseOptimization.css'
import { formatTimestamp } from '../../utils/function';
import db from '../../utils/storage';
import DrawerInfo from '../DatabaseOptimization/DrawerInfo';
import SqlPlan from '../NodeInformation/SqlPlan';

const { RangePicker } = DatePicker;
const { TextArea } = Input;
const { TabPane } = Tabs;
const yAx = {axisLabel: {
  formatter: (val) => {
    let value = val + '%'
    return value;
  }
}}
export default class SlowSqlDiagnosis extends Component {
  static propTypes={
    tableData:PropTypes.array.isRequired,
    tableHeader:PropTypes.array.isRequired,
    formData:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      dataSource: [],
      columns: [],
      loading: false,
      propformData:{},
      proptableData:[],
      proptableHeader:[],
      selectedRowKeys: [],
      selectedRow:[],
      checkType: 1440,
      startTime: new Date().getTime() - 86400000,
      endTime: new Date().getTime(),
      isAspClick:false,
      barYData:[],
      userData:{timestamps:[],values:[]},
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
      textAreaData:'',
      isAnalyzeVisible:false,
      isOpen:false,
      uniqueSqlId:'',
      sqlData:[],
      executionTime:['0-10ms','10ms-100ms','100ms-1s','1s-5s','5s+'],
      isPlanVisible: false,
      planData:''
    }
  }
  onRowClick = (record, index) => {
    const selectedList = [...this.state.selectedRowKeys]
    if (selectedList[0] === record.key){
      this.setState({
        selectedRowKeys: [],
        selectedRow: []
      },() => 
      this.getCpuData ()
      );
      return
    }
    selectedList[0] = record.key
    if( this.state.isAspClick ){
      this.setState({
        selectedRowKeys: selectedList,
        selectedRow: [record],
      },() => 
      this.getCpuData ('',record.unique_sql_id)
      )
    } else {
      let time = this.getLastTime(record.start_time,this.state.checkType,1)
      this.setState({
        selectedRowKeys: selectedList,
        selectedRow: [record],
        startTime:time[0],
        endTime:time[1]
      },() => 
      this.getCpuData ()
      )
    }
  }
  getLastTime(time,Interval,medianFig){
    let newTime = new Date(time.replace(/-/g,'/')).getTime(),startTime = '',endTime = ''
    startTime = newTime - Interval * 60 * 1000
    endTime = medianFig ? newTime + Interval * 60 * 1000 : newTime
    return [startTime,endTime]
  }
  changeTypeVal(e) {
    this.setState({checkType: e.target.value}
      ,() => {
        let newDate = this.state.isAspClick ? formatTimestamp(new Date().getTime()) : this.state.selectedRow[0].start_time
        let time = this.getLastTime(newDate,this.state.checkType,!this.state.isAspClick)
        this.setState({startTime: time[0],endTime:time[1]},() => {this.getCpuData ('',this.state.selectedRow.length ? this.state.selectedRow[0].unique_sql_id : '')})
      }
    )
  }
  async getDiagnosis (params) {
    this.setState({ loading: true })
    const { success, msg, data } = await getIntelligentSqlAnalysisInterface(params)
    if (success) {
      let res = []
      data[1][0][0].forEach((it, idx) => {
        let obj = {
          key: idx,
          root_cause: data[1][0][0][idx],
          suggestion: data[1][1][0][idx]
        }
        res.push(obj)
      })
      this.setState({
        loading: false,
        rootCauseDataSource: res,
        textAreaData:data[0]
      })
    } else {
      this.setState({loading: false}, () => {
        message.error(msg)
      })
    }
  }
  async searchCpu(sqlId){
    let paramOne = {
      instance:db.ss.get('Instance_value').split(':')[0],
      from_timestamp:this.state.startTime,
      to_timestamp:this.state.endTime,
      label:'os_cpu_user_usage',
      fetch:false
    }
    const { success, data, msg } = await getLabelData(paramOne)
    if (success) {
    let formatTimeData = [],formatTimeDataAll = [],YData = [],xAxisData = [],yAxisData = [],collectData = [],barData = [],sqlTime = [[],[],[],[],[]]
    if(this.state.selectedRow.length){
      if(this.state.isAspClick){
        collectData = await this.getCollectData(sqlId);
        sqlTime = collectData[0];
        barData = collectData[1];
        if(barData[0].length && barData[1].length){
          let selected = barData[1]
          if(selected[0] > data[0].timestamps[0] && selected[selected.length-1] < data[0].timestamps[data[0].timestamps.length-1]){
            xAxisData = [...data[0].timestamps].concat(selected);
            xAxisData = [...new Set(xAxisData)];
            xAxisData.sort((a, b) => {return a - b})
            barData[1].forEach((item,index) => {
              yAxisData.push([formatTimestamp(item).replace(' ', '\n'),barData[0][index]])
            });
          }
        } else {
          xAxisData = data[0].timestamps;
        }
      } else {
        let selected = new Date(this.state.selectedRow[0].start_time.replace(/-/g,'/')).getTime()
        if(selected > data[0].timestamps[0] && selected < data[0].timestamps[data[0].timestamps.length-1]){
          xAxisData = [...data[0].timestamps, selected];
          xAxisData = [...new Set(xAxisData)];
          xAxisData.sort((a, b) => {return a - b})
          yAxisData.push([this.state.selectedRow[0].start_time.replace(' ', '\n'),1])
        }
      }
    } else {
      if(sqlId){
        
      } else {
        xAxisData = data[0].timestamps;
      }
    }
    xAxisData.forEach(ele => {
      formatTimeDataAll.push(formatTimestamp(ele));
    });
    data[0].timestamps.forEach(ele => {
      formatTimeData.push(formatTimestamp(ele));
    });
    data[0].values.forEach((item,index) => {
      YData.push([formatTimeData[index].replace(' ', '\n'),(item*100).toFixed(2)])
    });
      this.setState({
        userData: {timestamps:formatTimeDataAll,values:YData},
        barYData: yAxisData,
        sqlData:sqlTime
      })
    } else {
      message.error(msg)
    }
  }
 async getCollectData(sqlId){
  let paramTwo = {
    unique_sql_id:sqlId,
    start_time:this.state.startTime,
    end_time:this.state.endTime
  }
    const { success, data, msg } = await getCollect(paramTwo)
    if (success) {
      let durationIndex = 7,sqlTime = [[],[],[],[],[]],barData = [[],[]]
      if(data.header){
        for(let i = 0; i < data.header.length; i++) {
          if(data.header[i] === 'duration'){
            durationIndex = i
            break;
          }
        }
        data.rows.forEach((item) => {
          if(item[durationIndex] < 0.01){
            sqlTime[0].push(item[durationIndex])
          } else if(item[durationIndex] > 0.01 && item[durationIndex] <= 0.1){
            sqlTime[1].push(item[durationIndex])
          } else if(item[durationIndex] > 0.1 && item[durationIndex] <= 1){
            sqlTime[2].push(item[durationIndex])
          } else if(item[durationIndex] > 1 && item[durationIndex] <= 5){
            sqlTime[3].push(item[durationIndex])
          } else if(item[durationIndex] > 5){
            sqlTime[4].push(item[durationIndex])
          }
        })
        barData = this.getCollectBar(data.header,data.rows)
      }
      let total = (sqlTime[0].length + sqlTime[1].length + sqlTime[2].length + sqlTime[3].length + sqlTime[4].length)/100
      if(total){
        return [[sqlTime[0].length/total,sqlTime[1].length/total,sqlTime[2].length/total,sqlTime[3].length/total,sqlTime[4].length/total],barData]
      } else {
        return [[0,0,0,0,0],barData]
      }
    } else {
      message.error(msg)
    }
  }
  getCollectBar (header,rows) {
    let startTimeIndex = 5,timeInterval = '',allTimesBar = [],oldAllTimesBarSeries = [],allTimesBarSeries = [],allTimesBarXdata = []
    if(header){
      for(let i = 0; i < header.length; i++) {
        if(header[i] === 'start_time'){
          startTimeIndex = i
          break;
        }
      }
      if(rows.length > 0 && rows.length < 12){
        timeInterval = (this.state.endTime - this.state.startTime)/rows.length
        for(let i = 0; i < rows.length; i++) {
          allTimesBar.push([this.state.startTime + timeInterval*i,this.state.startTime + timeInterval*(i+1)])
        }
      } else if(rows.length >= 12){
        timeInterval = (this.state.endTime - this.state.startTime)/12
        for(let i = 0; i < 12; i++) {
          allTimesBar.push([this.state.startTime + timeInterval*i,this.state.startTime + timeInterval*(i+1)])
        }
      } else {
        timeInterval = ''
      }
      allTimesBar.forEach((aitem,aindex) => {
        let rowsBar = 0
        rows.forEach((item) => {
          if(new Date(item[startTimeIndex]).getTime() > aitem[0] && new Date(item[startTimeIndex]).getTime() <= aitem[1]){
            oldAllTimesBarSeries[aindex] = ++rowsBar
            if(rowsBar === 1){
              allTimesBarXdata.push(new Date(item[startTimeIndex]).getTime())
            }
          }
        })
      })
      oldAllTimesBarSeries.forEach((item) => {
        if(item){
          allTimesBarSeries.push(item)
        }
      })
      return [allTimesBarSeries,allTimesBarXdata]
    }
  }
  getCpuData (dates,sqlId) {
    this.setState({
      startTime:dates ? new Date(dates[0].replace(/-/g,'/')).getTime() : this.state.startTime,
      endTime:dates ? new Date(dates[1].replace(/-/g,'/')).getTime() : this.state.endTime,
    },() => 
    this.searchCpu(sqlId))
  }
  handleAnalyzeCancel(){
    this.setState({
      isAnalyzeVisible: false,
    })
  }
  getOption = (flg) => {
    return {
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross',
          crossStyle: {
            color: '#999'
          }
        }
      },
      grid: {
        left: '1%',
        right: '1%',
        bottom: '6%',
        containLabel: true
      },
      legend: {
        data: ['cpu_usage','number of slow queries'],
        right:20
      },
      xAxis: [
        flg === "two" ? {
          type: 'category',
          axisPointer: {
            type: 'shadow'
          },
          data: this.state.userData.timestamps.map(function (str) {
            return str.replace(' ', '\n');
          })
        } : {
          type: 'category',
          axisPointer: {
            type: 'shadow',
          },
          axisLabel:{
            interval:0
          },
          data: this.state.executionTime.map(function (str) {
            return str;
          })
        },
      ],
      yAxis: [
        {
          type: 'value',
          ...this.state.isAspClick && flg !== 'two' ? yAx : '',
        },
        {
          type: 'value',
          axisLabel: {
            formatter: (val) => {
              let value = val + '%'
              return value;
            }
          }
        }
      ],
      dataZoom: flg === "two" ? [
        {
          start: 0,
          end: 100,
          show: false,
          type: 'slider',
        },
        {
            type: 'inside',
            start: 0,
            end: 100,
        }
      ]:'',
      series: [
        flg === "two" ? {
          name: 'cpu_usage',
          type: 'line',
          yAxisIndex: 1,
          itemStyle: {
              normal: {
                  color: '#ec6f1a', //改变折线点的颜色
                  lineStyle: {
                      color: '#ec6f1a' //改变折线颜色
                  }
              }
          },
          data: this.state.userData.values,
          tooltip: {
            valueFormatter: function (val) {
              let value = val + '%'
              return value;
            }
          },
          connectNulls: true
        }:'',
        {
          name: flg === 'two' ? 'number of slow queries' : '',
          type: 'bar',
          data: flg === 'two' ? this.state.barYData: this.state.sqlData,
          itemStyle: {
            color: '#5990fd'
          },
          ...this.state.isAspClick && flg !== 'two' ? 
          {tooltip: {
            valueFormatter: function (val) {
              let value = val + '%'
              return value;
            }
          }} : '',
          connectNulls: true
        }
      ]
    };
  }
  getBack(){
    this.props.getBack(this.state.proptableData,this.state.proptableHeader,this.state.propformData)
  }
  isModal(row, record) {
    this.setState({
      isAnalyzeVisible: true,
    })
    let param = {...record}
    if(this.props.formData.dataSource === 'pg_stat_activity'){
      param.db_name =  param.datname
    }
    if(record.start_time && record.finish_time){
      param.start_time = new Date(record.start_time.replace(/-/g,'/')).getTime()
      param.finish_time = new Date(record.finish_time.replace(/-/g,'/')).getTime()
    }
    this.getDiagnosis(param)
  }
  onClose(){
    this.setState({
      isOpen: false,
    })
  }
  openTemplate(row, record){
    this.setState({
      isOpen: true,
      uniqueSqlId:record.unique_sql_id
    })
  }
  handlePlanCancel = () => {
    this.setState({
      isPlanVisible: false
    })
  }
  async isPlan(item) {
    let params = {
      query:item.query,
      db_name:item.datname ? item.datname : item.db_name,
      schema_name:''
    }
    const { success, data, msg } = await getExecutionPlan(params)
    if (success) {
      this.setState(() => ({
        isPlanVisible: true,
        planData: data
      }))
    } else {
      this.setState({
        planData: ''
      })
      message.error(msg)
    }
  }
  componentDidMount () {
    this.getCpuData ()
    let historyColumObj = {},newHeader = [...this.props.tableHeader]
    historyColumObj = {
      title: 'operation',
      dataIndex: 'operation',
      width: 180,
      key: 'operation',
      ellipsis: true,
      align:'center',
      fixed:'right',
      render: (row, record) => {
        return <div>
          <img src={this.state.isAspClick ? Detail : Analyze} title={this.state.isAspClick ? 'Detail' : 'Analyze'} disabled alt="" onClick={(e) => {e.stopPropagation();this.state.isAspClick ? this.openTemplate(row, record) : this.isModal(row, record)}} ></img>
          <img style={{marginLeft:10}} src={ExecutionPlan} title='ExecutionPlan' disabled alt="" onClick={(e) => {e.stopPropagation();this.isPlan(record)}} ></img>
          </div>
      }
    }
    newHeader.push(historyColumObj)
    if(this.props.formData.dataSource === 'asp' && this.props.tableHeader[1].title === 'datname'){
      this.setState({
        isAspClick: true,
      })
    }
    this.setState({
      dataSource: this.props.tableData,
      columns: newHeader,
      proptableData: this.props.tableData,
      proptableHeader: this.props.tableHeader,
      propformData: this.props.formData,
  })
  }
  render () {
    return (
      <div className='SlowSqlDiagnosis'>
        <p>
        <Radio.Group
          value={this.state.checkType}
          onChange={(val) => {
            this.changeTypeVal(val);
          }}
        >
          <Radio.Button value={5}>Last 5min</Radio.Button>
          <Radio.Button value={10}>Last 10min</Radio.Button>
          <Radio.Button value={60}>Last 1h</Radio.Button>
          <Radio.Button value={180}>Last 3h</Radio.Button>
          <Radio.Button value={1440}>Last 24h</Radio.Button>
          <Radio.Button value={4320}>Last 3days</Radio.Button>
        </Radio.Group>
          <RangePicker
            format="YYYY-MM-DD HH:mm:ss"
            value={[
              moment(this.state.startTime),
              moment(this.state.endTime),
            ]}
            style={{ marginLeft: 10 }}
            showTime
            onChange={(dates,dateStrings) => this.getCpuData(dateStrings,this.state.selectedRow[0].unique_sql_id)}
          />
          <div
          className="buttonstyle"
          style={{ textAlign: "right", float: "right" }}
          >
            <img
              src={GetBack}
              alt=""
              onClick={() => this.getBack()}
            ></img>
        </div>
        </p>
        {this.state.isAspClick ? 
          <Row gutter={10} className='mb-10'>
            <Col span={this.state.selectedRow.length === 0 ? 24 : 16}>
              <Card title="SQL Statistics">
                <ReactEcharts
                    ref={(e) => {
                      this.echartsElement = e
                    }}
                    option={this.getOption('two')}
                    style={{ height: 300 }}
                    notMerge={true}
                    lazyUpdate={true}
                  >
                </ReactEcharts>
              </Card>
            </Col>
            {this.state.selectedRow.length === 1 ? <Col span={8}>
              <Card title="Segment time-consuming SQL distribution">
                <ReactEcharts
                  ref={(e) => {
                    this.echartsElement = e
                  }}
                  option={this.getOption('one')}
                  style={{ height: 300 }}
                  notMerge={true}
                  lazyUpdate={true}
                >
                </ReactEcharts>
              </Card>
            </Col> : ''}
          </Row> : <Row gutter={10} className='mb-10'>
              <Col span={24}>
                <Card title="SQL Statistics">
                  <ReactEcharts
                      ref={(e) => {
                        this.echartsElement = e
                      }}
                      option={this.getOption('two')}
                      style={{ width: '100%', height: 300 }}
                      notMerge={true}
                      lazyUpdate={true}
                    >
                  </ReactEcharts>
                </Card>
              </Col>
        </Row>} 
        <Card title="SQL List" style={{ minHeight: 570 }} >
          <Table rowSelection={{type: 'radio',selectedRowKeys:this.state.selectedRowKeys}}
          onRow={(record, index) => ({onClick: () => this.onRowClick(record, index)})} size="small" bordered dataSource={this.state.dataSource} columns={this.state.columns} rowKey={record => record.key} loading={this.state.loading} scroll={{ x: '100%' }} />
        </Card>
        <Modal title="Diagnosis" width="40vw" footer={null} destroyOnClose='true' visible={this.state.isAnalyzeVisible} maskClosable = {false} onCancel={() => this.handleAnalyzeCancel()} >
          <p style={{fontWeight:'bold'}}>Implementation plan</p>
          <TextArea rows={8} value={this.state.textAreaData} disabled />
          <Table size='small' style={{ Maxheight: 80 }} bordered dataSource={this.state.rootCauseDataSource} columns={this.state.rootCausecolumns} rowKey={record => record.key} loading={this.state.loading} />
        </Modal>
        <Modal title={`Execution Plan (SQL: ${this.state.planData[1]} , schame: ${this.state.planData[2]})`} style={{maxWidth: "60vw"}} bodyStyle={{overflowY: "auto",height:600,background: '#f1f1f1'}} width="60vw" okButtonProps={{ style: { display: 'none' } }} 
         destroyOnClose={true} visible={this.state.isPlanVisible} maskClosable = {false} centered='true' onCancel={() => this.handlePlanCancel()}>
           <SqlPlan planData = {this.state.planData[3]} />
        </Modal>
        <Drawer placement="right" closable={false} width={'50%'} getContainer={false} destroyOnClose={true}  maskClosable={false} onClose={() => this.onClose()}  open={this.state.isOpen}>
          <DrawerInfo onClose={this.onClose.bind(this)} isModal={this.isModal.bind(this)} startTime={this.state.startTime} endTime={this.state.endTime} uniqueSqlId={this.state.uniqueSqlId}  />
        </Drawer>
      </div>
    )
  }
}