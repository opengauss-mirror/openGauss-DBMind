import React, { Component } from 'react';
import { Col, Row } from 'antd';
import { Empty, message } from 'antd';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import { getMetric } from '../../api/autonomousManagement';

export default class DBPerformanceIndicators extends Component {
  constructor(props) {
    super(props)
    this.state = {
      chartData1: {},
      chartData2: {},
      chartData3: {},
      chartData4: {},
      selValue:this.props.selValue,
      selTimeValue:this.props.selTimeValue,
      startTime:this.props.startTime,
      endTime:this.props.endTime
    }
  }
  async getPerformanceData1 () {
    let param = {
      instance:this.state.selValue,
      latest_minutes:this.state.selTimeValue ? this.state.selTimeValue : null,
      label:'gaussdb_total_connection',
      fetch_all:false,
      regex:false,
      from_timestamp:this.state.startTime ? this.state.startTime : null,
      to_timestamp:this.state.endTime ? this.state.endTime : null
    }
    const { success, data, msg }= await getMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getPerformanceData2 () {
    let param = {
      instance:this.state.selValue,
      latest_minutes:this.state.selTimeValue ? this.state.selTimeValue : null,
      label:'gaussdb_active_connection',
      fetch_all:false,
      regex:false,
      from_timestamp:this.state.startTime ? this.state.startTime : null,
      to_timestamp:this.state.endTime ? this.state.endTime : null
    }
    const { success, data, msg }= await getMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getPerformanceData3 () {
    let param = {
      instance:this.state.selValue,
      latest_minutes:this.state.selTimeValue ? this.state.selTimeValue : null,
      label:'gaussdb_idle_connection',
      fetch_all:false,
      regex:false,
      from_timestamp:this.state.startTime ? this.state.startTime : null,
      to_timestamp:this.state.endTime ? this.state.endTime : null
    }
    const { success, data, msg }= await getMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getPerformanceData4 () {
    let param = {
      instance:this.state.selValue,
      latest_minutes:this.state.selTimeValue ? this.state.selTimeValue : null,
      label:'statement_responsetime_percentile_p80',
      fetch_all:false,
      regex:false,
      from_timestamp:this.state.startTime ? this.state.startTime : null,
      to_timestamp:this.state.endTime ? this.state.endTime : null
    }
    const { success, data, msg }= await getMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getPerformanceData5 () {
    let param = {
      instance:this.state.selValue,
      latest_minutes:this.state.selTimeValue ? this.state.selTimeValue : null,
      label:'statement_responsetime_percentile_p95',
      fetch_all:false,
      regex:false,
      from_timestamp:this.state.startTime ? this.state.startTime : null,
      to_timestamp:this.state.endTime ? this.state.endTime : null
    }
    const { success, data, msg }= await getMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getPerformanceData6 () {
    let param = {
      instance:this.state.selValue,
      latest_minutes:this.state.selTimeValue ? this.state.selTimeValue : null,
      label:'gaussdb_qps_by_instance',
      fetch_all:false,
      regex:false,
      from_timestamp:this.state.startTime ? this.state.startTime : null,
      to_timestamp:this.state.endTime ? this.state.endTime : null
    }
    const { success, data, msg }= await getMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  divisionItem(arr1, arr2) {
    let newArr = arr1.map(function (item, index) {
      if(arr2[index]){
        return item/arr2[index];
      } else {
        return 0;
      }
    });
    return newArr;
  }
async getPerformanceDataAll () {
  Promise.all([
    this.getPerformanceData1(),
    this.getPerformanceData2(),
    this.getPerformanceData3(),
    this.getPerformanceData4(),
    this.getPerformanceData5(),
    this.getPerformanceData6()
  ]).then((result)=>{
    if(result[0]){
      let newResult = [],activeRateData = [],waitingRateData = [],xDataArray = [[],[],[],[],[],[],[],[]],yDataArray = [[],[],[],[],[],[],[],[]]
      activeRateData = JSON.parse(JSON.stringify(result[1]))
      waitingRateData = JSON.parse(JSON.stringify(result[2]))
      activeRateData[0].values= this.divisionItem(result[1][0].values, result[0][0].values);
      waitingRateData[0].values= this.divisionItem(result[2][0].values, result[0][0].values);
      newResult = [result[0],result[1],result[2],activeRateData,waitingRateData,result[3],result[4],result[5]]
      newResult.forEach((item,index) => {
        xDataArray[index] = item[0].timestamps
      });
      newResult.forEach((item,index) => {
        item[0].values.forEach((oitem) => {
          yDataArray[index].push(oitem)
        });
      });
      let data1 = {'legend':[{image:'',description:'Sessions'},{image:'',description:'Active Session'},{image:'',description:'Waiting Session'}],'xAxisData':xDataArray[0],'seriesData':[{data:yDataArray[0],description:'Sessions',colors:'#5990FD'},{data:yDataArray[1],description:'Active Session',colors:'#EEBA18'},{data:yDataArray[2],description:'Waiting Session',colors:'#9185F0'}],'flg':0,'legendFlg':2,title:"Sessions/Active Sessions/Waiting Sessions",'unit':'','fixedflg':0,'toolBox':true}
      let data2 = {'legend':[{image:'',description:'Active'},{image:'',description:'Waiting'}],'xAxisData':xDataArray[3],'seriesData':[{data:yDataArray[3],description:'Active',colors:'#EEBA18'},{data:yDataArray[4],description:'Waiting',colors:'#9185F0'}],'flg':1,'legendFlg':2,title:'Active Session Rate/Waiting Session Rate','unit':'%','fixedflg':0,'toolBox':true}
      let data3 = {'legend':[{image:'',description:'80% SQL Response Time'},{image:'',description:'95% SQL Response Time'}],'xAxisData':xDataArray[5],'seriesData':[{data:yDataArray[5],description:'80% SQL Response Time',colors:'#2070F3'},{data:yDataArray[6],description:'95% SQL Response Time',colors:'#9185F0'}],'flg':0,'legendFlg':2,title:'SQL Response Time (/ms)','unit':'','fixedflg':0,'toolBox':true}
      let data4 = {'legend':[{image:'',description:'Tps'}],'xAxisData':xDataArray[7],'seriesData':[{data:yDataArray[7],description:'Tps',colors:'#EEBA18'}],'flg':0,'legendFlg':2,title:'TPS','unit':'/s','fixedflg':0,'toolBox':true}
      this.setState({
        chartData1: data1,
        chartData2: data2,
        chartData3: data3,
        chartData4: data4
      })
    }
    }).catch((error) => {
      console.log('error', error)
    })
  }
  componentDidUpdate(prevProps) {
    if(prevProps.selValue !== this.props.selValue || prevProps.selTimeValue !== this.props.selTimeValue || prevProps.startTime !== this.props.startTime || prevProps.endTime !== this.props.endTime || prevProps.tabkey !== this.props.tabkey) {
      this.setState(() => ({
        selValue: this.props.selValue,selTimeValue: this.props.selTimeValue,startTime: this.props.startTime,endTime: this.props.endTime
      }),()=>{
        if(this.props.tabkey === "2"){
          this.getPerformanceDataAll()
        }
      })
    }
  }
  componentDidMount () {
    this.getPerformanceDataAll()
  }
  render() {
    return (
      <div>
        <Row gutter={[10, 10]}>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork echartData={this.state.chartData1} />
          </Col>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork echartData={this.state.chartData2} />
          </Col>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork echartData={this.state.chartData3} />
          </Col>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork echartData={this.state.chartData4} />
          </Col>
        </Row>
      </div>
    )
  }
}
