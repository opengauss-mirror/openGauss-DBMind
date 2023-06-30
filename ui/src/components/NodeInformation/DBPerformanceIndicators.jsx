import React, { Component } from 'react';
import { Col, Row } from 'antd';
import { Empty, message } from 'antd';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import { getServiceCapabilityData } from '../../api/autonomousManagement';

export default class DBPerformanceIndicators extends Component {
  constructor(props) {
    super(props)
    this.state = {
      chartData1: {},
      chartData2: {},
      chartData3: {},
      chartData4: {},
      chartData5: {},
      selValue:this.props.selValue,
      selTimeValue:this.props.selTimeValue,
    }
  }
  async getPerformanceData1 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'gaussdb_total_connection',
      fetch:false
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getPerformanceData2 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'gaussdb_active_connection',
      fetch:false
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getPerformanceData3 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'gaussdb_idle_connection',
      fetch:false
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getPerformanceData4 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_sql_count_insert',
      fetch:false
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getPerformanceData5 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_sql_count_update',
      fetch:false
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getPerformanceData6 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_sql_count_delete',
      fetch:false
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getPerformanceData7 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_sql_count_select',
      fetch:false
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getPerformanceData8 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'statement_responsetime_percentile_p80',
      fetch:false
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getPerformanceData9 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'statement_responsetime_percentile_p95',
      fetch:false
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getPerformanceData10 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'gaussdb_qps_by_instance',
      fetch:false
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
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
    this.getPerformanceData6(),
    this.getPerformanceData7(),
    this.getPerformanceData8(),
    this.getPerformanceData9(),
    this.getPerformanceData10()
  ]).then((result)=>{
    if(result[0]){
      let newResult = [],activeRateData = [],waitingRateData = [],xDataArray = [[],[],[],[],[],[],[],[],[],[],[],[]],yDataArray = [[],[],[],[],[],[],[],[],[],[],[],[]]
      activeRateData = JSON.parse(JSON.stringify(result[1]))
      waitingRateData = JSON.parse(JSON.stringify(result[2]))
      activeRateData[0].values= this.divisionItem(result[1][0].values, result[0][0].values);
      waitingRateData[0].values= this.divisionItem(result[2][0].values, result[0][0].values);
      newResult = [result[0],result[1],result[2],activeRateData,waitingRateData,result[3],result[4],result[5],result[6],result[7],result[8],result[9]]
      newResult.forEach((item,index) => {
        xDataArray[index] = item[0].timestamps
      });
      newResult.forEach((item,index) => {
        item[0].values.forEach((oitem) => {
          yDataArray[index].push(oitem)
        });
      });
      let data1 = {'legend':[{image:'',description:'Sessions'},{image:'',description:'Active Session'},{image:'',description:'Waiting Session'}],'xAxisData':xDataArray[0],'seriesData':[{data:yDataArray[0],description:'Sessions',colors:'#5990FD'},{data:yDataArray[1],description:'Active Session',colors:'#EEBA18'},{data:yDataArray[2],description:'Waiting Session',colors:'#9185F0'}],'flg':0,'legendFlg':2,title:"Sessions/Active Sessions/Waiting Sessions",'unit':'','fixedflg':0}
      let data2 = {'legend':[{image:'',description:'Active'},{image:'',description:'Waiting'}],'xAxisData':xDataArray[3],'seriesData':[{data:yDataArray[3],description:'Active',colors:'#EEBA18'},{data:yDataArray[4],description:'Waiting',colors:'#9185F0'}],'flg':1,'legendFlg':2,title:'Active Session Rate/Waiting Session Rate','unit':'%','fixedflg':0}
      let data3 = {'legend':[{image:'',description:'Insert'},{image:'',description:'Update'},{image:'',description:'Delete'},{image:'',description:'Select'}],'xAxisData':xDataArray[5],'seriesData':[{data:yDataArray[5],description:'Insert',colors:'#EC6F1A'},{data:yDataArray[6],description:'Update',colors:'#9185F0'},{data:yDataArray[7],description:'Delete',colors:'#2DA769'},{data:yDataArray[8],description:'Select',colors:'#EEBA18'}],'flg':0,'legendFlg':2,title:'Insert/Update/Delete/Select','unit':'','fixedflg':0}
      let data4 = {'legend':[{image:'',description:'80% SQL Response Time'},{image:'',description:'95% SQL Response Time'}],'xAxisData':xDataArray[9],'seriesData':[{data:yDataArray[9],description:'80% SQL Response Time',colors:'#2070F3'},{data:yDataArray[10],description:'95% SQL Response Time',colors:'#9185F0'}],'flg':0,'legendFlg':2,title:'SQL Response Time (/ms)','unit':'','fixedflg':0}
      let data5 = {'legend':[{image:'',description:'Tps'}],'xAxisData':xDataArray[11],'seriesData':[{data:yDataArray[11],description:'Tps',colors:'#EEBA18'}],'flg':0,'legendFlg':2,title:'TPS','unit':'/s','fixedflg':0}
      this.setState({
        chartData1: data1,
        chartData2: data2,
        chartData3: data3,
        chartData4: data4,
        chartData5: data5,
      })
    }
    }).catch((error) => {
      console.log('error', error)
    })
  }
  componentDidUpdate(prevProps) {
    if(prevProps.selValue !== this.props.selValue || prevProps.selTimeValue !== this.props.selTimeValue || prevProps.tabkey !== this.props.tabkey) {
      this.setState(() => ({
        selValue: this.props.selValue,selTimeValue: this.props.selTimeValue
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
          <Col className="gutter-row cpuborder" span={24}>
            <NodeEchartFormWork echartData={this.state.chartData5} />
          </Col>
        </Row>
      </div>
    )
  }
}
