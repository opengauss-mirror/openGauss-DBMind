import React, { Component } from 'react';
import { Col, Row, message, Collapse } from 'antd';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import { getServiceCapabilityData } from '../../api/autonomousManagement';

const { Panel } = Collapse;
export default class DBServiceCapability extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selValue:this.props.selValue,
      selTimeValue:this.props.selTimeValue,
      primitiveDataAll:[],
      serviceAllData:[],
      vectorKey:''
    }
  }
  async getServiceData1 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_db_xact_commit',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getServiceData2 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_db_xact_rollback',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getServiceData3 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_db_conflicts',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getServiceData4 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_db_confl_lock',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getServiceData5 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_db_confl_snapshot',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getServiceData6 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_db_confl_bufferpin',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getServiceData7 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_db_confl_deadlock',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getServiceData8 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_db_deadlocks',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getServiceData9 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_db_temp_bytes',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getServiceData10 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_db_temp_files',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getServiceData11 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'gaussdb_tup_inserted_rate',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getServiceData12 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'gaussdb_tup_deleted_rate',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getServiceData13 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'gaussdb_tup_updated_rate',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getServiceData14 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'gaussdb_tup_fetched_rate',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  compare(property){
    return function(a,b){
        var value1 = a.labels[property];
        var value2 = b.labels[property];
        return value1 - value2;
    }
  }
  additionItem(arr1, arr2) {
    let newArr = arr1.map(function (item, index) {
      return item + arr2[index];
    });
    return newArr;
  }
  divisionItem(arr1, arr2) {
    let newArr = arr1.map(function (item, index) {
      return item/(item + arr2[index]);
    });
    return newArr;
  }
async getServiceDataAll () {
  Promise.all([
    this.getServiceData1(),
    this.getServiceData2(),
    this.getServiceData3(),
    this.getServiceData4(),
    this.getServiceData5(),
    this.getServiceData6(),
    this.getServiceData7(),
    this.getServiceData8(),
    this.getServiceData9(),
    this.getServiceData10(),
    this.getServiceData11(),
    this.getServiceData12(),
    this.getServiceData13(),
    this.getServiceData14()
  ]).then((result)=>{
    if(result[0]){
      result.forEach((item,index) => {
        item.sort(this.compare('datname'))
      });
      
      let newResult = [],totalArrayData = [],successRateArrayData = [],failureRateArrayData = []
      result[0].forEach((item,index) => {
        let lengthDiff = 0
        totalArrayData.push(JSON.parse(JSON.stringify(item)))
        successRateArrayData.push(JSON.parse(JSON.stringify(item)))
        failureRateArrayData.push(JSON.parse(JSON.stringify(item)))
        if(result[0][index].values.length > result[1][index].values.length){
          lengthDiff = result[0][index].values.length - result[1][index].values.length
          result[0][index].values.length.splice(0,lengthDiff);
          totalArrayData[index].timestamps = result[1][index].timestamps
        } else if(result[0][index].values.length < result[1][index].values.length){
          lengthDiff = result[1][index].values.length - result[0][index].values.length
          result[1][index].values.length.splice(0,lengthDiff);
          totalArrayData[index].timestamps = result[0][index].timestamps
        }
        totalArrayData[index].values= this.additionItem(result[0][index].values, result[1][index].values);
        successRateArrayData[index].values= this.divisionItem(result[0][index].values, result[1][index].values);
        failureRateArrayData[index].values= this.divisionItem(result[1][index].values, result[0][index].values);
      });
      newResult = [result[0],result[1],totalArrayData,successRateArrayData,failureRateArrayData,result[2],result[3],result[4],result[5],result[6],result[7],result[8],result[9],result[10],result[11],result[12],result[13]]
      let primitiveDataAll = [],serviceAllArray = []
      result[0].forEach((item,index) => {
        let DataItems = []
        newResult.forEach((oitem,oindex) => {
          //根据不同的接口数据index进行特异性处理
          // oitem[index].values.forEach((pitem,pindex) => {
          //   oitem[index].values[pindex] = (pitem/1024/1024/1024).toFixed(2)
          // });
          DataItems.push(oitem[index])
      });
      primitiveDataAll.push(DataItems)
      });
      primitiveDataAll.forEach((item,index) => {
              let chartData = []
              let data1 = {'legend':[{image: '', description:'Success'},{image: '', description: 'Failure'},{image: '', description: 'Total'}],'xAxisData':item[0].timestamps,'seriesData':[{data:item[0].values,description: 'Success', colors: '#2DA769'}, { data:item[1].values, description: 'Failure', colors: '#F43146'}, { data:item[2].values, description: 'Total', colors: '#5990FD'}],'flg':0,'legendFlg':2,title:'Success/Failure/Total Transactions','unit':'','fixedflg':0}
              let data2 = {'legend':[{image: '', description:'Success'},{image: '', description: 'Failure'}],'xAxisData':item[3].timestamps,'seriesData':[{data:item[3].values,description: 'Success', colors: '#2DA769'}, { data:item[4].values, description: 'Failure', colors: '#5990FD'}],'flg':1,'legendFlg':2,title:"Transaction Success/Failure Rate",'unit':'%','fixedflg':1}
              let data3 = {'legend':[{image: '', description: 'Conflicts'},{image: '', description: 'Confl Lock'},{image: '', description: 'Confl Snapshot'},{image: '', description: 'Confl Bufferpin'},{image: '', description: 'Confl Deadlock'}],'xAxisData':item[5].timestamps,'seriesData':[{data:item[5].values,description: 'Conflicts', colors: '#2DA769'},{data:item[6].values, description: 'Confl Lock', colors: '#F43146'},{data:item[7].values, description: 'Confl Snapshot', colors: '#5990FD'},{data:item[8].values, description: 'Confl Bufferpin', colors: '#EEBA18'},{data:item[9].values, description: 'Confl Deadlock', colors: '#9185F0'}],'flg':0,'legendFlg':2,title:"Conflicts Rate",'unit':'','fixedflg':0}
              let data4 = {'legend':[{image: '', description: 'Deadlock Rate'}],'xAxisData':item[10].timestamps,'seriesData':[{data:item[10].values,description: 'Deadlock Rate', colors: '#EEBA18'}],'flg':0,'legendFlg':2,title:"Deadlock Rate",'unit':'','fixedflg':0}
              let data5 = {'legend':[{image: '', description: 'Temp Files'},{image: '', description: 'Temp Bytes'}],'xAxisData':item[11].timestamps,'seriesData':[{data:item[12].values,description: 'Temp Files', colors: '#EC6F1A'},{data:item[11].values,description: 'Temp Bytes', colors: '#9185EF'}],'flg':0,'legendFlg':2,title:"Temp File",'unit':'','fixedflg':0}
              let data6 = {'legend':[{image: '', description:'Insert'},{image: '', description: 'Delete'},{image: '', description: 'Update'},{image: '', description: 'Select'}],'xAxisData':item[13].timestamps,'seriesData':[{data:item[13].values,description: 'Insert', colors: '#2DA769'},{ data:item[14].values, description: 'Delete', colors: '#F43146'},{ data:item[15].values, description: 'Update', colors: '#5990FD'},{ data:item[16].values, description: 'Select', colors: '#EEBA18'}],'flg':1,'legendFlg':2,title:"Averaged Rate Of DML",'unit':'%','fixedflg':1}
              chartData.push(data1,data2,data3,data4,data5,data6)
              serviceAllArray.push(chartData)
            })
          this.setState(() => ({
            serviceAllData: serviceAllArray,
            primitiveDataAll:primitiveDataAll,
            vectorKey:0,
          }),()=>{
            this.onChange(0)
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
        if(this.props.tabkey === "1"){
          this.getServiceDataAll()
        }
      })
    }
  }
  componentDidMount() {
    this.getServiceDataAll()
  }
  onChange = (key) => {
    this.setState({vectorKey:key})
  };
  render() {
    return (
      <div>
          {
            this.state.serviceAllData.length > 0 ? this.state.serviceAllData.map((item,index) => {
              return (
                <Collapse  activeKey={this.state.vectorKey}  onChange={(key)=>{this.onChange(key)}} expandIconPosition='end' >
                <Panel header={this.state.primitiveDataAll[index][0].labels.datname} key={index} forceRender={true} className='panelStyle'>
                <Row gutter={[10, 10]}>
                  <Col className="gutter-row cpuborder" span={12}>
                    <NodeEchartFormWork echartData={item[0]} />
                  </Col>
                  <Col className="gutter-row cpuborder" span={12}>
                    <NodeEchartFormWork echartData={item[1]} />
                  </Col>
                  <Col className="gutter-row cpuborder" span={12}>
                    <NodeEchartFormWork echartData={item[2]} />
                  </Col>
                  <Col className="gutter-row cpuborder" span={12}>
                    <NodeEchartFormWork echartData={item[3]} />
                  </Col>
                  <Col className="gutter-row cpuborder" span={12}>
                    <NodeEchartFormWork echartData={item[4]} />
                  </Col>
                  <Col className="gutter-row cpuborder" span={12}>
                    <NodeEchartFormWork echartData={item[5]} />
                  </Col>
                </Row>
              </Panel>
              </Collapse>
                )

            }):''
          }
      </div>
    )
  }
}
