import React, { Component } from 'react';
import { Col, Row } from 'antd';
import { Empty, message } from 'antd';
import icon9 from '../../assets/imgs/icon9.png';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import { getDBMemoryData } from '../../api/autonomousManagement';

export default class DBMemory extends Component {
  constructor(props) {
    super(props)
    this.state = {
      chartData1: {},
      chartData2: {},
      chartData3: {},
      chartData4: {},
      selValue:this.props.selValue,
      selTimeValue:this.props.selTimeValue,
    }
  }
  async getDBMemoryData1 () {
    let param = {
      instance:this.state.selValue,
      minutes:0,
      label:'max_dynamic_memory',
    }
    const { success, data, msg }= await getDBMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getDBMemoryData2 () {
    let param = {
      instance:this.state.selValue,
      minutes:0,
      label:'max_shared_memory',
    }
    const { success, data, msg }= await getDBMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getDBMemoryData3 () {
    let param = {
      instance:this.state.selValue,
      minutes:0,
      label:'max_process_memory',
    }
    const { success, data, msg }= await getDBMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getDBMemoryData4 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'dynamic_used_memory',
    }
    const { success, data, msg }= await getDBMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getDBMemoryData5 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'dynamic_peak_memory',
    }
    const { success, data, msg }= await getDBMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getDBMemoryData6 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'dynamic_used_shrctx',
    }
    const { success, data, msg }= await getDBMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getDBMemoryData7 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'dynamic_peak_shrctx',
    }
    const { success, data, msg }= await getDBMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getDBMemoryData8 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'shared_used_memory',
    }
    const { success, data, msg }= await getDBMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getDBMemoryData9 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'process_used_memory',
    }
    const { success, data, msg }= await getDBMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getDBMemoryData10 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'other_used_memory',
    }
    const { success, data, msg }= await getDBMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  divisionItem(arr1, arr2) {
    let newArr = arr1.map(function (item, index) {
      return item/arr2[index];
    });
    return newArr;
  }
async getDBMemoryDataAll () {
  Promise.all([
    this.getDBMemoryData1(),
    this.getDBMemoryData2(),
    this.getDBMemoryData3(),
    this.getDBMemoryData4(),
    this.getDBMemoryData5(),
    this.getDBMemoryData6(),
    this.getDBMemoryData7(),
    this.getDBMemoryData8(),
    this.getDBMemoryData9(),
    this.getDBMemoryData10()
  ]).then((result)=>{
    if(result[0]){
      let newResult = [],xDataArray = [[],[],[],[],[],[],[]],yDataArray = [[],[],[],[],[],[],[]]
      newResult = [result[3],result[4],result[5],result[6],result[7],result[8],result[9]]
      newResult.forEach((item,index) => {
        xDataArray[index] = item[0].timestamps
      });
      newResult.forEach((item,index) => {
        item[0].values.forEach((oitem) => {
          yDataArray[index].push(oitem)
        });
      });
      let data1 = {'legend':[{image:'',description:'Used Memory'},{image:'',description:'peak_memory'},{image:'',description:'used_shrctx'},{image:'',description:'peak_shrctx'}],'xAxisData':xDataArray[0],'seriesData':[{data:yDataArray[0],description:'used_memory',colors:'#2DA769'},{data:yDataArray[1],description:'peak_memory',colors:'#EC6F1A'},{data:yDataArray[2],description:'used_shrctx',colors:'#EEBA18'},{data:yDataArray[3],description:'peak_shrctx',colors:'#5890FD'}],'flg':0,'legendFlg':2,title:["Max Dynamic Memory",result[0][0].values[0]+'GB'],'unit':'','fixedflg':0}
      let data2 = {'legend':[{image:'',description:'Shared Memory'}],'xAxisData':xDataArray[4],'seriesData':[{data:yDataArray[4],description:'Shared Memory',colors:'#EEBA18'}],'flg':0,'legendFlg':2,title:['Max Shared Memory',result[1][0].values[0]+'GB'],'unit':'','fixedflg':0}
      let data3 = {'legend':[{image:'',description:'Process Memory'}],'xAxisData':xDataArray[5],'seriesData':[{data:yDataArray[5],description:'Process Memory',colors:'#EC6F1A'}],'flg':0,'legendFlg':2,title:['Max Process Memory',result[2][0].values[0]+'GB'],'unit':'','fixedflg':0}
      let data4 = {'legend':[{image:'',description:'Used Memory'}],'xAxisData':xDataArray[6],'seriesData':[{data:yDataArray[6],description:'Used Memory',colors:'#2070F3'}],'flg':0,'legendFlg':2,title:'Other Used Memory','unit':'','fixedflg':0}
      this.setState({
        chartData1: data1,
        chartData2: data2,
        chartData3: data3,
        chartData4: data4,
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
        if(this.props.tabkey === "7"){
          this.getDBMemoryDataAll()
        }
      })
    }
  }
  componentDidMount () {
    this.getDBMemoryDataAll()
  }
  render() {
    return (
      <div>
        <Row gutter={[10, 10]}>
          <Col className="gutter-row cpuborder" style={{display:'block',textAlign:'right'}} span={24}>
            <span><img src={icon9} alt="" className='iconstyle'></img></span>
          </Col>
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
