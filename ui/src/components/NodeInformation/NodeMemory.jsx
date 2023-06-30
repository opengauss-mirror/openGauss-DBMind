import React, { Component } from 'react';
import { Col, Row } from 'antd';
import Used from '../../assets/imgs/Used.png';
import Available from '../../assets/imgs/Available.png';
import Buffer from '../../assets/imgs/Buffer.png';
import Cache from '../../assets/imgs/Cache.png';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import { getMemoryData } from '../../api/autonomousManagement';

export default class NodeMemory extends Component {
  constructor(props) {
    super(props)
    this.state = {
      ifShow: false,
      chartData1: {},
      chartData2: {},
      chartData3: {},
      chartData4: {},
      chartData5: {},
      selValue:this.props.selValue,
      selTimeValue:this.props.selTimeValue,
      ehcartLeft1:0,
      ehcartLeft2:0,
      ehcartLeft3:0,
      ehcartLeft4:0,
      ehcartRight1:0,
      ehcartRight2:0,
      spaceRight1:0,
      spaceRight2:0,
      spaceLeft1:0,
      spaceLeft2:0,
    }
  }
  async getMemoryData1 () {
    let param = {
      instance:this.state.selValue,
      minutes:0,
      label:'node_memory_MemTotal_bytes',
      fetch:false
    }
    const { success, data, msg }= await getMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getMemoryData2 () {
    let param = {
      instance:this.state.selValue,
      minutes:0,
      label:'node_memory_MemAvailable_bytes',
      fetch:false
    }
    const { success, data, msg }= await getMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getMemoryData3 () {
    let param = {
      instance:this.state.selValue,
      minutes:0,
      label:'node_memory_SwapTotal_bytes',
      fetch:false
    }
    const { success, data, msg }= await getMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getMemoryData4 () {
    let param = {
      instance:this.state.selValue,
      minutes:0,
      label:'node_memory_SwapFree_bytes',
      fetch:false
    }
    const { success, data, msg }= await getMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getMemoryData5 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_mem_usage',
      fetch:false
    }
    const { success, data, msg }= await getMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getMemoryData6 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'node_memory_MemAvailable_bytes',
      fetch:false
    }
    const { success, data, msg }= await getMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getMemoryData7 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'node_memory_Buffers_bytes',
      fetch:false
    }
    const { success, data, msg }= await getMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getMemoryData8 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'node_memory_Cached_bytes',
      fetch:false
    }
    const { success, data, msg }= await getMemoryData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
async getMemoryDataAll () {
  Promise.all([
    this.getMemoryData1(),
    this.getMemoryData2(),
    this.getMemoryData3(),
    this.getMemoryData4(),
    this.getMemoryData5(),
    this.getMemoryData6(),
    this.getMemoryData7(),
    this.getMemoryData8()
  ]).then((result)=>{
    if(result[0]){
      let totalRight = result[2][0].values[0] + result[3][0].values[0],totalLeft = [],usageData = [],usageValues = []
      result[4][0].values.forEach((oitem) => {
        usageValues.push(result[0][0].values[0]*oitem)
      });
      usageData = JSON.parse(JSON.stringify(result[4]))
      usageData[0].values = usageValues
      let allChartArray = [usageData,result[5],result[6],result[7],result[4]],xDataArray = [[],[],[],[],[]],yDataArray = [[],[],[],[],[]]
      allChartArray.forEach((item,index) => {
        xDataArray[index] = item[0].timestamps
      });
      allChartArray.forEach((item,index) => {
        item[0].values.forEach((oitem) => {
          if(index === 4){
            yDataArray[index].push(oitem)
          } else {
            yDataArray[index].push((oitem/1024/1024/1024).toFixed(2))
          }
        });
      });
      yDataArray.forEach((oitem) => {
        totalLeft.push(Number(oitem[oitem.length-1]))
      });
      let data1 = {'legend':[{image: Used, description: 'Used Space(GB)'}],'xAxisData':xDataArray[0],'seriesData':[{data:yDataArray[0],description: 'Used Space(GB)', colors: '#EC6F1A'}],'flg':0,'legendFlg':1,'unit':'GB','fixedflg':0}
      let data2 = {'legend':[{image: Available, description: 'Available Space'}],'xAxisData':xDataArray[1],'seriesData':[{data:yDataArray[1],description: 'Available Space', colors: '#2DA769'}],'flg':0,'legendFlg':1,'unit':'GB','fixedflg':0}
      let data3 = {'legend':[{image: Buffer, description: 'Buffer Space'}],'xAxisData':xDataArray[2],'seriesData':[{data:yDataArray[2],description: 'Buffer Space', colors: '#9185F0'}],'flg':0,'legendFlg':1,'unit':'GB','fixedflg':0}
      let data4 = {'legend':[{image: Cache, description: 'Cache Space'}],'xAxisData':xDataArray[3],'seriesData':[{data:yDataArray[3],description: 'Cache Space', colors: '#EEBA18'}],'flg':0,'legendFlg':1,'unit':'GB','fixedflg':0}
      let data5 = {'legend':[{image: Used, description: 'Used Space(%)'}],'xAxisData':xDataArray[4],'seriesData':[{data:yDataArray[4],description: 'Used Space(%)', colors: '#EC6F1A'}],'flg':1,'legendFlg':1,'unit':'%','fixedflg':0}
      this.setState({
        chartData1: data1,
        chartData2: data2,
        chartData3: data3,
        chartData4: data4,
        chartData5: data5,
        ehcartLeft1:totalLeft[0]+totalLeft[1]+totalLeft[2]+totalLeft[3] ? totalLeft[0]/(totalLeft[0]+totalLeft[1]+totalLeft[2]+totalLeft[3])*100 : 0,
        ehcartLeft2:totalLeft[0]+totalLeft[1]+totalLeft[2]+totalLeft[3] ? totalLeft[1]/(totalLeft[0]+totalLeft[1]+totalLeft[2]+totalLeft[3])*100 : 0,
        ehcartLeft3:totalLeft[0]+totalLeft[1]+totalLeft[2]+totalLeft[3] ? totalLeft[2]/(totalLeft[0]+totalLeft[1]+totalLeft[2]+totalLeft[3])*100 : 0,
        ehcartLeft4:totalLeft[0]+totalLeft[1]+totalLeft[2]+totalLeft[3] ? totalLeft[3]/(totalLeft[0]+totalLeft[1]+totalLeft[2]+totalLeft[3])*100 : 0,
        ehcartRight1:totalRight ? (result[2][0].values[0]/totalRight)*100 : 0,
        ehcartRight2:totalRight ? (result[3][0].values[0]/totalRight)*100 : 0,
        spaceRight1:(result[2][0].values[0]/1024/1024/1024).toFixed(2),
        spaceRight2:(result[3][0].values[0]/1024/1024/1024).toFixed(2),
        spaceLeft1:(result[0][0].values[0]/1024/1024/1024).toFixed(2),
        spaceLeft2:(result[1][0].values[0]/1024/1024/1024).toFixed(2),
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
        if(this.props.tabkey === "3"){
          this.getMemoryDataAll()
        }
      })
    }
  }
  componentDidMount() {
    this.getMemoryDataAll()
  }
  render() {
    return (
      <div className='Memoryclass'>
        <Row gutter={10}>
          <Col className="gutter-row cpuborder" span={12}>
          <div className='Memorystyle'>
            <span style={{width: this.state.ehcartLeft1 ? `${this.state.ehcartLeft1}%` : 0 ,backgroundColor:'#EC6E18'}}></span>
            <span style={{width: this.state.ehcartLeft2 ? `${this.state.ehcartLeft2}%` : 0 ,backgroundColor:'#2DA769'}}></span>
            <span style={{width: this.state.ehcartLeft3 ? `${this.state.ehcartLeft3}%` : 0 ,backgroundColor:'#9185F0'}}></span>
            <span style={{width: this.state.ehcartLeft4 ? `${this.state.ehcartLeft4}%` : 0 ,backgroundColor:'#EEBA18'}}></span>
            <span style={{width: this.state.ehcartLeft1 && this.state.ehcartLeft2 && this.state.ehcartLeft3 && this.state.ehcartLeft4 ? 
              '0%' : '100%' ,backgroundColor:'#E8E8E8'}}></span>
          </div>
          <p style={{fontWeight: 'bold',textAlign:'end'}} >Available Space { this.state.spaceLeft2 } GB / Total Space { this.state.spaceLeft1 } GB</p>
          </Col>
          <Col className="gutter-row cpuborder" span={12}>
          <div className='Memorystyle'>
            <span style={{width:this.state.ehcartRight1 ? `${this.state.ehcartRight1}%` : 0 ,backgroundColor:'#EC6E18'}}></span>
            <span style={{width:this.state.ehcartRight2 ? `${this.state.ehcartRight2}%` : 0 ,backgroundColor:'#2DA769'}}></span>
            <span style={{width:this.state.ehcartRight1 && this.state.ehcartRight2 ? '0%' : '100%' ,backgroundColor:'#E8E8E8'}}></span>
          </div>
          <p style={{fontWeight: 'bold',textAlign:'end'}}>Available Swap Space {this.state.spaceRight2} GB / Total Swap Space {this.state.spaceRight1} GB</p>
          </Col>
        </Row>
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
