import React, { Component } from 'react';
import { Col, Row, Collapse, message } from 'antd';
import CurrentReceiveRate from '../../assets/imgs/Current Receive Rate.png';
import CurrentSendingRate from '../../assets/imgs/Current Sending Rate.png';
import ReceiveDrop from '../../assets/imgs/Receive_drop.png';
import TransmitDrop from '../../assets/imgs/Transmit_drop.png';
import TransmitError from '../../assets/imgs/Transmit_error.png';
import ReceiveError from '../../assets/imgs/Receive_error.png';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import { getCommonMetric } from '../../api/autonomousManagement';

const { Panel } = Collapse;
export default class NodeNetwork extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selValue:this.props.selValue,
      selTimeValue:this.props.selTimeValue,
      primitiveDataAll:[],
      networkAllData:[],
      vectorKey:["0"]
    }
  }
  async getNetworkData1 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_network_receive_bytes',
      fetch:true
    }
    const { success, data, msg }= await getCommonMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getNetworkData2 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_network_transmit_bytes',
      fetch:true
    }
    const { success, data, msg }= await getCommonMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getNetworkData3 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_network_receive_drop',
      fetch:true
    }
    const { success, data, msg }= await getCommonMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getNetworkData4 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_network_transmit_drop',
      fetch:true
    }
    const { success, data, msg }= await getCommonMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getNetworkData5 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_network_receive_error',
      fetch:true
    }
    const { success, data, msg }= await getCommonMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getNetworkData6 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_network_transmit_error',
      fetch:true
    }
    const { success, data, msg }= await getCommonMetric(param)
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
  async getNetworkDataAll() {
    Promise.all([
      this.getNetworkData1(),
      this.getNetworkData2(),
      this.getNetworkData3(),
      this.getNetworkData4(),
      this.getNetworkData5(),
      this.getNetworkData6()
    ]).then((result)=>{
      if(result[0]){
        result.forEach((item,index) => {
          item.sort(this.compare('device'))
        });
        let primitiveDataAll = [],networkAllArray = []
        result[0].forEach((item,index) => {
          let DataItems = []
          result.forEach((oitem,oindex) => {
            DataItems.push(oitem[index])
        });
        primitiveDataAll.push(DataItems)
        });
        primitiveDataAll.forEach((item,index) => {
                let chartData = [],data1 = {},data2 = {},data3 = {},data4 = {}
                  data1 = {'legend':[{image: CurrentReceiveRate, description: 'Current Receive Rate'}],'xAxisData':item[0]?item[0].timestamps:item[1].timestamps,'seriesData':[{data:item[0]?item[0].values:[...item[1].values.fill(0)],description: 'Current Receive Rate', colors: '#2DA769'}],'flg':0,'legendFlg':1,'unit':'KB/s','fixedflg':4}
                  data2 = {'legend':[{image: CurrentSendingRate, description: 'Current Sending Rate'}],'xAxisData':item[1]?item[1].timestamps:item[0].timestamps,'seriesData':[{data:item[1]?item[1].values:[...item[0].values.fill(0)],description: 'Current Sending Rate', colors: '#5990FD'}],'flg':0,'legendFlg':1,'unit':'KB/s','fixedflg':4}
                  data3 = {'legend':[{image: ReceiveDrop, description: 'Receive Drop'},{image: TransmitDrop, description: 'Transmit Drop'}],'xAxisData':item[2]?item[2].timestamps:item[3].timestamps,'seriesData':[{data:item[2]?item[2].values:[...item[3].values.fill(0)],description: 'Receive Drop', colors: '#2DA769'},{data:item[3]?item[3].values:[...item[2].values.fill(0)],description: 'Transmit Drop', colors: '#EC6F1A'}],'flg':0,'legendFlg':1,'unit':'','fixedflg':4}
                  data4 = {'legend':[{image: ReceiveError, description: 'Receive Error'},{image: TransmitError, description: 'Transmit Error'}],'xAxisData':item[4]?item[4].timestamps:item[5].timestamps,'seriesData':[{data:item[4]?item[4].values:[...item[5].values.fill(0)],description: 'Receive Error', colors: '#F43146'},{data:item[5]?item[5].values:[...item[4].values.fill(0)],description: 'Transmit Error', colors: '#9185F0'}],'flg':0,'legendFlg':1,'unit':'','fixedflg':4}
                chartData.push(data1,data2,data3,data4)
                networkAllArray.push(chartData)
              })
            this.setState(() => ({
              networkAllData: networkAllArray,
              primitiveDataAll:primitiveDataAll,
            }),()=>{
              this.onChange(this.state.vectorKey)
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
        if(this.props.tabkey === "4"){
          this.getNetworkDataAll()
        }
      })
    }
  }
  componentDidMount() {
    this.getNetworkDataAll()
  }
  onChange = (key) => {
    this.setState({vectorKey:key}, () => {
      this.forceUpdate();
    })
  };
  render() {
    return (
      <div className='nodeNetwork'>

          {
            this.state.networkAllData.length > 0 ? this.state.networkAllData.map((item,index) => {
              return (
                <Collapse  activeKey={this.state.vectorKey}  onChange={(key)=>{this.onChange(key)}} expandIconPosition='end' >
                <Panel header={
                  <Row gutter={[0, 12]}>
                    <Col className="gutter-row" span={3}>
                      <span className='networkPanelheader'>{this.state.primitiveDataAll[index][0]?this.state.primitiveDataAll[index][0].labels.device:'Default'}</span>
                    </Col>
                    <Col className="gutter-row" span={3}>
                      <span className='panelTitleSize' >status:</span>
                      <span className='panelTitleBold' >Enable</span>
                      <span className='panelCircle circleColorGreen'></span>
                    </Col>
                    <Col className="gutter-row" span={5}>
                      <span className='panelTitleSize'>Current Recevice Rate:</span>
                      <span className='panelTitleBold' >{this.state.primitiveDataAll[index][0]?(this.state.primitiveDataAll[index][0].values[this.state.primitiveDataAll[index][0].values.length-1]*100).toFixed(2):0}KB/s</span>
                      <span className='panelCircle circleColorPurple'></span>
                    </Col>
                    <Col className="gutter-row" span={5}>
                      <span className='panelTitleSize'>Current Sending Rate</span>
                      <span className='panelTitleBold' >{this.state.primitiveDataAll[index][1]?(this.state.primitiveDataAll[index][1].values[this.state.primitiveDataAll[index][1].values.length-1]*100).toFixed(2):0}KB/s</span>
                      <span className='panelCircle circleColorBlue'></span>
                    </Col>
                  </Row>
                } key={index} forceRender={true} className='panelStyle'>
                <Row gutter={[10, 10]}>
                  <Col className="gutter-row cpuborder" span={12}>
                    {this.state.vectorKey.indexOf(index.toString()) !== -1 ?<NodeEchartFormWork echartData={item[0]} />:''}
                  </Col>
                  <Col className="gutter-row cpuborder" span={12}>
                    {this.state.vectorKey.indexOf(index.toString()) !== -1 ?<NodeEchartFormWork echartData={item[1]} />:''}
                  </Col>
                  <Col className="gutter-row cpuborder" span={12}>
                    {this.state.vectorKey.indexOf(index.toString()) !== -1 ?<NodeEchartFormWork echartData={item[2]} />:''}
                  </Col>
                  <Col className="gutter-row cpuborder" span={12}>
                    {this.state.vectorKey.indexOf(index.toString()) !== -1 ?<NodeEchartFormWork echartData={item[3]} />:''}
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
