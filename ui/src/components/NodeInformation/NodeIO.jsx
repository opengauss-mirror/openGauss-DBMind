import React, { Component } from 'react';
import { Col, Row, Collapse, message} from 'antd';
import AverageQueueLength from '../../assets/imgs/Average Queue Length.png';
import BandwidthUtilization from '../../assets/imgs/Bandwidth Utilization.png';
import Readrate from '../../assets/imgs/Read rate.png';
import SingleReadTime from '../../assets/imgs/Single Read Time.png';
import SingleWriteTime from '../../assets/imgs/Single Write Time.png';
import Tps from '../../assets/imgs/Tps.png';
import Writerate from '../../assets/imgs/Write rate.png';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import { getCommonMetric } from '../../api/autonomousManagement';

const { Panel } = Collapse;
export default class NodeIO extends Component {
  constructor(props) {
    super(props)
    this.state = {
      selValue:this.props.selValue,
      selTimeValue:this.props.selTimeValue,
      primitiveDataAll:[],
      ioAllData:[],
      vectorKey:''
    }
  }
  async getIoData1 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_disk_io_read_bytes',
      fetch:false
    }
    const { success, data, msg }= await getCommonMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getIoData2 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_disk_io_write_bytes',
      fetch:false
    }
    const { success, data, msg }= await getCommonMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getIoData3 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_disk_io_read_delay',
      fetch:false
    }
    const { success, data, msg }= await getCommonMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getIoData4 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_disk_io_write_delay',
      fetch:false
    }
    const { success, data, msg }= await getCommonMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getIoData5 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_disk_iops',
      fetch:false
    }
    const { success, data, msg }= await getCommonMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getIoData6 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_disk_io_queue_length',
      fetch:false
    }
    const { success, data, msg }= await getCommonMetric(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getIoData7 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_disk_ioutils',
      fetch:false
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
  async getIoDataAll() {
    Promise.all([
      this.getIoData1(),
      this.getIoData2(),
      this.getIoData3(),
      this.getIoData4(),
      this.getIoData5(),
      this.getIoData6(),
      this.getIoData7()
    ]).then((result)=>{
      if(result[0]){
        result.forEach((item,index) => {
          item.sort(this.compare('device'))
        });
        let primitiveDataAll = [],ioAllArray = []
        result[0].forEach((item,index) => {
          let DataItems = []
          result.forEach((oitem,oindex) => {
            DataItems.push(oitem[index])
        });
        primitiveDataAll.push(DataItems)
        });
        primitiveDataAll.forEach((item,index) => {
                let chartData = []
                let data1 = {'legend':[{image: Readrate, description:'SysRead Ratetem'},{image: Writerate, description: 'Write Rate'}],'xAxisData':item[0].timestamps,'seriesData':[{data:item[0].values,description:'SysRead Ratetem',colors:'#5990FD'}, { data:item[1].values, description: 'Write Rate', colors: '#2DA769'}],'flg':0,'legendFlg':1,'unit':'KB/s','fixedflg':1}
                let data2 = {'legend':[{image: SingleReadTime, description: 'Single Read Time'},{image: SingleWriteTime, description: 'Single Write Time'}],'xAxisData':item[2].timestamps,'seriesData':[{data:item[2].values,description: 'Single Read Time', colors: '#2DA769'}, { data:item[3].values, description: 'Single Write Time', colors: '#5990FD'}],'flg':0,'legendFlg':1,'unit':'KB/s','fixedflg':1}
                let data3 = {'legend':[{image: Tps, description: 'Tps'}],'xAxisData':item[4].timestamps,'seriesData':[{data:item[4].values,description: 'Tps', colors: '#F43146'}],'flg':0,'legendFlg':1,'unit':'KB/s','fixedflg':1}
                let data4 = {'legend':[{image: AverageQueueLength, description: 'Average Queue Length'}],'xAxisData':item[5].timestamps,'seriesData':[{data:item[5].values,description: 'Average Queue Length', colors: '#2DA769'}],'flg':0,'legendFlg':1,'unit':'KB/s','fixedflg':1}
                let data5 = {'legend':[{image: BandwidthUtilization, description: 'Bandwidth Utilization'}],'xAxisData':item[6].timestamps,'seriesData':[{data:item[6].values,description: 'Bandwidth Utilization', colors: '#9185F0'}],'flg':0,'legendFlg':1,'unit':'KB/s','fixedflg':1}
                chartData.push(data1,data2,data3,data4,data5)
                ioAllArray.push(chartData)
              })
            this.setState(() => ({
              ioAllData: ioAllArray,
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
        if(this.props.tabkey === "2"){
          this.getIoDataAll()
        }
      })
    }
  }
  componentDidMount() {
    this.getIoDataAll()
  }
  onChange = (key) => {
    this.setState({vectorKey:key})
  };
  render() {
    return (
      <div>
          {
            this.state.ioAllData.length > 0 ? this.state.ioAllData.map((item,index) => {
              return (
                <Collapse  activeKey={this.state.vectorKey}  onChange={(key)=>{this.onChange(key)}} expandIconPosition='end' >
                <Panel header={this.state.primitiveDataAll[index][0].labels.device} key={index} forceRender={true} className='panelStyle'>
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
                  <Col className="gutter-row cpuborder" span={24}>
                    <NodeEchartFormWork echartData={item[4]} />
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
