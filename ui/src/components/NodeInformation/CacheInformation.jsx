import React, { Component } from 'react';
import { Col, Row, message, Collapse } from 'antd';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import { getServiceCapabilityData } from '../../api/autonomousManagement';

const { Panel } = Collapse;
export default class CacheInformation extends Component {
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
  async getCacheInformationData1 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_db_blks_read',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getCacheInformationData2 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_db_blks_hit',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getCacheInformationData3 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_db_blks_access',
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
  async getCacheInformationDataAll () {
    Promise.all([
      this.getCacheInformationData1(),
      this.getCacheInformationData2(),
      this.getCacheInformationData3()
    ]).then((result)=>{
      if(result[0]){
        result.forEach((item,index) => {
          item.sort(this.compare('datname'))
        });
        let primitiveDataAll = [],serviceAllArray = []
        result[0].forEach((item,index) => {
          let DataItems = []
          result.forEach((oitem,oindex) => {
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
                let data1 = {'legend':[{image: '', description: 'Disk Read/Write'}],'xAxisData':item[0].timestamps,'seriesData':[{data:item[0].values,description: 'Disk Read/Write', colors: '#2DA769'}],'flg':0,'legendFlg':2,title:'Disk Read/Write', 'unit':'','fixedflg':0}
                let data2 = {'legend':[{image: '', description: 'Cache Read/Write'}],'xAxisData':item[1].timestamps,'seriesData':[{data:item[1].values,description: 'Cache Read/Write', colors: '#5990FD'}],'flg':0,'legendFlg':2,title:'Cache Read/Write','unit':'','fixedflg':0}
                let data3 = {'legend':[{image: '', description: 'Hit Rate/Write'}],'xAxisData':item[2].timestamps,'seriesData':[{data:item[2].values,description: 'Hit Rate/Write', colors: '#9185F0'}],'flg':0,'legendFlg':2,title:'Hit Rate/Write','unit':'','fixedflg':0}
                chartData.push(data1,data2,data3)
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
      if(prevProps.selValue !== this.props.selValue || prevProps.selTimeValue !== this.props.selTimeValue || prevProps.tabkey !== this.props.tabkey || prevProps.tabChildkey !== this.props.tabChildkey) {
        this.setState(() => ({
          selValue: this.props.selValue,selTimeValue: this.props.selTimeValue
        }),()=>{
          if(this.props.tabkey === "3" && this.props.tabChildkey === "2" ){
            this.getCacheInformationDataAll()
          }
        })
      }
    }
    componentDidMount() {
      this.getCacheInformationDataAll()
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
                <Row gutter={10}>
                  <Col className="gutter-row cpuborder" span={12}>
                    <NodeEchartFormWork echartData={item[0]} />
                  </Col>
                  <Col className="gutter-row cpuborder" span={12}>
                    <NodeEchartFormWork echartData={item[1]} />
                  </Col>
                  <Col className="gutter-row cpuborder" span={24}>
                    <NodeEchartFormWork echartData={item[2]} />
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
