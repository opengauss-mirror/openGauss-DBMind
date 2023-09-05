import React, { Component } from 'react';
import { Col, Row, message } from 'antd';
import SystemImg from '../../assets/imgs/System.png';
import UserImg from '../../assets/imgs/User.png';
import EmptyImg from '../../assets/imgs/Empty.png';
import WaitImg from '../../assets/imgs/Wait.png';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import { commonMetricMethod } from '../../utils/function';

const metricData = ['os_cpu_system_usage','os_cpu_user_usage','os_cpu_idle_usage','os_cpu_iowait_usage']
export default class NodeCpu extends Component {
  constructor(props) {
    super(props)
    this.state = {
      chartData1:{},
      chartData2:{},
      chartData3:{},
      chartData4:{},
      param: {
        instance:this.props.selValue,
        latest_minutes:this.props.selTimeValue ? this.props.selTimeValue : null,
        fetch_all:false,
        from_timestamp:this.props.startTime ? this.props.startTime : null,
        to_timestamp:this.props.endTime ? this.props.endTime : null
      }
    }
  }
async getCpuDataAll () {
  Promise.all([
    commonMetricMethod(this.state.param,{label:metricData[0]}),
    commonMetricMethod(this.state.param,{label:metricData[1]}),
    commonMetricMethod(this.state.param,{label:metricData[2]}),
    commonMetricMethod(this.state.param,{label:metricData[3]})
  ]).then((result)=>{
    if(result[0]){
      let xDataArray = [[],[],[],[]],yDataArray = [[],[],[],[]]
      result.forEach((item,index) => {
        xDataArray[index] = item[0].timestamps
      });
      result.forEach((item,index) => {
        item[0].values.forEach((oitem) => {
          yDataArray[index].push(oitem)
        });
      });
      let data1 = {'legend':[{image:SystemImg,description:'System'}],'xAxisData':xDataArray[0],'seriesData':[{data:yDataArray[0],description:'System',colors:'#2DA769'}],'flg':1,'legendFlg':1,'unit':'%','fixedflg':0,'toolBox':true}
      let data2 = {'legend':[{image:UserImg,description:'User'}],'xAxisData':xDataArray[1],'seriesData':[{data:yDataArray[1],description:'User',colors:'#5990FD'}],'flg':1,'legendFlg':1,'unit':'%','fixedflg':0,'toolBox':true}
      let data3 = {'legend':[{image:EmptyImg,description:'Empty'}],'xAxisData':xDataArray[2],'seriesData':[{data:yDataArray[2],description:'Empty',colors:'#9185F0'}],'flg':1,'legendFlg':1,'unit':'%','fixedflg':0,'toolBox':true}
      let data4 = {'legend':[{image:WaitImg,description:'IO Wait'}],'xAxisData':xDataArray[3],'seriesData':[{data:yDataArray[3],description:'IO Wait',colors:'#EC6F1A'}],'flg':1,'legendFlg':1,'unit':'%','fixedflg':0,'toolBox':true}
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
    if(prevProps.selValue !== this.props.selValue || prevProps.selTimeValue !== this.props.selTimeValue || prevProps.startTime !== this.props.startTime || prevProps.endTime !== this.props.endTime || prevProps.tabkey !== this.props.tabkey) {
      this.setState(() => ({
        param:Object.assign(this.state.param,{instance: this.props.selValue,latest_minutes: this.props.selTimeValue ? this.props.selTimeValue : null,from_timestamp: this.props.startTime,to_timestamp: this.props.endTime})
      }),()=>{
        if(this.props.tabkey === "1"){
          this.getCpuDataAll()
        }
      })
    }
  }
  componentDidMount () {
    this.getCpuDataAll()
  }
  render() {
    return (
      <div>
        <Row gutter={[10,10]}>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork  echartData={this.state.chartData1}/>
          </Col>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork  echartData={this.state.chartData2}/>
          </Col>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork  echartData={this.state.chartData3}/>
          </Col>
          <Col className="gutter-row cpuborder" span={12}>
            <NodeEchartFormWork  echartData={this.state.chartData4}/>
          </Col>
        </Row>
      </div>
    )
  }
}
