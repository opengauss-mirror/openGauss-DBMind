import React, { Component } from 'react';
import { Col, Row } from 'antd';
import { Empty, message } from 'antd';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import { commonMetricMethod } from '../../utils/function';

const metricData = ['gaussdb_total_connection','gaussdb_active_connection','gaussdb_idle_connection','statement_responsetime_percentile_p80','statement_responsetime_percentile_p95','gaussdb_qps_by_instance']
export default class DBPerformanceIndicators extends Component {
  constructor(props) {
    super(props)
    this.state = {
      chartData1: {},
      chartData2: {},
      chartData3: {},
      chartData4: {},
      param: {
        instance:this.props.selValue,
        latest_minutes:this.props.selTimeValue ? this.props.selTimeValue : null,
        fetch_all:false,
        regex:false,
        from_timestamp:this.props.startTime ? this.props.startTime : null,
        to_timestamp:this.props.endTime ? this.props.endTime : null
      }
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
    commonMetricMethod(this.state.param,{label:metricData[0]}),
    commonMetricMethod(this.state.param,{label:metricData[1]}),
    commonMetricMethod(this.state.param,{label:metricData[2]}),
    commonMetricMethod(this.state.param,{label:metricData[3]}),
    commonMetricMethod(this.state.param,{label:metricData[4]}),
    commonMetricMethod(this.state.param,{label:metricData[5]})
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
        param:Object.assign(this.state.param,{instance: this.props.selValue,latest_minutes: this.props.selTimeValue ? this.props.selTimeValue : null,from_timestamp: this.props.startTime,to_timestamp: this.props.endTime})
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
