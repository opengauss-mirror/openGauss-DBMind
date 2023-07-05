import React, { Component } from 'react';
import { Col, Row, Collapse, message} from 'antd';
import ReactEcharts from 'echarts-for-react';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';

const { Panel } = Collapse;
export default class DrawerStatistics extends Component {
  constructor(props) {
    super(props)
    this.state = {
      vectorKey:0,
      dataType:['returned rows','fetched tuples','returned tuples','inserted tuples','updated tuples','deleted tuples','db_time','cpu_time','data io time','parse time','plan time','lock_wait_time','lwlock_wait_time','hard parse','soft parse'],
      echartData:[],
      clientAddressxAxis:[],
      clientAddressValues:[]
    }
  }
  getData(){
    let timeData = [],allArrayData = [[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]],
    indexName = ['n_returned_rows','n_tuples_fetched','n_tuples_returned','n_tuples_inserted','n_tuples_updated','n_tuples_deleted','db_time','cpu_time','data_io_time','parse_time','plan_time','lock_wait_time','lwlock_wait_time','n_hard_parse','n_soft_parse','client_addr']
    this.props.dataSource.forEach((item, index) => {
      if(item["start_time"] !== undefined){
        timeData.push(new Date(item["start_time"]).getTime())
      }
      indexName.forEach((oitem, oindex) => {
        if(item[oitem] !== undefined){
          allArrayData[oindex].push(item[oitem])
        }
      })
    });
    let data1 = {'legend':[{image:'',description: this.state.dataType[0]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[0],description: this.state.dataType[0], colors: '#2DA769'}],'flg':0,'legendFlg':2,title:this.state.dataType[0],'unit':'','fixedflg':0}
    let data2 = {'legend':[{image:'',description: this.state.dataType[1]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[1],description: this.state.dataType[1], colors: '#5990FD'}],'flg':0,'legendFlg':2,title:this.state.dataType[1],'unit':'','fixedflg':0}
    let data3 = {'legend':[{image:'',description: this.state.dataType[2]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[2],description: this.state.dataType[2], colors: '#EC701C'}],'flg':0,'legendFlg':2,title:this.state.dataType[2],'unit':'','fixedflg':0}
    let data4 = {'legend':[{image:'',description: this.state.dataType[3]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[3],description: this.state.dataType[3], colors: '#2DA769'}],'flg':0,'legendFlg':2,title:this.state.dataType[3],'unit':'','fixedflg':0}
    let data5 = {'legend':[{image:'',description: this.state.dataType[4]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[4],description: this.state.dataType[4], colors: '#5990FD'}],'flg':0,'legendFlg':2,title:this.state.dataType[4],'unit':'','fixedflg':0}
    let data6 = {'legend':[{image:'',description: this.state.dataType[5]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[5],description: this.state.dataType[5], colors: '#EC701C'}],'flg':0,'legendFlg':2,title:this.state.dataType[5],'unit':'','fixedflg':0}
    let data7 = {'legend':[{image:'',description: this.state.dataType[6]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[6],description: this.state.dataType[6], colors: '#5990FD'}],'flg':0,'legendFlg':2,title:this.state.dataType[6],'unit':'','fixedflg':0}
    let data8 = {'legend':[{image:'',description: this.state.dataType[7]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[7],description: this.state.dataType[7], colors: '#F43146'}],'flg':0,'legendFlg':2,title:this.state.dataType[7],'unit':'','fixedflg':0}
    let data9 = {'legend':[{image:'',description: this.state.dataType[8]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[8],description: this.state.dataType[8], colors: '#2DA769'}],'flg':0,'legendFlg':2,title:this.state.dataType[8],'unit':'','fixedflg':0}
    let data10 = {'legend':[{image:'',description: this.state.dataType[9]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[9],description: this.state.dataType[9], colors: '#2DA769'}],'flg':0,'legendFlg':2,title:this.state.dataType[9],'unit':'','fixedflg':0}
    let data11 = {'legend':[{image:'',description: this.state.dataType[10]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[10],description: this.state.dataType[10], colors: '#2DA769'}],'flg':0,'legendFlg':2,title:this.state.dataType[10],'unit':'','fixedflg':0}
    let data12 = {'legend':[{image:'',description: this.state.dataType[11]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[11],description: this.state.dataType[11], colors: '#2DA769'}],'flg':0,'legendFlg':2,title:this.state.dataType[11],'unit':'','fixedflg':0}
    let data13 = {'legend':[{image:'',description: this.state.dataType[12]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[12],description: this.state.dataType[12], colors: '#2DA769'}],'flg':0,'legendFlg':2,title:this.state.dataType[12],'unit':'','fixedflg':0}
    let data14 = {'legend':[{image:'',description: this.state.dataType[13]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[13],description: this.state.dataType[13], colors: '#2DA769'}],'flg':0,'legendFlg':2,title:this.state.dataType[13],'unit':'','fixedflg':0}
    let data15 = {'legend':[{image:'',description: this.state.dataType[14]}],'xAxisData':timeData,'seriesData':[{data:allArrayData[14],description: this.state.dataType[14], colors: '#2DA769'}],'flg':0,'legendFlg':2,title:this.state.dataType[14],'unit':'','fixedflg':0}
    let a = new Map(),clientHeader = [],clientValues = [];
    allArrayData[15].forEach((item) => {
      if (!a.get(item)) {
        clientHeader.push(item)
        a.set(item, 1);
      } else {
        let b=a.get(item)
        a.set(item, b+1);
      }
    });
    a.forEach((value) => {
      clientValues.push(value)
    })
    this.setState({
      echartData:[data1,data2,data3,data4,data5,data6,data7,data8,data9,data10,data11,data12,data13,data14,data15],
      clientAddressxAxis:clientHeader,
      clientAddressValues:clientValues
    })
  }
  getOption = () => {
    return {
      title: {
        text: 'Source IP distribution',
        textStyle:{
          color:'#272727',
          fontFamily:'Arial',
          fontSize:16
        }
      },
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross',
          crossStyle: {
            color: '#999'
          }
        }
      },
      grid: {
        left: '1%',
        right: '1%',
        bottom: '6%',
        containLabel: true
      },
      xAxis: [
        {
          type: 'category',
          axisPointer: {
            type: 'shadow',
          },
          axisLabel:{
            interval:0
          },
          data: this.state.clientAddressxAxis.map(function (str) {
            return str;
          })
        }
      ],
      yAxis: [
        {
          type: 'value',
          axisLabel: {
            formatter: (val) => {
              let value = val + '%'
              return value;
            }
          }
        }
      ],
      series: [
        {
          type: 'bar',
          data: this.state.clientAddressValues,
          itemStyle: {
            color: '#5990fd'
          },
          barWidth : 45,
          tooltip: {
            valueFormatter: function (val) {
              let value = val + '%'
              return value;
            }
          },
        }
      ]
    };
  }
  componentDidUpdate(prevProps) {
    if(prevProps.dataSource !== this.props.dataSource || prevProps.columns !== this.props.columns ) {
      this.getData()
    }
  }
  componentDidMount() {
    this.getData()
  }
  onChange = (key) => {
    this.setState({vectorKey:key})
  };
  render() {
    return (
      <div>
        <Collapse  activeKey={this.state.vectorKey}  onChange={(key)=>{this.onChange(key)}} expandIconPosition='end' >
          <Panel header="Rows" forceRender={true} className='panelStyle'>
            <Row gutter={[10, 10]}>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[0]} />
              </Col>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[1]} />
              </Col>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[2]} />
              </Col>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[3]} />
              </Col>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[4]} />
              </Col>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[5]} />
              </Col>
            </Row>
          </Panel>
          <Panel header="Time"  forceRender={true} className='panelStyle'>
            <Row gutter={[10, 10]}>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[6]} />
              </Col>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[7]} />
              </Col>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[8]} />
              </Col>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[9]} />
              </Col>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[10]} />
              </Col>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[11]} />
              </Col>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[12]} />
              </Col>
            </Row>
          </Panel>
          <Panel header="Parse" forceRender={true} className='panelStyle'>
            <Row gutter={[10, 10]}>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[13]} />
              </Col>
              <Col className="gutter-row cpuborder" span={24}>
                <NodeEchartFormWork echartData={this.state.echartData[14]} />
              </Col>
            </Row>
          </Panel>
        </Collapse>
        <ReactEcharts
          ref={(e) => {
            this.echartsElement = e
          }}
          option={this.getOption()}
          style={{ width: '100%', height: 300 }}
          notMerge={true}
          lazyUpdate={true}
        >
        </ReactEcharts>
      </div>
    )
  }
}