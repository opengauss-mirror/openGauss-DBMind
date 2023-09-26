import React, { Component } from 'react';
import { Empty, Modal } from 'antd';
import ReactEcharts from 'echarts-for-react';
import { commonMetricMethod } from '../../utils/function';
import { getTransaction } from '../../api/overview';
import db from '../../utils/storage';

export default class TransactionStateChart extends Component {
  constructor() {
    super()
    this.state = {
      xpartData: [],
      ypartData: [],
      xallData: [],
      yallData: [],
      ifShow: true,
      isModalVisible:false,
      param: {
        instance:db.ss.get('Instance_value')
      },
      metricData:['pg_db_xact_commit','pg_db_xact_rollback']
    }
  }
  getOption (flg) {
    return {
      tooltip: {
        trigger: 'axis',
        axisPointer: {
          type: 'cross',
          crossStyle: {
            color: '#999'
          }
        }
      },
      legend: {
        data: ['commit', 'abort'],
        x: 'right',
        y:-5
      },
      grid: {
        x:60,
        y: '10%',
      },
      xAxis: [
        {
          type: 'category',
          data: flg ? this.state.xpartData : this.state.xallData,
          axisPointer: {
            type: 'shadow'
          },
          axisTick: {
            show: false
          }
        }
      ],
      yAxis: [
        {
          axisLabel: {
            margin: 2,
            formatter: (value, index) => {
              if (value >= 10000 && value < 10000000) {
                value = value / 10000 + "万";
              } else if (value >= 10000000) {
                value = value / 10000000 + "千万";
              }
              return value;
            }
          },
          type: 'value'
        }
      ],
      color:['#5990fdff','#fecd03ff'],
      series: [
        {
          name: 'commit',
          type: 'bar',
          barWidth:flg ? 18 : 36,
          barGap:'60%',/*多个并排柱子设置柱子之间的间距*/
          data: flg ? this.state.ypartData[1] : this.state.yallData[1],
        },
        {
          name: 'abort',
          type: 'bar',
          barWidth:flg ? 18 : 36,
          data: flg ? this.state.ypartData[0] : this.state.yallData[0],
        },
      ]
    };
  }
  getTransactionAll(flg){
    Promise.all([
      commonMetricMethod(this.state.param,{label:this.state.metricData[0]},getTransaction),
      commonMetricMethod(this.state.param,{label:this.state.metricData[1]},getTransaction)
    ]).then((result)=>{
      if(result[0].length){
        let xData = [],commitData = [],abortData = []
        result[0].forEach((item, index) => {
          xData.push(item.labels.datname)
          commitData.push(item.values[0])
        });
        result[1].forEach((item, index) => {
          abortData.push(item.values[0])
        });
        if(flg){
          if(xData.length > 5){
            xData = xData.slice(0,5)
            commitData = commitData.slice(0,5)
            abortData = abortData.slice(0,5)
          }
          this.setState(() => ({
            ifShow: true,
            xpartData: xData,
            ypartData: [abortData,commitData],
          }))
        } else {
          this.setState(() => ({
            ifShow: true,
            xallData: xData,
            yallData: [abortData,commitData],
          }))
        }
      } else {
        this.setState({ifShow: false})
      }
    }).catch((error) => {
      console.log('error', error)
    })
  }
  isMore() {
    this.setState({
      isModalVisible: true
    },()=>{
      this.getTransactionAll(false)
    })
  }
  handleCancel = () => {
    this.setState({
      isModalVisible: false,
    })
  }
  componentDidMount () {
    this.getTransactionAll(true)
  }
  render () {
    return (
      <div>
        {this.state.ifShow ? <ReactEcharts
            ref={(e) => {
              this.echartsElement = e
            }}
            option={this.getOption(true)}
            style={{ width: '100%', height: '258px' }}
            lazyUpdate={true}
          >
          </ReactEcharts> : <Empty description={this.state.ifShow} style={{ height: 200, paddingTop: 50 }} />}
          <Modal title="Transaction State" style={{maxWidth: "70vw"}} bodyStyle={{overflowY: "auto",height: "60vh",}} width="70vw" okButtonProps={{ style: { display: 'none' } }} 
         destroyOnClose='true' visible={this.state.isModalVisible} maskClosable = {false} centered='true' onCancel={() => this.handleCancel()}>
          <ReactEcharts
            ref={(e) => {
              this.echartsElement = e
            }}
            option={this.getOption(false)}
            style={{ width: 1296, height: 500 }}
            lazyUpdate={true}
          >
          </ReactEcharts>
        </Modal>
      </div>
    )
  }
}
