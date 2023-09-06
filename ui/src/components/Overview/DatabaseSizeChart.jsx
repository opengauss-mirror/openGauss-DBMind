import React, { Component } from 'react';
import { Empty, Modal, message } from 'antd';
import ReactEcharts from 'echarts-for-react';
import { getDatabaseSize } from '../../api/overview';
import db from '../../utils/storage';

export default class DatabaseSizeChart extends Component {
  constructor() {
    super()
    this.state = {
      xpartData: [],
      ypartData: [],
      xallData: [],
      yallData: [],
      ifShow: true,
      isModalVisible:false,
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
      grid: {
        x:60,
        y: '12%',
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
      yAxis:{
        type: 'value',
      },
      color:'#9185f0ff',
      series: [
        {
          name: 'commit',
          type: 'bar',
          barWidth:flg ? 18 : 36,
          barGap:'60%',/*多个并排柱子设置柱子之间的间距*/
          data:flg ? this.state.ypartData : this.state.yallData,
                      //显示数值
                itemStyle: {
                  normal: {
                    label: {
                      show: true, //开启显示
                      position: 'top', //在上方显示
                      textStyle: {
                        //数值样式
                        color: 'black',
                        fontSize: 12,
                      },
                    },
                  },
                }, 
        }
      ]
    };
  }
  
  async getDatabaseSize (flg) {
    let param = {
      instance:db.ss.get('Instance_value'),
      label:'pg_database_size_bytes'
    }
    const { success, data, msg } = await getDatabaseSize(param)
    if (success) {
      let xData = [],yData = []
        data.forEach((item, index) => {
          xData.push(item.labels.datname)
          yData.push((item.values[0]/1024).toFixed(2))
        });
        if(flg){
          if(xData.length > 5){
            xData = xData.slice(0,5)
            yData = yData.slice(0,5)
          }
          this.setState(() => ({
            ifShow: true,
            xpartData: xData,
            ypartData: yData,
          }))
        } else {
          this.setState(() => ({
            ifShow: true,
            xallData: xData,
            yallData: yData,
          }))
        }
    } else {
      this.setState({ifShow: false})
      message.error(msg)
    }
  }
  isMore() {
    this.setState({
      isModalVisible: true
    },()=>{
      this.getDatabaseSize(false)
    })
  }
  handleCancel = () => {
    this.setState({
      isModalVisible: false,
    })
  }
  componentDidMount () {
    this.getDatabaseSize(true)
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
          <Modal title="Database Size" style={{maxWidth: "70vw"}} bodyStyle={{overflowY: "auto",height: "60vh",}} width="70vw" okButtonProps={{ style: { display: 'none' } }} 
         destroyOnClose='true' visible={this.state.isModalVisible} maskClosable = {false} centered='true' onCancel={() => this.handleCancel()}>
          <ReactEcharts
            ref={(e) => {
              this.echartsElement = e
            }}
            option={this.getOption(false)}
            style={{ width: 1296, height: 500  }}
            lazyUpdate={true}
          >
          </ReactEcharts>
        </Modal>
      </div>
    )
  }
}
