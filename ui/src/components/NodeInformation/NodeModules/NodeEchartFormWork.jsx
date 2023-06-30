import React, { Component } from 'react';
import { Col, Row } from 'antd';
import ReactEcharts from 'echarts-for-react';
import PropTypes from 'prop-types';
import { Empty, message } from 'antd';
import { formatTimestamp } from '../../../utils/function';

const yAx = {type: 'value',max: 100,}
let legendObj =   {
      color: '#4d5964',
      fontSize: 12,
      fontFamily: 'Arial',
      fontWeight: 'bold',
      right:20
    }

export default class NodeEchartFormWork extends Component {
  static propTypes={
    echartData:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      legendData: [],
      titleData: [],
      xAxisData: [],
      seriesData: [],
      flg:0,
      title:'',
      legendFlg:1,
      ifShow: true,
      unit:'',
    }
  }
  getOption = () => {
    legendObj["data"] = this.state.legendData
    return {
      title: [{
        left:this.state.legendFlg === 2 ? '1%' : 0,
        text: !Array.isArray(this.state.title) ? this.state.title : this.state.title[0],
        textStyle:{
            color:'#4D5964',
            fontWeight:'bold',
            fontFamily:'Arial',
            fontSize: 12,
        }},{
          left:this.state.legendFlg === 2 ? 140 : 0,
          text: Array.isArray(this.state.title) ? this.state.title[1] : '',
          textStyle:{
              color:'#4D5964',
              fontWeight:'bold',
              fontFamily:'Arial',
              fontSize: 12,
          }}],
      tooltip: {
        trigger: 'axis',
        formatter:(param)=>{
          let res = param[0].axisValue.split('\n').join(' ') + '<br>'
          param.forEach((item,index)=>{
            res += `${item.marker}${item.seriesName}&nbsp;&nbsp;&nbsp;&nbsp;<span style="font-weight: bold;text-align: right;float:right">${item.value}${this.state.unit}</span><br>`
          })
          return res
        },
        axisPointer: {
          type: 'cross',
          label: {
            backgroundColor: '#6a7985'
          }
        }
      },
      legend: this.state.legendFlg === 2 ? {...legendObj} : '',
      grid: {
        top: this.state.legendFlg === 2 ? '12%' : '3%',
        left: '3%',
        right: '4%',
        bottom: '6%',
        containLabel: true
      },
      xAxis: [
        {
          type: 'category',
          boundaryGap: false,
          splitLine: {
            //网格线
            show: true, //是否显示
            lineStyle: {
              //网格线样式
              color: '#F2F2F2', //网格线颜色
              width: 1, //网格线的加粗程度
              type: 'dashed' //网格线类型
            }
          },
          axisLine: {
            show: true,
            lineStyle: {
              color: '#939393',
              width: 1,
              type: 'solid'
            }
          },
          axisLabel: {
            show: true,
            margin: 10,
            textStyle: {
              color: '#4D5964',
              fontSize: 11,
              fontFamily: 'Arial',
              fontWeight: 'normal'
            }
          },
          data: this.state.xAxisData.map(function (str) {
            return str.replace(' ', '\n');
          })
        }
      ],
      yAxis: [
        {
          ...this.state.flg ? yAx : '',
          min: 0,
          splitLine: {
            //网格线
            show: true, //是否显示
            lineStyle: {
              //网格线样式
              color: '#F2F2F2', //网格线颜色
              width: 1, //网格线的加粗程度
              type: 'dashed' //网格线类型
            }
          },
          ayisLine: {
            show: true,
            lineStyle: {
              color: '#939393',
              width: 1,
              type: 'solid'
            }
          },
          axisLabel: {
            margin: 10,
            formatter: (val) => {
              let value = this.state.flg ? val + '%' : val
              return value;
            },
            show: true,
            textStyle: {
              color: '#4D5964',
              fontSize: 11,
              fontFamily: 'Arial',
              fontWeight: 'normal',
              align: 'right'
            }
          }
        }
      ],
      series: this.state.seriesData
    };
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    if (JSON.stringify(nextProps.echartData) !== '{}' && nextProps.echartData.legend.length>0) {
      let legendItem = {},titleItem = {},legendData = [],titleData = [],xAxisData = [],seriesData = [],seriesItem = {},
      colors = ['#2DA769','#5990FD','#9185F0','#EC6F1A','#F43146']
      nextProps.echartData.legend.forEach(function (data, i, v) {
        legendItem = {
          name: data.description
        }
        let titleitem = nextProps.echartData.seriesData[i].data[nextProps.echartData.seriesData[i].data.length-1]
        titleItem = {
          icon: data.image,
          description: data.description,
          titleData: nextProps.echartData.unit === '%' ? (titleitem*100).toFixed(2) : nextProps.echartData.fixedflg ? titleitem.toFixed(nextProps.echartData.fixedflg) : titleitem
        }
        legendData.push(legendItem)
        titleData.push(titleItem)
      })
      nextProps.echartData.xAxisData.forEach(function (data, i, v) {
        xAxisData.push(formatTimestamp(data))
      })
      nextProps.echartData.seriesData.forEach(function (item, i, v) {
        let itemData = []
        item.data.forEach((item) => {
          if(nextProps.echartData.unit === '%'){
            itemData.push((item*100).toFixed(2))
          } else if(nextProps.echartData.fixedflg){
            itemData.push(item.toFixed(nextProps.echartData.fixedflg))
          }
        });
        seriesItem = {
          data: nextProps.echartData.fixedflg || nextProps.echartData.unit === '%' ? itemData : item.data,
          name: item.description,
          type: 'line',
          smooth: true,
          symbol: 'circle',
          symbolSize: 3,
          itemStyle: {
            normal: {
              color: item.colors,
              lineStyle:{
                width:1
              }
            }
          },
          ...nextProps.echartData.isStack && {
            stack: 'Total',
            areaStyle: {
              color: {
                x: 0,
                y: 0,
                x2: 0,
                y2: 1,
                colorStops: [{
                  offset: 0, color: item.colors+'ff'
                }, {
                  offset: 1, color: item.colors+'00'
                } ],
                global: false
              }
            }
          },
          emphasis: {
            focus: 'series'
          },
        }
        seriesData.push(seriesItem)
      })
      this.setState({
        legendData:legendData,
        xAxisData:xAxisData,
        seriesData:seriesData,
        titleData:titleData,
        flg:nextProps.echartData.flg,
        title:nextProps.echartData.title,
        legendFlg:nextProps.echartData.legendFlg,
        unit:nextProps.echartData.unit
      })
    } else {
      this.setState({
        ifShow: false
      })
    }
  }
  render() {
    return (
      this.state.ifShow ?
      <div style={{border: '1px solid #e8e8e8ff',paddingTop:10}}>
      <p className='formworkp'>
        <>
        {this.state.legendFlg === 1 && this.state.titleData.map((item,index) => {
                return (
                  <span key={index} className='spanstyle'><b className='descriptionstyle'>{item.description+ " "}</b><b className='titlestyle'>{this.state.flg || this.state.unit ? item.titleData+this.state.unit+ " " : item.titleData+ " "}</b><img alt='' src={item.icon}></img></span>
                )
              })}
        </>
      </p>
      <ReactEcharts
        ref={(e) => {
          this.echartsElement = e
        }}
        option={this.getOption()}
        style={{ width: '100%', height: 240 }}
        lazyUpdate={true}
      >
      </ReactEcharts>
      </div>
      : <Empty description={false} style={{ paddingTop: 50 }} />
    )
  }
}
