import React, { Component } from 'react';
import { Card } from 'antd';
import PropTypes from 'prop-types';
import ReactEcharts from 'echarts-for-react';

export default class StatisticsForSchemaChart extends Component {
  static propTypes={
    statisticsforSchema:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      xdata: [],
      ydata: []
    }
  }
  getOption = () => {
    return {
      tooltip: { show: true },
      xAxis: {
        type: 'category',
        axisLabel: {
          interval: 0,
          textStyle: {
            color: '#4D5964',
            fontSize: '12'
          },
        },
        data: this.state.xdata
      },
      yAxis: {
        type: 'value',
       
        axisLabel: {
          textStyle: {
            color: '#4D5964',
            fontSize: '10'
          }
        }
      },
      grid: {
        width: '90%',
        height: '80%',
        top: '5%'
      },
      series: [
        {
          data: this.state.ydata,
          type: 'bar',
      
          barMaxWidth:28,
          itemStyle: {
            color: "#5990FD",
          },
        }
      ]
    };
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    let xdataArr = []
    let ydataArr = []

  Object.keys(nextProps?.statisticsforSchema).forEach(function (key, i, v) {
    ydataArr.push(nextProps.statisticsforSchema[key])
    xdataArr = v
  })

     
      this.setState({
        xdata: xdataArr,
        ydata: ydataArr
      })
  }
  render () {
    return (
      <div >
        <Card title="Statistics For Schema" style={{ height: '278px' }}>
          <ReactEcharts
            ref={(e) => {
              this.echartsElement = e
            }}
            option={this.getOption()}
            style={{ width: '100%', height: 200 }}
            lazyUpdate={true}
          >
          </ReactEcharts>
        </Card>
      </div>
    )
  }
}
