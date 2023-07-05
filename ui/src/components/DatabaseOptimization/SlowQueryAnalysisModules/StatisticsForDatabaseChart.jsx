import React, { Component } from 'react';
import { Card } from 'antd';
import PropTypes from 'prop-types';
import ReactEcharts from 'echarts-for-react';

export default class StatisticsForDatabaseChart extends Component {
  static propTypes={
    statisticsForDatabase:PropTypes.object.isRequired
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
          overflow: "truncate",
          width: 35,
          textStyle: {
            color: '#4D5964',
            fontSize: '12'
          },
          formatter: function (name) {
            if (name.length > 5) {
              name = name.slice(0, 5) + "...";
            }
            return name;
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
        height:'80%',
        top: '5%'
      },
      series: [
        {
          data: this.state.ydata,
          type: 'bar',

          itemStyle: {
            color: "#9185F0",
          },
        }
      ]
    };
  }
  UNSAFE_componentWillReceiveProps (nextProps) {

    let xdataArr = []
    let ydataArr = []

    Object.keys(nextProps?.statisticsForDatabase).forEach(function (key, i, v) {
      ydataArr.push(nextProps.statisticsForDatabase[key])
      xdataArr = v
    })
     
      this.setState({
        xdata: xdataArr,
        ydata: ydataArr
      })
   
  }
  render () {
    return (
      <div>
        <Card title="Statistics For Database" style={{ height: '278px' }}>
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
