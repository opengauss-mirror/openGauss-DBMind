import React, { Component } from 'react';
import { Card } from 'antd';
import PropTypes from 'prop-types';
import ReactEcharts from 'echarts-for-react';

export default class SystemtableRateChart extends Component {
  static propTypes={
    sysInSlowQuery:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      chartData: []
    }
  }
  getOption = () => {
    return {
      tooltip: {
        trigger: 'item'
      },
      legend: {
        top: '0%',
        left: '0%',
        width: '10%',
        show: true
      },
      label: {
        show: true
      },
      title: {
        show: true
      },
      series: [
        {
          name: 'Transaction State',
          type: 'pie',
          radius: ['40%', '80%'],
          avoidLabelOverlap: false,
          itemStyle: {
            borderColor: '#fff',
            borderWidth: 0
          },
          left: '20%',
          label: {
            show: false,
            position: 'bottom',
            fontSize: '16px',
            color: 'auto'
          },
          emphasis: {
            itemStyle: {
              shadowBlur: 10,
              shadowOffsetX: 0,
              shadowColor: 'rgba(0, 0, 0,0.5)'
            }
          },
          labelLine: {
            show: false
          },
          color: ['#9fe080', '#5470c6'],
          data: this.state.chartData
        }
      ]
    };
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    let dataArr = []
    Object.keys(nextProps.sysInSlowQuery).forEach(function (key) {
      let obj = {
        name: key,
        value: nextProps.sysInSlowQuery[key]
      }
      dataArr.push(obj)
    })
    this.setState({chartData: dataArr})
  }
  render () {
    return (
      <div className="mb-20" >
        <Card title="System table Rate In Slow Query">
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
