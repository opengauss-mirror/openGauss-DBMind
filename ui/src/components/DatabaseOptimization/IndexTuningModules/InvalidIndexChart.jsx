import React, { Component } from 'react';
import { Card } from 'antd';
import * as echarts from 'echarts';
import PropTypes from 'prop-types';

export default class InvalidIndexChart extends Component {
  static propTypes={
    invalidIndexData:PropTypes.array.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      chartVal: []
    }
  }
  componentDidMount () {
    this.initChart()
  }
  initChart () {
    let myChart = echarts.init(document.getElementById('indexTuningChart3'))
    let option = {
      tooltip: {
        trigger: 'item'
      },
      legend: {
        top: '0%',
        left: '0%',
        width: '10%',
      },
      label: {
        show: true
      },
      title: {
        show: true
      },
      series: [
        {
          name: '',
          type: 'pie',
          radius: ['50%', '80%'],
          avoidLabelOverlap: false,
          left: '0%',
          bottom: 5,
          itemStyle: {
            borderColor: '#fff',
            borderWidth: 0
          },
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
          data: this.state.chartVal
        }
      ]
    }
    myChart.setOption(option)
    myChart.resize()
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    this.setState({chartVal: nextProps.invalidIndexData}, () => {
      this.initChart()
    })
  }
  render () {
    return (
      <div>
        <Card title="Valid Index">
          <div id="indexTuningChart3" style={{ width: '100%', height: 200 }}></div>
        </Card>
      </div>
    )
  }
}
