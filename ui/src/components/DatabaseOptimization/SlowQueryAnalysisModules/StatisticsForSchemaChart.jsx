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
        width: '45%',
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
          label: {
            show: false,
            position: 'bottom',
            fontSize: '16px',
            color: 'auto'
          },
          left: '20%',
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
          color: ['#5470c6', '#1890ff'],
          data: this.state.chartData
        }
      ]
    };
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    let dataArr = []
    Object.keys(nextProps.statisticsforSchema).forEach(function (key) {
      let obj = {
        name: key,
        value: nextProps.statisticsforSchema[key]
      }
      dataArr.push(obj)
    })
    this.setState({chartData: dataArr})
  }
  render () {
    return (
      <div >
        <Card title="Statistics For Schema" style={{ height: 280 }}>
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
