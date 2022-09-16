import React, { Component } from 'react';
import { Card } from 'antd';
import PropTypes from 'prop-types';
import ReactEcharts from 'echarts-for-react';

export default class DistributionChart extends Component {
  static propTypes={
    distribution:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      xdata: [],
      ydata: []
    }
  }
  getOption () {
    return {
      tooltip: { show: true },
      xAxis: {
        type: 'category',
        axisLabel: {
          interval: 0,
          textStyle: {
            color: '#314b71',
            fontSize: '12'
          },
        },
        data: this.state.xdata
      },
      yAxis: {
        type: 'value',
        offset: -20,
        axisLabel: {
          textStyle: {
            color: '#314b71',
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
          showBackground: true,
          backgroundStyle: {
            color: 'rgba(180, 180, 180, 0.2)'
          }
        }
      ]
    };
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    let xdataArr = []
    let ydataArr = []
    Object.keys(nextProps.distribution).forEach(function (key, i, v) {
      ydataArr.push(nextProps.distribution[key])
      xdataArr = v
    })
    this.setState({
      xdata: xdataArr,
      ydata: ydataArr
    })
  }
  render () {
    return (
      <div className="mb-20" >
        <Card title="Distribution of slow Query">
          <ReactEcharts
            ref={(e) => {
              this.echartsElement = e
            }}
            option={this.getOption()}
            style={{ width: '100%', height: '200px' }}
            lazyUpdate={true}
          >
          </ReactEcharts>
        </Card>
      </div>
    )
  }
}
