import React, { Component } from 'react';
import { Card, Empty } from 'antd';
import PropTypes from 'prop-types';
import ReactEcharts from 'echarts-for-react';

export default class DistributionRootCause extends Component {
  static propTypes={
    distributionRootCause:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      ydata: [],
      ifShow: true,
    }
  }
  getOption () {
    return {
      tooltip: {
        trigger: 'item'
      },
      legend: {
        orient: 'center',
        left: 'right'
      },
      series: [
        {
          type: 'pie',
          radius: ['40%', '80%'],
          center: ['30%', '50%'],
          data: this.state.ydata,
          label: {
            normal: {
               position: 'inner',
               show : false
            }
          },
          emphasis: {
            itemStyle: {
              shadowBlur: 10,
              shadowOffsetX: 0,
              shadowColor: 'rgba(0, 0, 0, 0.5)'
            }
          }
        }
      ]
    };
  }
  UNSAFE_componentWillReceiveProps (nextProps) {
    let ydataArr = []
    if (JSON.stringify(nextProps.distributionRootCause) !== '{}') {
      Object.keys(nextProps.distributionRootCause).forEach(function (key, i, v) {
        ydataArr.push({value:nextProps.distributionRootCause[key],name:key})
      })
      this.setState({
        ydata: ydataArr
      })
    } else {
      this.setState({
        ifShow: false
      })
    }
  }
  render () {
    return (
      <div style={{ textAlign: 'center' }}>
        <Card title="Distribution Root Cause">
        {this.state.ifShow ? <ReactEcharts
            ref={(e) => {
              this.echartsElement = e
            }}
            option={this.getOption()}
            style={{ width: '100%', height: '200px' }}
            lazyUpdate={true}
          >
          </ReactEcharts> : <Empty description={this.state.ifShow} style={{ height: 200, paddingTop: 50 }} />}
        </Card>
      </div>
    )
  }
}
