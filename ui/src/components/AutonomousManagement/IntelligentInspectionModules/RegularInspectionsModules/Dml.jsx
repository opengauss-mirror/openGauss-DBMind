import React, { Component } from 'react';
import { Card, Empty } from 'antd';
import PropTypes from 'prop-types';
import ReactEcharts from 'echarts-for-react';

export default class DistributionChart extends Component {
  static propTypes={
    dmlData:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      xdata: [],
      ydata: [],
      ifShow: true,
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
    if (JSON.stringify(nextProps.dmlData) !== '{}') {
      Object.keys(nextProps.dmlData).forEach(function (key, i, v) {
        ydataArr.push(nextProps.dmlData[key])
        xdataArr = v
      })
      this.setState({
        xdata: xdataArr,
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
      <div>
        <Card title="DML" className="tps">
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
