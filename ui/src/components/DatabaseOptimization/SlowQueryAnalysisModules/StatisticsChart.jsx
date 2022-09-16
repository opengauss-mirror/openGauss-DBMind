import React, {Component} from 'react';
import {Card} from 'antd';
import PropTypes from 'prop-types';
import ReactEcharts from 'echarts-for-react';

export default class StatisticsChart extends Component {
  static propTypes={
    statistics:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      seriesData: [],
      legendData: []
    }
  }
  getOption = () => {
    return {
      tooltip: {
        trigger: 'item',
        formatter: '{a} <br/>{b} : {c} ({d}%)'
      },
      legend: {
        show: true,
        type: 'scroll',
        orient: 'vertical',
        right: 40,
        top: 10,
        bottom: 20,
        name: this.state.legendData
      },
      series: [
        {
          name: 'data',
          type: 'pie',
          radius: '80%',
          center: ['35%', '50%'],
          data: this.state.seriesData,
          label: {
            show: false,
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
    let legendArr = []
    let seriesArr = []
    if (nextProps.statistics.rows.length > 0) {
      nextProps.statistics.rows.forEach(element => {
        let obj = {
          name: element[0],
          value: element[1],
        }
        legendArr.push(element[0])
        seriesArr.push(obj)
      });
    }
    this.setState({
      seriesData: seriesArr,
      legendArr: legendArr
    })
  }
  render () {
    return (
      <div className="mb-20" >
        <Card title="Statistics for Slow Query Template">
          <ReactEcharts
            ref={(e) => {
              this.echartsElement = e
            }}
            option={this.getOption()}
            style={{ width: '100%', height: '250px' }}
            lazyUpdate={true}
          >
          </ReactEcharts>
        </Card>
      </div>
    )
  }
}
