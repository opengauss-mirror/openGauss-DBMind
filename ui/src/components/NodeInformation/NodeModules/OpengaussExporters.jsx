import React, { Component } from 'react';
import * as echarts from 'echarts';
import ReactEcharts from 'echarts-for-react';
import { Card, message, Empty, Spin } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import '../../../assets/css/main/clusterInfo.css';
import { getstatusNodeInterface } from '../../../api/clusterInformation';

const chartItemStyle = {
  symbol: 'rect',
  symbolSize: [120, 30],
  label: {
    color: '#1890ff',
    fontSize: 13,
  },
  emphasis: {
    itemStyle: {
      color: 'rgba(24,144,255,1)',
    },
    label: {
      color: '#ffffff',
    },
  },
}

export default class TopologicalGraph extends Component {
  constructor() {
    super()
    this.state = {
      choosedNode: null,
      chartdata: [],
      links: [],
      categories: [{
        name: '类目0'//根
      }, {
        name: '类目1'//叶
      }],
      isModalVisible: false,
      newnodeName: '',
      showFlag: 0
    }
  }
  initChart () {
    let myChart = echarts.init(document.getElementById('exportersChart'))
    let option = {
      tooltip: {},
      animationDurationUpdate: 1500,
      animationEasingUpdate: 'quinticInOut',
      series: [
        {
          type: 'graph',
          layout: 'circular',
          symbol: 'circle',
          symbolSize: [520, 190],
          label: {
            show: true,
            color: '#fff'
          },
          edgeSymbol: ['circle', 'arrow'],
          edgeSymbolSize: [8, 10],
          edgeLabel: {
            fontSize: 16
          },
          data: this.state.chartdata,
          links: this.state.links,
        },
      ],
    }
    myChart.setOption(option, true)
    myChart.on('click', (params) => {
      this.setState({
        choosedNode: params.data.name
      })
    });
  }
  getOption = () => {
    return {
      tooltip: {},
      animationDurationUpdate: 1500,
      animationEasingUpdate: 'quinticInOut',
      series: [
        {
          type: 'graph',
          layout: 'circular',
          symbol: 'circle',
          symbolSize: [520, 190],
          label: {
            show: true,
            color: '#fff'
          },
          edgeSymbol: ['circle', 'arrow'],
          edgeSymbolSize: [8, 10],
          edgeLabel: {
            fontSize: 16
          },
          data: this.state.chartdata,
          links: this.state.links,
        },
      ],
    }
  }
  async getstatusNode () {
    const { success, data, msg } = await getstatusNodeInterface()
    if (success) {
      if(data.node_list.length>0){
      let chartData = []
      let linkArrFlag = []
      if (data.topo.root.length === 1 && data.topo.leaf.length > 0) {//一个root多个leaf(√)
        let rootArrFlag = []
        let leafArrFlag = []
        data.topo.leaf.forEach((item, index) => {
          let obj = {
            id: index + 'leaf',
            name: item.address,
            value: item.datname,
            itemStyle: {
              color: 'rgba(221,238,255,1)',
              borderColor: '#2aa2ff',
              borderRadius: 100
            },
            ...chartItemStyle
          }
          let linkObj = {
            id: index + 'leaf',
            source: data.topo.root[0].address,
            target: item.address,
            lineStyle: {
              width: 1,
              color: 'red'
            }
          }
          leafArrFlag.push(obj)
          linkArrFlag.push(linkObj)
        })
        data.topo.root.forEach((item, index) => {
          let obj = {
            id: index + 'root',
            name: item.address,
            value: item.datname,
            itemStyle: {
              color: '#ffd79f',
              borderColor: '#2aa2ff',
              borderRadius: 100
            },
            ...chartItemStyle
          }
          rootArrFlag.push(obj)
        })
        chartData = rootArrFlag.concat(leafArrFlag)
      } else if (data.topo.root.length === 0 && data.topo.leaf.length > 0) {//多个leaf
        data.topo.leaf.forEach((item, index) => {
          let obj = {
            id: index,
            name: item.address,
            value: item.datname,
            itemStyle: {
              color: 'rgba(221,238,255,1)',
              borderColor: '#2aa2ff',
              borderRadius: 100
            },
            ...chartItemStyle
          }
          chartData.push(obj)
        })
        linkArrFlag = []
      } else if (data.topo.leaf.length === 0 && data.topo.root.length > 0) {//多个root
        data.topo.root.forEach((item, index) => {
          let obj = {
            id: index,
            name: item.address,
            value: item.datname,
            itemStyle: {
              color: '#ffd79f',
              borderColor: '#2aa2ff',
              borderRadius: 100
            },
            ...chartItemStyle
          }
          chartData.push(obj)
        })
        linkArrFlag = []
      } else if (data.topo.leaf.length > 0 && data.topo.root.length > 1) {//多个root多个leaf
        let rootArrFlag = []
        let leafArrFlag = []
        data.topo.root.forEach((item, index) => {
          let obj = {
            id: index,
            name: item.address,
            value: item.datname,
            itemStyle: {
              color: '#ffd79f',
              borderColor: '#2aa2ff',
              borderRadius: 100
            },
            ...chartItemStyle
          }
          rootArrFlag.push(obj)
        })
        data.topo.leaf.forEach((item, index) => {
          let obj = {
            id: index,
            name: item.address,
            value: item.datname,
            itemStyle: {
              color: 'rgba(221,238,255,1)',
              borderColor: '#2aa2ff',
              borderRadius: 100
            },
            ...chartItemStyle
          }
          leafArrFlag.push(obj)
        })
        chartData = rootArrFlag.concat(leafArrFlag)
        for (let i in data.topo.root) {
          for (let j in data.topo.leaf) {
            let obj = {
              source: i.address,
              target: j.address
            }
            linkArrFlag.push(obj)
          }
        }
      }
      this.setState({
        showFlag: 0,
        chartdata: chartData,
        links: linkArrFlag
      }, () => {
        this.getOption()
      })
    }else{
      this.setState({showFlag: 1})
    }
    } else {
      this.setState({showFlag: 1})
      message.error(msg)
    }
  }
  onChange = (e) => {
    this.setState({
      newnodeName: e.target.value
    })
  }
  handleRefresh () {
    this.setState({
      showFlag: 2
    }, () => {
      this.getstatusNode()
    })
  }
  componentDidMount () {
    this.getstatusNode()
    this.getOption()
  }
  componentWillUnmount () {
      this.setState = () => {return}
  }
  render () {
    return (
      <div>
        <Card title="openGauss Exporters" extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />} style={{ height: 350, padding: 0 }}>
          {
            this.state.showFlag === 0 ?
              <ReactEcharts
                ref={(e) => {
                  this.echartsElement = e
                }}
                option={this.getOption()}
                style={{ height: 250 }}
                lazyUpdate={true}
              >
              </ReactEcharts>
              : this.state.showFlag === 1 ? <Empty description={false} style={{ paddingTop: 50 }} /> : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>
          }
        </Card>
      </div>
    )
  }
}