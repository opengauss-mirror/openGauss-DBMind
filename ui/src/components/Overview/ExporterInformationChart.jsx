import React, { Component } from 'react';
import { ReloadOutlined } from '@ant-design/icons';
import cn from '../../assets/imgs/cn.png';
import dn from '../../assets/imgs/dn.png';
import instance from '../../assets/imgs/instance.png';
import master from '../../assets/imgs/master.png';
import slave from '../../assets/imgs/slave.png';
import '../../assets/css/main/overview.css';
import { Card, Row, Col, Empty, message, Spin } from 'antd';
import { getClusterInformationInterface } from '../../api/overview';
export default class ExporterInformationChart extends Component {
  constructor() {
    super()
    this.state = {
      clusterInfos: [],
      showFlag: 0
    }
  }
  async getClusterInformation () {
    const { success, data, msg } = await getClusterInformationInterface()
    if (success) {
      if (JSON.stringify(data) !== '{}') {
        let arr = [], imgs = [{ img: instance, name: 'Exporters' }, { img: cn, name: 'CN Exporters' }, { img: dn, 'name': 'DN Exporters' }, { img: master, name: 'Master Exporters' }, { img: slave, name: 'Slave Exporters' }]
        Object.keys(data.cluster_summary).forEach(function (key) {
          if (key !== 'deployment_form' && key !== 'version') {
            let obj = {
              name: key,
              num: data.cluster_summary[key]
            }
            arr.push(obj)
          }
        })
        arr.forEach((item) => {
          imgs.forEach((it) => {
            let itemName = (item.name).toLocaleLowerCase()
            let itName = ((it.name).split(/\s+/))[0].toLocaleLowerCase()
            if (itName.indexOf(itemName) !== -1) {
              item['imgShow'] = it.img
              item['clusterName'] = it.name
            }
          })
        })
        this.setState({
          showFlag: 0,
          clusterInfos: arr
        })
      } else {
        this.setState({showFlag: 1})
        message.error(msg)
      }
    } else {
      this.setState({showFlag: 1})
      message.error(msg)
    }
  }
  handleRefresh () {
    this.setState({showFlag: 2}, () => {
      this.getClusterInformation()
    })
  }
  componentDidMount () {
    this.getClusterInformation()
  }
  render () {
    return (
      <div className="clusterInfo">
        <Card title="Exporter Information" style={{ height: 350 }} extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />}>
          {this.state.showFlag === 0 ? <>
            <Row gutter={16} style={{ justifyContent: 'center' }}>
              {
                this.state.clusterInfos.map((item) => {
                  return (
                    <Col className="gutter-row" span={8} key={item.name}>
                      <div className="clusterCont">
                        <img src={item.imgShow} alt="" />
                        <h3 >{item.clusterName}</h3>
                        <h4>{item.num}</h4>
                      </div>
                    </Col>
                  )
                })
              }
            </Row>
          </> : this.state.showFlag === 1 ? <Empty description={false} style={{ paddingTop: 50 }} /> : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>}
        </Card>
      </div>
    )
  }
}
