import React, {Component} from 'react';
import {Tabs} from 'antd';
import {CheckOutlined, DesktopOutlined, PieChartOutlined} from '@ant-design/icons';
import '../assets/css/common.css';
import Node from '../components/NodeInformation/Node';
import Host from '../components/NodeInformation/Host';
import Statistic from '../components/NodeInformation/Statistics';

const { TabPane } = Tabs;
export default class Cluster extends Component {
  constructor() {
    super()
    this.state = {}
  }
  render () {
    return (
      <div className="contentWrap">
        <Tabs tabBarGutter={0} type="card" size={'small'}>
          <TabPane
            tab={
              <span>
                <CheckOutlined />
                Node
              </span>
            }
            key="1"
          >
            <Node />
          </TabPane>
          <TabPane
            tab={
              <span>
                <DesktopOutlined />
                Host
              </span>
            }
            key="2"
          >
            <Host />
          </TabPane>
          <TabPane
            tab={
              <span>
                <PieChartOutlined />
                Statistics
              </span>
            }
            key="3"
          >
            <Statistic />
          </TabPane>
        </Tabs>
      </div>
    );
  }
}