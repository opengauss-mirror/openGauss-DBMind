import React, { Component } from 'react';
import { Tabs } from 'antd';
import '../assets/css/common.css';
import '../assets/css/main/nodeinformation.css';
import Node from '../components/NodeInformation/Node';
import Host from '../components/NodeInformation/Host';

const { TabPane } = Tabs;
export default class Cluster extends Component {
  constructor() {
    super()
    this.state = {}
  }
  render () {
    return (
      <div className="contentWrap nodestyle">
        <Tabs   size={'large'}>
          <TabPane
            tab={
              <span>
                System resource
              </span>
            }
            key="1"
          >
            <Node />
          </TabPane>
          <TabPane
            tab={
              <span>
                DB
              </span>
            }
            key="2"
          >
            <Host />
          </TabPane>
        </Tabs>
      </div>
    );
  }
}