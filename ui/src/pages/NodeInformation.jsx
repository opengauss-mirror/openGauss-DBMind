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
    this.state = {
      activeKey: '1'
    }
  }
  componentDidMount() {
    // 恢复上次选中的顶层Tab（System resource/DB）
    const saved = sessionStorage.getItem('nodeinfo.active')
    if (saved) {
      try {
        const val = JSON.parse(saved)
        if (val === '1' || val === '2') {
          this.setState({ activeKey: val })
        }
      } catch (e) {}
    }
  }
  onTopTabChange = (key) => {
    this.setState({ activeKey: key })
    sessionStorage.setItem('nodeinfo.active', JSON.stringify(key))
  }
  render () {
    return (
      <div className="contentWrap nodestyle">
        <Tabs size={'large'} activeKey={this.state.activeKey} onChange={this.onTopTabChange}>
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
