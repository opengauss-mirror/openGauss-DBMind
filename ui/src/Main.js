import React from 'react';
import { Redirect, Switch } from 'react-router-dom';
import { Route } from 'react-router-dom';
import { Layout } from 'antd';
import HeaderTop from './components/Header.jsx';
import MenuLeft from './components/MenuLeft.jsx';
import Overview from './pages/Overview';
import NodeInformation from './pages/NodeInformation.jsx';
import AutonormousManagement from './pages/AutonomousManagement.jsx';
import DatabaseOptimization from './pages/DatabaseOptimization';
import SecurityManagement from './pages/SecurityManagement';
import AiToolkit from './pages/AiToolkit.jsx';
import DbmindSettings from './pages/DbmindSettings';
import Foot from './components/Foot';
import 'antd/dist/antd.css';
import './assets/css/main/index.css';

const { Content } = Layout;
class Main extends React.Component {
  state = {
    collapsed: false,
  };
  toggleCollapsed = () => {
    this.setState({
      collapsed: !this.state.collapsed,
    });
  };
  render() {
    return (
      <div style={{ height: 'calc(100% - 60px)' }}>
        <HeaderTop />
        <Layout className="container" id="maincontainer">
          <MenuLeft />
          <Layout style={{ width: '85%', position: 'relative', }}>
            <Content className="contentBag" style={{ overflowY: 'auto', overflowX: 'hidden', marginBottom: 40 }}>
              <Switch>
                <Route path="/overview" component={Overview}></Route>
                <Route path="/nodeinfor" component={NodeInformation}></Route>
                <Route path="/autonomousmanagement" component={AutonormousManagement}></Route>
                <Route path="/databaseoptimization" component={DatabaseOptimization}></Route>
                <Route path="/securitymanagement" component={SecurityManagement}></Route>
                <Route path="/dbmind-settings" component={DbmindSettings}></Route>
                <Route path="/aitoolkit" component={AiToolkit}></Route>
                <Redirect to="/login"></Redirect>
              </Switch>
              <Foot />
            </Content>
          </Layout>
        </Layout>
      </div>
    )
  }
}
export default Main;
