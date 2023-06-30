import React from 'react';
import { Redirect, Switch } from 'react-router-dom';
import { Route } from 'react-router-dom';
import { Layout } from 'antd';
import HeaderTop from './components/Header.jsx';
import MenuLeft from './components/MenuLeft.jsx';
import Overview from './pages/Overview';
import NodeInformation from './pages/NodeInformation.jsx';
import Alarms from './components/AutonomousManagement/Alarms';
import SecurityManagement from './components/AutonomousManagement/SecurityManagement';
import IntelligentInspection from './components/AutonomousManagement/IntelligentInspection';
import IndexTuning from './components/DatabaseOptimization/IndexTuning';
import DatabaseTuning from './components/DatabaseOptimization/DatabaseTuning';
import SlowQueryAnalysis from './components/DatabaseOptimization/SlowQueryAnalysis';
import RegularInspections from './components/DatabaseOptimization/RegularInspections';
import IndexAdvisor from './components/AiToolkit/IndexAdvisor';
import QueryTuning from './components/AiToolkit/QueryTuning';
import IntelligentSqlAnalysis from './components/AiToolkit/IntelligentSqlAnalysis';
import IntelligentSqlCondition from './components/DatabaseOptimization/IntelligentSqlCondition';
import RiskAnalysis from './components/AiToolkit/RiskAnalysis';
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
                <Route path="/AutonomouseManagement/nodeinfor" component={NodeInformation}></Route>
                <Route path="/AutonomouseManagement/alarms" component={Alarms}></Route>
                <Route path="/AutonomouseManagement/securitymanagement" component={SecurityManagement}></Route>
                <Route path="/AutonomouseManagement/intelligentInspection" component={IntelligentInspection}></Route>
                <Route path="/DatabaseOptimization/indexTuning" component={IndexTuning}></Route>
                <Route path="/DatabaseOptimization/databaseTuning" component={DatabaseTuning}></Route>
                <Route path="/DatabaseOptimization/slowqueryanalysis" component={SlowQueryAnalysis}></Route>
                <Route path="/DatabaseOptimization/regularinspections" component={RegularInspections}></Route>
                <Route path="/DatabaseOptimization/intelligentsqlcondition" component={IntelligentSqlCondition}></Route>
                <Route path="/Aitoolkit/indexadvisor" component={IndexAdvisor}></Route>
                <Route path="/Aitoolkit/querytuning" component={QueryTuning}></Route>
                <Route path="/Aitoolkit/intelligentsqlanalysis" component={IntelligentSqlAnalysis}></Route>
                <Route path="/Aitoolkit/riskanalysis" component={RiskAnalysis}></Route>
                <Route path="/dbmind-settings" component={DbmindSettings}></Route>
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
