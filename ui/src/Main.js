import React, { Suspense, lazy } from 'react';
import { Redirect, Switch } from 'react-router-dom';
import { Route } from 'react-router-dom';
import { Layout } from 'antd';
import HeaderTop from './components/Header.jsx';
import SkeletonPage from './components/common/SkeletonPage';
import MenuLeft from './components/MenuLeft.jsx';
// RegularInspections 已下线
import NotFound from './pages/NotFound';
import Foot from './components/Foot';
// 路由页面按需加载，降低首屏体积
// 高频路由开启空闲预取，优化点击切换首屏等待
const Overview = lazy(() => import(/* webpackPrefetch: true */ './pages/Overview'));
const NodeInformation = lazy(() => import(/* webpackPrefetch: true */ './pages/NodeInformation.jsx'));
const Alarms = lazy(() => import('./components/AutonomousManagement/Alarms'));
const SecurityManagement = lazy(() => import('./components/AutonomousManagement/SecurityManagement'));
const IndexTuning = lazy(() => import(/* webpackPrefetch: true */ './components/DatabaseOptimization/IndexTuning'));
const DatabaseTuning = lazy(() => import('./components/DatabaseOptimization/DatabaseTuning'));
const SlowQueryAnalysis = lazy(() => import(/* webpackPrefetch: true */ './components/DatabaseOptimization/SlowQueryAnalysis'));
const IndexAdvisor = lazy(() => import('./components/AiToolkit/IndexAdvisor'));
const QueryTuning = lazy(() => import('./components/AiToolkit/QueryTuning'));
const IntelligentSqlAnalysis = lazy(() => import(/* webpackPrefetch: true */ './components/AiToolkit/IntelligentSqlAnalysis'));
const IntelligentSqlCondition = lazy(() => import('./components/DatabaseOptimization/IntelligentSqlCondition'));
const RiskAnalysis = lazy(() => import('./components/AiToolkit/RiskAnalysis'));
const DbmindSettings = lazy(() => import('./pages/DbmindSettings'));
// antd 样式与全局样式已在入口 index.js 统一导入

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
              <Suspense fallback={<SkeletonPage />}>
                <Switch>
                  <Route path="/overview" component={Overview}></Route>
                  <Route path="/AutonomousManagement/nodeinfor" component={NodeInformation}></Route>
                  <Route path="/AutonomousManagement/alarms" component={Alarms}></Route>
                  <Route path="/AutonomousManagement/securitymanagement" component={SecurityManagement}></Route>
                  <Route path="/DatabaseOptimization/indexTuning" component={IndexTuning}></Route>
                  <Route path="/DatabaseOptimization/databaseTuning" component={DatabaseTuning}></Route>
                  <Route path="/DatabaseOptimization/slowqueryanalysis" component={SlowQueryAnalysis}></Route>
                  {/** 历史路径返回 404 页面 */}
                  <Route path="/DatabaseOptimization/regularinspections" component={NotFound}></Route>
                  <Route path="/DatabaseOptimization/intelligentsqlcondition" component={IntelligentSqlCondition}></Route>
                  <Route path="/Aitoolkit/indexadvisor" component={IndexAdvisor}></Route>
                  <Route path="/Aitoolkit/querytuning" component={QueryTuning}></Route>
                  <Route path="/Aitoolkit/intelligentsqlanalysis" component={IntelligentSqlAnalysis}></Route>
                  <Route path="/Aitoolkit/riskanalysis" component={RiskAnalysis}></Route>
                  <Route path="/dbmind-settings" component={DbmindSettings}></Route>
                  <Redirect to="/overview"></Redirect>
                </Switch>
              </Suspense>
              <Foot />
            </Content>
          </Layout>
        </Layout>
      </div>
    )
  }
}
export default Main;
