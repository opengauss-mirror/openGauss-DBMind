import React from 'react';
import { Route, HashRouter as Router, Switch } from 'react-router-dom';
import history from './utils/history';
import LogIn from './pages/LogIn.jsx';
import Main from './Main';

function App() {
  return (
    <Router history={history}>
      <Switch>
        <Route path="/login" component={LogIn} />
        <Route path="/" component={Main} />
      </Switch>
    </Router>
  );
}
export default App;
