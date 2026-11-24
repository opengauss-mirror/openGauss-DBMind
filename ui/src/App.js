/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-DBMind is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

import React from 'react';
import { Route, Router, Switch } from 'react-router-dom';
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
