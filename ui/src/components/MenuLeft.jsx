import React, { Component } from 'react';
import 'antd/dist/antd.css';
import '../assets/css/main/index.css';
import { Layout, Menu } from 'antd';
import { withRouter } from 'react-router-dom';
import menuList from '../router/menu.js';

const { Sider } = Layout;
class MenuLeft extends Component {
  constructor(props) {
    super(props)
    this.state = {
      collapsed: false,
    };
  }
  toggle = () => {
    this.setState({
      collapsed: !this.state.collapsed,
    });
  };
  render () {
    let path = this.props.location.pathname
    path = '/' + path.split('/')[1]
    const { history } = this.props
    const onClick = (MenuItem) => {
      history.push(MenuItem.key)
    }
    return (
      <Sider collapsible collapsed={this.state.collapsed} onCollapse={this.toggle} width={this.state.collapsed ? '40px' : '260px'}>
        <Menu theme="dark" mode="inline" selectedKeys={[path]} items={menuList} onClick={onClick}></Menu>
      </Sider >
    )
  }
}
export default withRouter(MenuLeft)
