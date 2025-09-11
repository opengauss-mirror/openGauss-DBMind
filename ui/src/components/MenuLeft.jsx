import React, { Component } from 'react';
// 全局样式已在入口统一导入
import { Layout, Menu } from 'antd';
import db from '../utils/storage';
import { withRouter } from 'react-router-dom';
import menuList from '../router/menu.js';

const { Sider } = Layout;
class MenuLeft extends Component {
  constructor(props) {
    super(props)
    this.state = {
      collapsed: false,
      selectedKey:this.props.location.pathname,
      openKey: ''
    };
  }
  toggle = () => {
    this.setState({
      collapsed: !this.state.collapsed,
    });
  };
  componentDidMount(){
    this.openHierarchy()
  }
  openHierarchy(){
    // 优先读取会话内记忆的 openKey
    const remembered = db.ss.get('menu.openKey');
    if (remembered) {
      this.setState({ openKey: remembered });
      return;
    }
    // 回退：根据 URL 推断上层路径作为 openKey
    const { pathname } = this.props.location;
    let path = pathname.split('/');
    if (pathname.split('/').length > 2) {
      path = path[path.length - 2];
      this.setState(() => ({ openKey: '/' + path }));
    }
  }
  onOpenChange = (k) => {
    if (k.length > 1) {
      const key = k[k.length - 1];
      this.setState({ openKey: key });
      db.ss.set('menu.openKey', key);
    } else {
      this.setState({ openKey: '' });
      db.ss.remove('menu.openKey');
    }
  }
  componentDidUpdate(prevProps) {
    if (prevProps.location.pathname !== this.props.location.pathname) {
      this.openHierarchy()
      this.setState({selectedKey:this.props.location.pathname})
    }
  }
  render () {
    const { history } = this.props
    const onClick = (MenuItem) => {
      history.push(MenuItem.key)
      this.setState({selectedKey:MenuItem.key})
    }
    return (
      <Sider collapsible collapsed={this.state.collapsed} onCollapse={this.toggle} width={this.state.collapsed ? '40px' : '260px'}>
        <Menu theme="dark" mode="inline" selectedKeys={[this.state.selectedKey]} items={menuList} onClick={onClick}  onOpenChange={this.onOpenChange} openKeys={[this.state.openKey]}></Menu>
      </Sider >
    )
  }
}
export default withRouter(MenuLeft)
