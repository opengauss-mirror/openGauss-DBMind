import React, {Component} from 'react';
import 'antd/dist/antd.css';
import '../assets/css/main/header.css';
import {DownOutlined, ExportOutlined, UserOutlined} from '@ant-design/icons';
import {Dropdown, Menu} from 'antd';
import LogoDB from '../assets/imgs/dlogo.png';
import {withRouter} from 'react-router-dom';
import db from '../utils/storage';

class HeaderTop extends Component {
  constructor(props) {
    super(props)
    this.state = {
      userInfo: '',
    };
  }
  logout = () => {
    db.ss.remove('access_token')
    db.ss.remove('token_type')
    db.ss.remove('user_name')
    db.ss.remove('expires_in')
    this.props.history.push('/login')
  }
  componentDidMount () {
    this.setState({
      userInfo: db.ss.get('user_name')
    })
  }
  render () {
    const menu = (
      <Menu style={{ height: 40, zIndex: 1, }}>
        <Menu.Item key="1">
          <span onClick={this.logout} id="Administer" style={{ color: '#000' }}><ExportOutlined style={{ marginRight: 15, fontSize: 16 }} />Log Out</span>
        </Menu.Item>
      </Menu>
    );
    return (
      <div className="menu_top">
        <div className="top">
          <img src={LogoDB} alt="" style={{ float: 'left', width: 168, height: 46 }}></img>
          <div className="userInfoContent">
            <div className="userInfo2">
              <Dropdown overlay={menu}>
                <div className="info">
                  <UserOutlined style={{ fontSize: 18, marginRight: 10 }} /><span className="infoName">{this.state.userInfo}</span><DownOutlined />
                </div>
              </Dropdown>
            </div>
          </div>
        </div>
      </div>
    )
  }
}
export default withRouter(HeaderTop)
