import React, { Component } from 'react';
import 'antd/dist/antd.css';
import '../assets/css/main/header.css';
import { DownOutlined, ExportOutlined, UserOutlined, UnlockOutlined } from '@ant-design/icons';
import { Dropdown, Menu, Select, Modal, Form, Input, message } from 'antd';
import LogoDB from '../assets/imgs/dlogo.png';
import { withRouter } from 'react-router-dom';
import db from '../utils/storage';
import { loginInterface, getAgentListInterface } from '../api/common';

const { Option } = Select;
const style = { marginBottom: 30, marginLeft: 98, marginRight: 86, width: 300 };
class HeaderTop extends Component {
  dataForm = React.createRef();
  constructor(props) {
    super(props)
    this.state = {
      userInfo: '',
      selOldValue: '',
      selNewValue: '',
      options: [],
      isModalVisible: false,
    };
  }
  logout = () => {
    db.ss.remove('access_token')
    db.ss.remove('token_type')
    db.ss.remove('user_name')
    db.ss.remove('expires_in')
    db.ss.remove('Instance_value')
    this.props.history.push('/login')
  }
  changeSelVal (value) {
    this.setState({isModalVisible: true})
    this.setState({selNewValue: value})
  }
  async getItemList () {
    const { success, data, msg } = await getAgentListInterface()
    let optionArr = []
    Object.keys(data).forEach(function (key) {
      optionArr.push({key:key,value:data[key].toString()})
    })
    if (success) {
      this.setState({options: optionArr})
    } else {
      message.error(msg)
    }
  }
  okHandle = async () => {
    const fieldsValue = await this.dataForm.current.validateFields();
    this.changeAgent(fieldsValue)
  };
  async changeAgent (value) {
    let params = {
      grant_type: '',
      username: value.username,
      password: value.password,
      scope: this.state.selNewValue,
      client_id: '',
      client_secret: ''
    }
    await loginInterface(params).then((res) =>{
      if (Object.prototype.hasOwnProperty.call(res,'success')) {
        message.error(res.msg)
      } else {
        db.ss.set('access_token', res.access_token)
        db.ss.set('token_type', res.token_type)
        db.ss.set('user_name', value.username)
        db.ss.set('expires_in', res.expires_in)
        db.ss.set('Instance_value', this.state.selNewValue)
        this.setState({isModalVisible: false,selOldValue:this.state.selNewValue})
        window.location.reload()
      }
    }).catch(()=>{
    })
  }
  handleCancel = () => {
    this.setState({isModalVisible: false})
  }
  componentDidMount () {
    this.getItemList();
    this.setState({
      userInfo: db.ss.get('user_name'),
      selOldValue: db.ss.get('Instance_value')
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
          <Select value={this.state.selOldValue} className="changeInstance"  placeholder="Instance List" onChange={(val) => { this.changeSelVal(val) }} showSearch
                   bordered={false} optionFilterProp="children" filterOption={(input, option) =>
                      option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 180,display: 'flex',color:'#c0bcbc',marginRight:20}}>
                    {
                      this.state.options.map(item => {
                        return (
                          <Option value={item.key} key={item} title={item.value}>{item.key}</Option>
                        )
                      })
                    }
          </Select>
            <div className="userInfo2">
              <Dropdown overlay={menu}>
                <div className="info">
                  <UserOutlined style={{ fontSize: 18, marginRight: 10 }} /><span className="infoName">{this.state.userInfo}</span><DownOutlined />
                </div>
              </Dropdown>
            </div>
          </div>
        </div>
        <Modal title="Change Instance"  destroyOnClose='true' visible={this.state.isModalVisible} maskClosable = {false} 
              centered='true' onOk={this.okHandle} onCancel={() => this.handleCancel()}>
            <Form
              name="basic"
              ref={this.dataForm}
              layout="inline"
              labelCol={{ span: 2 }}
              wrapperCol={{ span: 22 }}
              initialValues={{ remember: true }}
              autoComplete="off"
            >
              <Form.Item
                name="username"
                rules={[
                  {
                    required: true,
                    message: 'Please enter the user name.',
                  },
                ]}
                style={style}
              >
                <Input size="large" className="LogInTwoBtn" name="username" placeholder="User Name" prefix={<UserOutlined onChange={this.onChange} />} />
              </Form.Item>
              <Form.Item
                name="password"
                rules={[
                  {
                    required: true,
                    message: 'Please enter the password.',
                  },
                ]}
                style={style}
              >
                <Input.Password size="large" className="LogInTwoBtn" name="password" placeholder="Password" prefix={<UnlockOutlined onChange={this.onChange} />} />
              </Form.Item>
            </Form>
        </Modal>
      </div>
    )
  }
}
export default withRouter(HeaderTop)
