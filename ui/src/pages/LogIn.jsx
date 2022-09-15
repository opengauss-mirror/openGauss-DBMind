import React from 'react';
import { Button, Form, Input, message } from 'antd';
import { ArrowRightOutlined, UnlockOutlined, UserOutlined  } from '@ant-design/icons';
import '../assets/css/logIn.css';
import db from '../utils/storage';
import Logo from '../assets/imgs/logotip.png';
import { loginInterface } from '../api/common';

const style = { marginTop: 30, width: 400 }
class LogIn extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      username: '',
      password: '',
      loading:false
    }
  }
  onFinish = async (values) => {
    this.login(values)
  };
  onFinishFailed = () => { };
  async login (value) {
    let params = {
      grant_type: '',
      username: value.username,
      password: value.password,
      scope: '',
      client_id: '',
      client_secret: ''
    }
    this.setState({loading:true})
    const res = await loginInterface(params)
      if (Object.prototype.hasOwnProperty.call(res,'success')) {
      message.error(res.msg)
    } else {
      this.setState({loading:false})
      db.ss.set('access_token', res.access_token)
      db.ss.set('token_type', res.token_type)
      db.ss.set('user_name', value.username)
      db.ss.set('expires_in', res.expires_in)
      this.props.history.push('/overview')
    }
  }
  render () {
    return (
      <div className="loginbox">
        <div className="loginwrap">
          <img src={Logo} alt="" style={{ width: '30%' }} className="logintip" />
          <div className="btnboxrow">
            <Form
              name="basic"
              layout="inline"
              labelCol={{ span: 2 }}
              wrapperCol={{ span: 22 }}
              initialValues={{ remember: true }}
              onFinish={this.onFinish}
              onFinishFailed={this.onFinishFailed}
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
              <Form.Item
                wrapperCol={{ offset: 4, span: 18 }}
                style={{ marginTop: 33 }}
              >
                <Button type="primary" htmlType="submit"  icon={<ArrowRightOutlined />} loading={this.state.loading} style={{ borderRadius: 10, width: 80 }}></Button>
              </Form.Item>
            </Form>
          </div>
        </div>
      </div>
    )
  }
}
export default LogIn;
