import React from 'react';
import { Button, Form, Input, message, Select} from 'antd';
import { ArrowRightOutlined, UnlockOutlined, UserOutlined  } from '@ant-design/icons';
import '../assets/css/logIn.css';
import db from '../utils/storage';
import Logo from '../assets/imgs/logotip.png';
import { loginInterface, getAgentListInterface } from '../api/common';

const { Option } = Select;
const style = { marginTop: 30, width: 300 }
class LogIn extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      username: '',
      password: '',
      selValue: '',
      options: [],
      loading:false
    }
  }
  onFinish = async (values) => {
    this.login(values)
  };
  changeSelVal (value) {
    this.setState({selValue: value})
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
  onFinishFailed = () => { };
  async login (value) {
    let params = {
      grant_type: '',
      username: value.username,
      password: value.password,
      scope: this.state.selValue,
      client_id: '',
      client_secret: ''
    }
    this.setState({loading:true})
    await loginInterface(params).then((res) =>{
      if (Object.prototype.hasOwnProperty.call(res,'success')) {
        message.error(res.msg)
      } else {
        this.setState({loading:false})
        db.ss.set('access_token', res.access_token)
        db.ss.set('token_type', res.token_type)
        db.ss.set('user_name', value.username)
        db.ss.set('expires_in', res.expires_in)
        db.ss.set('Instance_value', this.state.selValue)
        this.props.history.push('/overview')
      }
    }).catch(()=>{
      this.setState({loading:false})
    })
  }
  componentDidMount () {
    this.getItemList()
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
                  name="database"
                  rules={[
                    {
                      required: true,
                      message: 'Please select an option.',
                    }
                  ]}
                  style={style}
                >
                  <Select value={this.state.selValue} className="LogInTwoBtn"  placeholder="Instance List" onChange={(val) => { this.changeSelVal(val) }} showSearch
                    optionFilterProp="children" filterOption={(input, option) =>
                      option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 275, borderRadius: 10}}>
                    {
                      this.state.options.map(item => {
                        return (
                          <Option value={item.key} key={item} title={item.value}>{item.key}</Option>
                        )
                      })
                    }
                  </Select>
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
