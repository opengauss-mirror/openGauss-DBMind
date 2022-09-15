import React from 'react';
import { ReloadOutlined } from '@ant-design/icons';
import { Card, Row, Col, Form, Input, message, Spin } from 'antd';
import '../../assets/css/common.css';
import { getClusterSummaryInterface } from '../../api/clusterInformation';
const { TextArea } = Input;
export default class Statistics extends React.PureComponent {
  constructor() {
    super()
    this.state = {
      layout: {
        labelCol: {
          span: 8,
        },
        wrapperCol: {
          span: 20,
        },
      },
      formData: {},
      RuntimeFormData: {},
      visilbe: false,
      showFlag: 0,
    }
  }
  async getClusterSummary () {
    const { success, data, msg } = await getClusterSummaryInterface()
    if (success) {
      this.setState(() => ({
        formData: { ...data.cluster_summary },
        RuntimeFormData: data.runtime,
        visilbe: true,
        showFlag: 0
      }))
    } else {
      message.error(msg)
    }
  }
  handleRefresh () {
    this.setState({
      visilbe: false,
      showFlag: 2
    }, () => {
      this.getClusterSummary()
    })
  }
  componentDidMount () {
    this.getClusterSummary()
  }
  render () {
    return (
      <div>
        <div>
          {
            this.state.showFlag === 0 ? <>
              <Card className="mb-20" style={{ height: 400 }} title="Exporter Summary" extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />} >
                {
                  this.state.visilbe && (
                    <Row gutter={192} style={{ padding: '0 50px' }} className="mlr-0">
                      <Col className="gutter-row plr-0" span={12}>
                        <Form {...this.state.layout} name="nest-messages" initialValues={{ ...this.state.formData }}>
                          <Form.Item
                            name="deployment_form"
                            label="Deployment Form"
                          >
                            <Input disabled />
                          </Form.Item>
                          <Form.Item
                            label="CN"
                            name="cn"
                          >
                            <Input disabled />
                          </Form.Item>
                          <Form.Item
                            label="DN"
                            name="dn"
                          >
                            <Input disabled />
                          </Form.Item>
                          <Form.Item label="Exporters" name="exporters">
                            <Input disabled />
                          </Form.Item>
                        </Form>
                      </Col>
                      <Col className="gutter-row plr-0" span={12}>
                        <Form {...this.state.layout} name="nest-messages" initialValues={{ ...this.state.formData }}>
                          <Form.Item
                            name="version"
                            label="openGauss Version"
                          >
                            <TextArea disabled rows={4} />
                          </Form.Item>
                          <Form.Item
                            label="Master"
                            name="master"
                          >
                            <Input disabled />
                          </Form.Item>
                          <Form.Item
                            label="Slave"
                            name="slave"
                          >
                            <Input disabled />
                          </Form.Item>
                        </Form>
                      </Col>
                    </Row>
                  )
                }
              </Card>
              <Card style={{ height: 400 }} title="Runtime Configuration" extra={<ReloadOutlined className="more_link" onClick={() => { this.handleRefresh() }} />} >
                {
                  this.state.visilbe && (
                    <Row gutter={192} style={{ padding: '0 50px' }} className="mlr-0">
                      <Col className="gutter-row plr-0" span={12}>
                        <Form {...this.state.layout} name="nest-messages" initialValues={{ ...this.state.RuntimeFormData }}>
                          <Form.Item
                            name="python_version"
                            label="Python Version"
                          >
                            <Input disabled />
                          </Form.Item>
                          <Form.Item
                            label="Deployment User"
                            name="deployment_user"
                          >
                            <Input disabled />
                          </Form.Item>
                          <Form.Item
                            label="Python File Path"
                            name="python_file_path"
                          >
                            <TextArea disabled rows={3} />
                          </Form.Item>
                        </Form>
                      </Col>
                      <Col className="gutter-row plr-0" span={12}>
                        <Form {...this.state.layout} name="nest-messages" initialValues={{ ...this.state.RuntimeFormData }}>
                          <Form.Item
                            name="python_path"
                            label="Python Path"
                          >
                            <Input disabled />
                          </Form.Item>
                          <Form.Item
                            label="LD_LIBRARY_PATH"
                            name="ld_library_path"
                          >
                            <Input disabled />
                          </Form.Item>
                          <Form.Item
                            label="Path"
                            name="path"
                          >
                            <TextArea disabled rows={3} />
                          </Form.Item>
                        </Form>
                      </Col>
                    </Row>
                  )}
              </Card>
            </> : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '300px auto' }} /> </div>
          }
        </div>
      </div>
    )
  }
}
