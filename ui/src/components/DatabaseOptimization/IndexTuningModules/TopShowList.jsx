import React, { Component } from 'react';
import { Col, Row, Space } from 'antd';
import PropTypes from 'prop-types';
import '../../../assets/css/main/databaseOptimization.css';

let widthNum = 0

export default class TopShowList extends Component {
  static propTypes={
    topList:PropTypes.array.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {}
  }
  render () {
    widthNum = (100 / this.props.topList.length).toFixed(2) + '%'
    const style = { maxWidth: '14.2%!important', flex: `0 0 ${widthNum}` }
    return (
      <div className="mb-20" >
        <Row gutter={16} >
          {
            this.props.topList.map(item =>
              <Col key={item.name} style={style}>
                <div className="dataBase" style={{ background: '#fff', height: 200, padding: '40px 20px' }}>
                  <h3 style={{ height: 52, color: '#b4b4b4', width: '100%', fontSize: 14 }}>{item.name}</h3>
                  <Space>
                    {item.img}
                    <span style={{ fontSize: 22 }}>{item.num}</span>
                  </Space>
                </div>
              </Col>)
          }
        </Row>
      </div >
    )
  }
}
