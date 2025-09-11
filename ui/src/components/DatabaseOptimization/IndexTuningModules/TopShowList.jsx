import React, { Component } from 'react';
import { Col, Row, Space } from 'antd';
import PropTypes from 'prop-types';
import '../../../assets/css/main/databaseOptimization.css';
import { formatTableTitleToUpper } from '../../../utils/function';
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
      <div className="mb-10" >
        <Row gutter={10} >
          {
            this.props.topList.map(item =>
              <Col key={item.name} style={style}>
                <div className="dataBase topShowList" style={{ background: '#fff', height: 100, padding: '20px 30px' }}>
                   <div style={{display:'flex',justifyContent: "space-between"}} ><h3 style={{color: '#b4b4b4', fontSize: 14 }}>{formatTableTitleToUpper(item.name)}</h3>
                    <Space>{item.img}</Space>
                    </div>
                    <p style={{ fontSize: 18,textAlign: "left" }}>{item.num}</p>
                  
                </div>
              </Col>)
          }

        </Row>
      </div >
    )
  }
}
