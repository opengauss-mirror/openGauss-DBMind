import React, {Component} from 'react';
import {Col, Row, Space} from 'antd';
import PropTypes from 'prop-types';
import StatisticsForDatabaseChart from './StatisticsForDatabaseChart';
import StatisticsForSchemaChart from './StatisticsForSchemaChart';

export default class TopShowList extends Component {
  static propTypes={
    toplist:PropTypes.array.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      toplistData: []
    }
  }
  render () {
    return (
      <div className="mb-20">
        <Row gutter={16}>
          {
            this.props.toplist.map(item =>
              <Col span={4} key={item.name}>
                <div className="dataBase" style={{ background: '#fff', height: 280, padding: '40px' }}>
                  <h3 style={{ height: 52, color: '#b4b4b4', width: '100%', fontSize: 14 }}>{item.name}</h3>
                  <Space>
                    {item.img}
                    <span style={{ fontSize: 22 }}>{item.num}</span>
                  </Space>
                </div>
              </Col>
            )
          }
          <Col className="gutter-row" span={6}>
            <div>
              <StatisticsForDatabaseChart statisticsForDatabase={this.props.statisticsForDatabase} />
            </div>
          </Col>
          <Col className="gutter-row" span={6}>
            <div>
              <StatisticsForSchemaChart statisticsforSchema={this.props.statisticsforSchema} />
            </div>
          </Col>
        </Row>
      </div >
    )
  }
}
