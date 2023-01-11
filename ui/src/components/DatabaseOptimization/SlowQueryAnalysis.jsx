
import React, { Component } from 'react';
import { Col, message, Row, Spin } from 'antd';
import TopShowList from './SlowQueryAnalysisModules/TopShowList';
import SystemtableRateChart from './SlowQueryAnalysisModules/SystemtableRateChart';
import SlowquerycountChart from './SlowQueryAnalysisModules/SlowquerycountChart';
import DistributionChart from './SlowQueryAnalysisModules/DistributionChart';
import MeanCpuTimeChart from './SlowQueryAnalysisModules/MeanCpuTimeChart';
import MeanIoTimeChart from './SlowQueryAnalysisModules/MeanIoTimeChart';
import MeanBufferHitRateChart from './SlowQueryAnalysisModules/MeanBufferHitRateChart';
import MeanFetchTimeChart from './SlowQueryAnalysisModules/MeanFetchTimeChart';
import StatisticsChart from './SlowQueryAnalysisModules/StatisticsChart';
import SlowQueryTable from './SlowQueryAnalysisModules/SlowQueryTable';
import TableofSlowQueryTable from './SlowQueryAnalysisModules/TableofSlowQueryTable';
import { getSlowQueryAnalysisInterface, getSlowQueryRecentCount} from '../../api/databaseOptimaztion';
import { FileSearchOutlined, FundOutlined, MonitorOutlined } from '@ant-design/icons';
const iconimg = [<FileSearchOutlined key="1"/>, <MonitorOutlined key="2"/>, <FundOutlined key="3"/>, <FileSearchOutlined key="4"/>, <MonitorOutlined key="5"/>, <FundOutlined key="6"/>]
export default class SlowQueryAnalysis extends Component {
  constructor(props) {
    super(props)
    this.state = {
      showflag: true,
      topList: [],
      statisticsForDatabase: {},
      statisticsforSchema: {},
      sysInSlowQuery: {},
      slowQueryCount: {},
      distribution: {},
      meanCpuTime: {},
      meanIoTime: {},
      meanBufferHitRate: {},
      meanFetchTime: {},
      statistics: {},
      slowQueryTemplate: {},
      tableOfSlowQuery: {}
    }
  }
  async getSlowQueryAnalysis (params) {
    const { success, data, msg } = await getSlowQueryAnalysisInterface(params)
    if (success) {
      this.getSlowQueryRecentCount();
      let toplistArr = []
      this.setState({
        showflag: false,
      }, () => {
        Object.keys(data).forEach(function (key) {
          if (key === 'main_slow_queries' || key === 'nb_unique_slow_queries' || key === 'slow_query_threshold') {
            let obj = {
              name: key === 'slow_query_threshold' ? key.replace(/_/g, ' ') + '    (ms)' : key.replace(/_/g, ' '),
              num: data[key],
              img: iconimg[Math.ceil(Math.random() * 5)]
            }
            toplistArr.push(obj)
          }
        })
      })
      this.setState({
        topList: toplistArr,
        statisticsForDatabase: data.statistics_for_database,
        statisticsforSchema: data.statistics_for_schema,
        sysInSlowQuery: data.systable,
        slowQueryCount: data.slow_query_count,
        distribution: data.distribution,
        meanCpuTime: data.mean_cpu_time,
        meanIoTime: data.mean_io_time,
        meanBufferHitRate: data.mean_buffer_hit_rate,
        meanFetchTime: data.mean_fetch_rate,
        statistics: data.slow_query_template,
        slowQueryTemplate: data.slow_query_template,
        tableOfSlowQuery: data.table_of_slow_query,
      })
    } else {
      message.error(msg)
    }
  }
  async getSlowQueryRecentCount () {
    const { success, data, msg } = await getSlowQueryRecentCount()
    if (success) {
      let dataObj = this.state.tableOfSlowQuery;
      dataObj['total'] = data;
      this.setState(() => ({
        tableOfSlowQuery: dataObj
      }))
    } else {
      message.error(msg)
    }
  }
  handleRefresh () {
    this.setState({showflag: true}, () => {
      this.getSlowQueryAnalysis({current: 1,pagesize: 10})
    })
  }
  componentDidMount () {
    this.props.onRef && this.props.onRef(this);
    this.getSlowQueryAnalysis({current: 1,pagesize: 10})
  }
  render () {
    return (
      <div style={{ textAlign: 'center' }}>
        {
          this.state.showflag ? <Spin style={{ margin: '260px 0 ' }} /> :
            <>
              <TopShowList toplist={this.state.topList} statisticsForDatabase={this.state.statisticsForDatabase} statisticsforSchema={this.state.statisticsforSchema} />
              <Row gutter={16}>
                <Col className="gutter-row" span={6}>
                  <div className="cardShow">
                    <SystemtableRateChart sysInSlowQuery={this.state.sysInSlowQuery} />
                  </div>
                </Col>
                <Col className="gutter-row" span={12}>
                  <div className="cardShow">
                    <SlowquerycountChart slowQueryCount={this.state.slowQueryCount} />
                  </div>
                </Col>
                <Col className="gutter-row" span={6}>
                  <div className="cardShow">
                    <DistributionChart distribution={this.state.distribution} />
                  </div>
                </Col>
              </Row>
              <Row gutter={16}>
                <Col className="gutter-row" span={6}>
                  <div className="cardShow">
                    <MeanCpuTimeChart meanCpuTime={this.state.meanCpuTime} />
                  </div>
                </Col>
                <Col className="gutter-row" span={6}>
                  <div className="cardShow">
                    <MeanIoTimeChart meanIoTime={this.state.meanIoTime} />
                  </div>
                </Col>
                <Col className="gutter-row" span={6}>
                  <div className="cardShow">
                    <MeanBufferHitRateChart meanBufferHitRate={this.state.meanBufferHitRate} />
                  </div>
                </Col>
                <Col className="gutter-row" span={6}>
                  <div className="cardShow">
                    <MeanFetchTimeChart meanFetchTime={this.state.meanFetchTime} />
                  </div>
                </Col>
              </Row>
              {/* ...... */}
              <Row gutter={16}>
                <Col className="gutter-row" span={12}>
                  <div className="cardShow">
                    <StatisticsChart statistics={this.state.statistics} />
                  </div>
                </Col>
                <Col className="gutter-row" span={12}>
                  <div className="cardShow">
                    <SlowQueryTable slowQueryTemplate={this.state.slowQueryTemplate} />
                  </div>
                </Col>
              </Row>
              {/* ..... */}
              <TableofSlowQueryTable tableOfSlowQuery={this.state.tableOfSlowQuery} />
            </>
        }

      </div>
    )
  }
}
