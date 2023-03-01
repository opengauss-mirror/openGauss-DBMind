import React, { Component } from 'react';
import { Col, message, Row, Spin } from 'antd';
import TopShowList from './IndexTuningModules/TopShowList';
import SuggestionsChangeChart from './IndexTuningModules/SuggestionsChangeChart';
import ImprovementRateChart from './IndexTuningModules/ImprovementRateChart';
import InvalidIndexChart from './IndexTuningModules/InvalidIndexChart';
import InvalidIndexesChange from './IndexTuningModules/InvalidIndexesChange';
import RedundantIndexesChangeChart from './IndexTuningModules/RedundantIndexesChangeChart';
import AdvisedIndexes from './IndexTuningModules/AdvisedIndexes';
import PositiveSql from './IndexTuningModules/PositiveSql';
import ExistingIndexes from './IndexTuningModules/ExistingIndexes';
import { getIndexTuningInterface, getPositiveSqlCount, getExistingIndexesCount } from '../../api/databaseOptimization';
import {
  ApartmentOutlined,
  BorderOuterOutlined,
  BoxPlotOutlined,
  CustomerServiceOutlined,
  DatabaseOutlined,
  NodeIndexOutlined,
  PartitionOutlined,
  TableOutlined
} from '@ant-design/icons';
const iconimg = [<DatabaseOutlined key="1"/>, <TableOutlined key="2"/>, < ApartmentOutlined key="3"/>, <CustomerServiceOutlined key="4"/>, <NodeIndexOutlined key="5"/>, < PartitionOutlined key="6"/>, < NodeIndexOutlined key="7"/>, <BorderOuterOutlined key="8"/>, <BoxPlotOutlined key="9"/>, <DatabaseOutlined key="10"/>, <TableOutlined key="11"/>, < ApartmentOutlined key="12"/>, <CustomerServiceOutlined key="13"/>,]
export default class IndexTuning extends Component {
  constructor(props) {
    super(props)
    this.state = {
      topList: [],
      advisedIndexes: {},
      positiveSQL: {},
      suggestions: {},
      invalidIndexes: {},
      redundantIndexes: {},
      promoteSqlRate: {},
      invalidIndexData: [],
      showflag: true,
      existing_indexes: {},
    }
  }
  async getIndexTuning (params) {
    const { success, data, msg } = await getIndexTuningInterface(params)
    if (success) {
      this.getPositiveSqlCount();
      this.getExistingIndexesCount();
      let topListData = []
      let InvalidIndexArr = []
      this.setState({showflag: false},
        Object.keys(data).forEach(function (key, i) {
          if (key !== 'advised_indexes' && key !== 'existing_indexes' && key !== 'improvement_rate' && key !== 'invalid_indexes' && key !== 'positive_sql' && key !== 'redundant_indexes' && key !== 'suggestions' && key !== 'valid_index') {
            let topName=''
            if(key.indexOf('sql')!==-1){
            topName=(key.replace(/_/g, ' ')).replace('sql', 'SQL')
            } else if(key.indexOf('db')!==-1)  {
              topName=(key.replace(/_/g, ' ')).replace('db', 'DB')
            } else{
              topName=key.replace(/_/g, ' ')
            }        
            let obj = {
              name: topName,
              num: data[key],
              img: iconimg[i],
              key:topName
            }
            topListData.push(obj)
          } else if (key === 'valid_index') {
            let obj = {
              name: key,
              value: data[key],
            }
            InvalidIndexArr.push(obj)
          }
        })
      )
      this.setState({
        topList: topListData,
        suggestions: data.suggestions,
        promoteSqlRate: (data.improvement_rate)*1,
        invalidIndexData: InvalidIndexArr,
        invalidIndexes: data.invalid_indexes,
        redundantIndexes: data.redundant_indexes,
        advisedIndexes: data.advised_indexes,
        positiveSQL: data.positive_sql,
        existing_indexes: data.existing_indexes
      })
    } else {
      message.error(msg)
    }
  }
  async getPositiveSqlCount () {
    const { success, data, msg } = await getPositiveSqlCount()
    if (success) {
      let dataObj = this.state.positiveSQL;
      dataObj['total'] = data;
      this.setState(() => ({
        positiveSQL: dataObj
      }))
    } else {
      message.error(msg)
    }
  }
  async getExistingIndexesCount () {
    const { success, data, msg } = await getExistingIndexesCount()
    if (success) {
      let dataObj = this.state.existing_indexes;
      dataObj['total'] = data;
      this.setState(() => ({
        existing_indexes: dataObj
      }))
    } else {
      message.error(msg)
    }
  }
  componentDidMount () {
    this.props.onRef && this.props.onRef(this);
    this.getIndexTuning({
      positive_current: 1,
      positive_pagesize: 10,
      existing_current: 1,
      existing_pagesize: 10})
  }
  handleRefresh () {
    this.setState({showflag: true}, () => {
      this.getIndexTuning({
        positive_current: 1,
        positive_pagesize: 10,
        existing_current: 1,
        existing_pagesize: 10})
    })
  }
  componentWillUnmount = () => {
      this.setState = () => {return}
  }
  render () {
    return (
      <div style={{ textAlign: 'center' }}>
        {this.state.showflag ? <Spin style={{ margin: '260px 0 ' }} /> :
          <>
            <TopShowList topList={this.state.topList} />
            <SuggestionsChangeChart suggestions={this.state.suggestions} />
            <Row gutter={16} className="mb-20">
              <Col className="gutter-row" span={6} >
                <ImprovementRateChart promoteSqlRate={this.state.promoteSqlRate} />
              </Col>
              <Col className="gutter-row" span={6}>
                <InvalidIndexChart invalidIndexData={this.state.invalidIndexData} />
              </Col>
              <Col className="gutter-row" span={6}>
                <InvalidIndexesChange invalidIndexes={this.state.invalidIndexes} />
              </Col>
              <Col className="gutter-row" span={6}>
                <RedundantIndexesChangeChart redundantIndexes={this.state.redundantIndexes} />
              </Col>
            </Row>
            <AdvisedIndexes advisedIndexes={this.state.advisedIndexes} />
            <PositiveSql positiveSQL={this.state.positiveSQL} />
            <ExistingIndexes existing_indexes={this.state.existing_indexes} />
          </>
        }
      </div>
    )
  }
}
