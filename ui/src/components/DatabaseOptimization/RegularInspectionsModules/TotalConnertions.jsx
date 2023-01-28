import React, { Component } from 'react';
import { Card, Table } from 'antd';
import PropTypes from 'prop-types';
import ResizeableTitle from '../../common/ResizeableTitle';
import '../../../assets/css/main/databaseOptimization.css';
import { formatTableTitle } from '../../../utils/function';

export default class TotalConnertions extends Component {
  static propTypes={
    totalConnertions:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      dataSource: [],
      columns: []
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  handleTableData (data) {
    let arr = []
    let columnArr = []
    let headNameOne,headNameTwo = ''
    Object.keys(data).forEach(function (key, i, v) {
      if(i === 0){
        headNameOne = key
        headNameTwo = data[key]
      }
      if(i > 0){
        arr.push({[headNameOne]:key,[headNameTwo]:data[key]})
      }
    })
    let columnName = {},columnName1 = {}
    Object.keys(data).forEach(function (key, i, v) {
      if(i === 0){
        columnName = {
          title: formatTableTitle(key),
          dataIndex: key,
          key: key,
          ellipsis: true,
        }
        columnName1 = {
          title: data[key],
          dataIndex: data[key],
          key: data[key],
          ellipsis: true,
        }
        columnArr.push(columnName)
        columnArr.push(columnName1)
      }
    })
    this.setState(() => ({
      dataSource: arr,
      columns: columnArr,
    }))
  }
  handleResize = index => (e, { size }) => {
    this.setState(({ columns }) => {
      const nextColumns = [...columns];
      nextColumns[index] = {
        ...nextColumns[index],
        width: size.width,
      };
      return { columns: nextColumns };
    });
  };
  UNSAFE_componentWillReceiveProps (nextProps) {
    this.handleTableData(nextProps.totalConnertions)
  }
  render () {
    const columns = this.state.columns.map((col, index) => ({
      ...col,
      onHeaderCell: column => ({
        width: column.width,
        onResize: this.handleResize(index)
      })
    }))
    return (
      <div style={{ textAlign: 'center' }} className='tableDiv'>
        <Card title="Total Connertions">
          <Table bordered components={this.components} columns={columns} dataSource={this.state.dataSource} size="small" rowKey={record => record.key} pagination={false} style={{ height: 157, overflowY: 'auto' }} scroll={{ x: '100%'}}/>
        </Card>
      </div>
    )
  }
}
