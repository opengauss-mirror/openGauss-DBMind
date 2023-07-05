import React, { Component } from 'react';
import { Card, Table, message } from 'antd';
import PropTypes from 'prop-types';
import ResizeableTitle from '../../common/ResizeableTitle';
import { formatTableTitle } from '../../../utils/function';

export default class InvalidIndexesChange extends Component {
  static propTypes={
    invalidIndexes:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      dataSource: [],
      columns: [],
      pagination: {
        total: 0,
        defaultCurrent: 1
      },
      loading: false
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  handleTableData (header, rows, total) {
    this.setState({loading: true})
    if (header.length > 0) {
      let historyColumObj = {}
      let tableHeader = []
      header.forEach(item => {
        historyColumObj = {
          title: formatTableTitle(item),
          dataIndex: item,
          ellipsis: true,
          width: 180,
          render: (row, record) => {
            if(item === 'insert'){
              return <div><span>{record.insert}</span><div className='insertclass'><span style={{width:`${record.select * 0.96}%`,backgroundColor:'#FDC000'}}></span><span style={{width:`${record.delete * 0.96}%`,backgroundColor:'#F36900'}}></span><span style={{width:`${record.update * 0.96}%`,backgroundColor:'#50C291'}}></span><span style={{width:`${record.insert * 0.96}%`,backgroundColor:'#6D8FF0'}}></span></div></div>
            } else {
              return row
            }
          }
        }
        tableHeader.push(historyColumObj)
      })
      let res = []
      rows.forEach((item, index) => {
        let tabledata = {}
        for (let i = 0; i < header.length; i++) {
          tabledata[header[i]] = item[i]
        }
        tabledata['key'] = index + ''
        res.push(tabledata)
      });
      this.setState(() => ({
        loading: false,
        dataSource: res,
        columns: tableHeader,
        pagination: {
          total: res.length,
          defaultCurrent: 1
        }
      }))
    }
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
  UNSAFE_componentWillReceiveProps (props) {
    this.props=props
    this.handleTableData(props.invalidIndexes.header, props.invalidIndexes.rows)
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
      <div className="mb-10">
        <Card title="Invalid Indexes">
          <Table size="small" bordered components={this.components} columns={columns} dataSource={this.state.dataSource} rowKey={record => record.key} pagination={this.state.pagination} loading={this.state.loading} scroll={{ x: '100%'}}/>
        </Card>
      </div>
    )
  }
}

