import React, { Component } from 'react';
import { Table,Col, Row, Empty, message } from 'antd';
import { UpOutlined, DownOutlined } from '@ant-design/icons';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import ResizeableTitle from '../common/ResizeableTitle';
import { getServiceCapabilityData } from '../../api/autonomousManagement';
import SystemImg from '../../assets/imgs/System.png';
import { formatTimestamp, formatTableTitle } from '../../utils/function';

export default class CapacityMetric extends Component {
  constructor(props) {
    super(props)
    this.state = {
      lockDataSource: [],
      columns: [],
      lockPagination: {
        total: 0,
        defaultCurrent: 1
      },
      loadingLock: false,
      echartData:[],
      selValue:this.props.selValue,
      selTimeValue:this.props.selTimeValue,
    }
  }

  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  async getCapacityMetricData () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'pg_database_size_bytes',
      fetch:true
    }
    const { success, data, msg }= await getServiceCapabilityData(param)
    if (success) {
        let tableHeader = [],historyColumObj = {},res = [],echartData = [],
        header = ['Database','Used Space (MB)']
        data.forEach((item, index) => {
          let seriesData = [],tabledata = {}
          item.values.forEach((bitem, index) => {
            seriesData.push(bitem.toFixed(2))
          });
          let echartsitem = {'legend':[{image:'',description:'Usage Space (MB)'}],
          'xAxisData':item.timestamps,
          'seriesData':[{data:seriesData,description:'Usage Space (MB)',colors:'#5990FD'}],'flg':0,'legendFlg':2,title:'Usage Space (MB)','unit':''}
          echartData.push(echartsitem)
          tabledata["Database"] = item.labels.datname
          tabledata["Used Space (MB)"] = (item.values[item.values.length-1]).toFixed(2)
          tabledata['key'] = index
          res.push(tabledata)
        });
        header.forEach((item,index) => {
          historyColumObj = {
            title: formatTableTitle(item),
            dataIndex: item,
            ellipsis: true,
            width: 180,
            key: index,
            ...(index && { sorter: (a, b) => {
              let aVal = a[item]
              let bVal = b[item]
              let c = isFinite(aVal),
                d = isFinite(bVal);
              return (c !== d && d - c) || (c && d ? aVal - bVal : aVal.localeCompare(bVal));
            } }),

          }
          tableHeader.push(historyColumObj)
        })
        this.setState(() => ({
          loadingLock: false,
          lockDataSource: res,
          columns: tableHeader,
          echartData:echartData,
          lockPagination: {
            total: res.length,
            defaultCurrent: 1
          }
        }))
    } else {
      this.setState({
        loadingLock: false,
        lockDataSource: [],
        columns: [],
      })
      message.error(msg)
    }
    }
    componentDidUpdate(prevProps) {
      if(prevProps.selValue !== this.props.selValue || prevProps.selTimeValue !== this.props.selTimeValue || prevProps.tabkey !== this.props.tabkey) {
        this.setState(() => ({
          selValue: this.props.selValue,selTimeValue: this.props.selTimeValue
        }),()=>{
          if(this.props.tabkey === "5"){
            this.getCapacityMetricData()
          }
        })
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
  customExpandIcon(props) {
    if (props.expanded) {
        return <span style={{ color: 'black' }} onClick={e => {
            props.onExpand(props.record, e);
        }}><UpOutlined /></span>
    } else {
        return <span style={{ color: 'black' }} onClick={e => {
            props.onExpand(props.record, e);
        }}><DownOutlined /></span>
    }
  }
  componentDidMount () {
    this.getCapacityMetricData()
  }
  render() {
    const columns = this.state.columns.map((col, index) => ({
      ...col,
      onHeaderCell: column => ({
        width: column.width,
        onResize: this.handleResize(index)
      })
    }))
    return (
      <div>
            <Table
              rowKey={record => record.key} columns={columns} components={this.components} dataSource={this.state.lockDataSource}
              pagination={this.state.lockPagination} loading={this.state.loadingLock} scroll={{ x: '100%'}}
              expandIcon={(props) => this.customExpandIcon(props)}
              expandable={{
                expandedRowRender: (record,index) => (
                  <NodeEchartFormWork  echartData={this.state.echartData[record.key]}/>
                )
              }}
            />
      </div>
    )
  }
}
