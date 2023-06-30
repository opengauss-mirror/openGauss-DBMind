import React, { Component } from 'react';
import { Table,Col, Row, Empty, message } from 'antd';
import { UpOutlined, DownOutlined } from '@ant-design/icons';
import NodeEchartFormWork from '../NodeInformation/NodeModules/NodeEchartFormWork';
import ResizeableTitle from '../common/ResizeableTitle';
import { getStorageData } from '../../api/autonomousManagement';
import SystemImg from '../../assets/imgs/System.png';
import { formatTimestamp, formatTableTitle } from '../../utils/function';

export default class Storage extends Component {
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
  async getStorageData1 () {
    let param = {
      instance:this.state.selValue,
      minutes:0,
      label:'node_filesystem_size_bytes',
      fetch:true
    }
    const { success, data, msg }= await getStorageData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getStorageData2 () {
    let param = {
      instance:this.state.selValue,
      minutes:this.state.selTimeValue,
      label:'os_disk_usage',
      fetch:true
    }
    const { success, data, msg }= await getStorageData(param)
    if (success) {
      return data
    } else {
      message.error(msg)
    }
  }
  async getStorageDataAll () {
    Promise.all([
      this.getStorageData1(),
      this.getStorageData2()
    ]).then((result)=>{
      if(result[0]){
          let tableHeader = [],historyColumObj = {},tableData = [],echartsData = [],lastData = [],res = [],echartData = [],
          header = ['Disk name','Mountpoint','Total space (GB)','Used space (GB)','Usage rate']
          result[0].forEach((aitem, aindex) => {
            result[1].forEach((bitem, bindex) => {
            if(aitem.labels.instance.split(':')[0] === bitem.labels.from_instance && aitem.labels.device === bitem.labels.device){
              tableData.push(aitem)
              echartsData.push(bitem)
              }
            });
          });
          let echartsitem = {'legend':[{image:SystemImg,description:'Usage Rate'}],
          'xAxisData':echartsData[0].timestamps,
          'seriesData':[{data:echartsData[0].values,description:'Usage Rate',colors:'#2DA769'}],'flg':1,'legendFlg':1,'unit':'%'}
          echartData.push(echartsitem)
          lastData.push(echartsData[0].values[echartsData[0].values.length-1])
          tableData.forEach((item, index) => {
            let tabledata = {}
            tabledata["Disk name"] = item.labels.device
            tabledata["Mountpoint"] = item.labels.mountpoint
            tabledata["Total space (GB)"] = (item.values/1024/1024/1024).toFixed(2)
            tabledata["Used space (GB)"] = (item.values/1024/1024/1024*lastData[index]).toFixed(2)
            tabledata["Usage rate"] = (lastData[index]*100).toFixed(2)+'%'
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
              ...(index > 1 && { sorter: (a, b) => {
                let aVal = a[item]
                let bVal = b[item]
                let c = isFinite(aVal),
                  d = isFinite(bVal);
                return (c !== d && d - c) || (c && d ? aVal - bVal : aVal.localeCompare(bVal));
              } })
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
      }
      }).catch((error) => {
        console.log('error', error)
      })
    }
    componentDidUpdate(prevProps) {
      if(prevProps.selValue !== this.props.selValue || prevProps.selTimeValue !== this.props.selTimeValue || prevProps.tabkey !== this.props.tabkey) {
        this.setState(() => ({
          selValue: this.props.selValue,selTimeValue: this.props.selTimeValue
        }),()=>{
          if(this.props.tabkey === "5"){
            this.getStorageDataAll()
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
        return <span style={{ color: 'black' }}  onClick={e => {
            props.onExpand(props.record, e);
        }}><UpOutlined /></span>
    } else {
        return <span style={{ color: 'black' }}  onClick={e => {
            props.onExpand(props.record, e);
        }}><DownOutlined /></span>
    }
  }
  componentDidMount () {
    this.getStorageDataAll()
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
                  <NodeEchartFormWork  echartData={this.state.echartData[index]}/>
                )
              }}
            />
      </div>
    )
  }
}
