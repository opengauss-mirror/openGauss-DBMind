import React, { Component } from 'react';
import { Card, Table, message, Modal} from 'antd';
import ResizeableTitle from '../common/ResizeableTitle';
import { getCollectionTable } from '../../api/overview';
import iconokgreen from '../../assets/imgs/iconokgreen.png';
import iconstop from '../../assets/imgs/iconstop.png';
import { capitalizeFirst } from '../../utils/function';

export default class CollectionTable extends Component {
  constructor() {
    super()
    this.state = {
      dataSource: [],
      columns: [],
      isModalVisible:false,
      suggestions:[]
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  async getCollectionTable () {
    const { success, data, msg } = await getCollectionTable()
    if (success) {
      this.handleTableData(data.header, data.rows,data.suggestions)
    } else {
      message.error(msg)
    }
  }
  handleTableData (header, rows,suggestions) {
    let historyColumObj = {}
    let tableHeader = []
    header.forEach(item => {
      historyColumObj = {
        title: capitalizeFirst(item.replace(/_/g, ' ')),
        dataIndex: item,
        key: item,
        align:item === 'is_alive' ? 'center' : 'left',
        ellipsis: true,
        width:item === 'is_alive' ? '20%' : '40%',
        render: (row, record) => {
          if(item === 'is_alive'){
            return <img src={record.is_alive ? iconokgreen : iconstop} alt="" className='iconstyle'></img>
          } else {
            return row
          }
        },
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
      dataSource: res,
      columns: tableHeader,
      suggestions:suggestions
    }))
  }
  isMore(flg) {
    if(flg){
      this.setState({
        isModalVisible: true
      })
    }
  }
  handleCancel = () => {
    this.setState({
      isModalVisible: false,
    })
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
  componentDidMount () {
    this.getCollectionTable()
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
      <Card title="Collection" className='instancename' style={{ height: 288}} extra={<img src={this.state.suggestions.length ? iconstop : iconokgreen} alt="" onClick={() => { this.isMore(this.state.suggestions.length) }} className='iconstyle' style={{width:20}}></img>} >
        <div className='overviewTable'>
          <Table bordered components={this.components} columns={columns} dataSource={this.state.dataSource} size="small" rowKey={record => record.key} pagination={false} style={{ height: 198, overflowY: 'auto' }} scroll={{ x: '100%'}}/>
        </div>
        <Modal title="Suggestions" style={{maxWidth: "40vw"}} bodyStyle={{overflowY: "auto",height: "30vh",}} width="40vw" okButtonProps={{ style: { display: 'none' } }} 
         destroyOnClose='true' visible={this.state.isModalVisible} maskClosable = {false} centered='true' onCancel={() => this.handleCancel()}>
           {this.state.suggestions.map((item,index) => {
                return (
                  <div style={{color:'#272727',textAlign:'left',fontWeight:500}}><span>{index+1 + '. '}</span><span>{item}</span></div>
                )
              })}
        </Modal>
      </Card>
    )
  }
}
