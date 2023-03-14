import React, { Component } from 'react';
import { UploadOutlined, SettingFilled, InfoCircleFilled } from '@ant-design/icons';
import { Button, Card, Input, message, Select, Table, Upload, Modal, InputNumber, Tooltip, Progress } from 'antd';
import { getItemListInterface, getListIndexAdvisorInterface, getListIndexAdvisorDefaultValue } from '../../api/aiTool';
import { formatTableTitle } from '../../utils/function';
import db from '../../utils/storage';

const { TextArea } = Input;
const { Option } = Select;
const labelStyle = {width:160,float:'left',textAlign:'right',lineHeight:'32px'}
const inputStyle = {marginLeft:20,marginRight:20}
export default class IndexAdvisor extends Component {
  constructor(props) {
    super(props)
    this.state = {
      columns: [],
      columnsRedundant: [],
      columnsUseless: [],
      columnsDetails: [],
      data: [],
      dataRedundant: [],
      dataUseless: [],
      dataDetails: [],
      pageSize: 10,
      current: 1,
      total: 0,
      paginationRedundant: {
        total: 0,
        defaultCurrent: 1
      },
      paginationUseless: {
        total: 0,
        defaultCurrent: 1
      },
      paginationDetails: {
        total: 0,
        defaultCurrent: 1
      },
      maxIndexNum: 10,
      maxIndexStorage: 100,
      minImprovedRate: '3.0',
      isDetailsVisible:false,
      isSettingVisible:false,
      selValue: '',
      textareaVal: '',
      options: [],
      fileList: '',
      flgNum:0,
      loadingAdvisor:false
    }
  }
  async getItemList () {
    const { success, data, msg } = await getItemListInterface()
    if (success) {
      this.setState({options: data})
    } else {
      message.error(msg)
    }
  }
  async getDefaultValue () {
    const { success, data, msg } = await getListIndexAdvisorDefaultValue()
    if (success) {
      this.setState({
        minImprovedRate: data.min_improved_rate,
        maxIndexNum: data.max_index_num,
        maxIndexStorage: data.max_index_storage},()=>{
        db.ss.set('maxIndexNum', data.max_index_num)
        db.ss.set('maxIndexStorage', data.max_index_storage)
        db.ss.set('minImprovedRate', data.min_improved_rate)
      })
    } else {
      message.error(msg)
    }
  }
  async getListIndexAdvisor (arrs,pageParams) {
    let params = {
      database: this.state.selValue,
      textareaVal: [arrs],
      max_index_num: this.state.maxIndexNum,
      max_index_storage: this.state.maxIndexStorage,
      min_improved_rate: this.state.minImprovedRate,
      current: pageParams ? pageParams.current : this.state.current,
      pagesize:pageParams ? pageParams.pagesize : this.state.pageSize
    }
    this.setState({ loadingAdvisor: true });
    const { success, data, msg } = await getListIndexAdvisorInterface(params)
    if (success) {
      let advisorColumObj = {},advisorHeader = ["index","improve_rate","index_size","templates","select","delete","update","insert"],advisorTableHeader = [],
      redundantColumObj = [],redundantHeader = ["schemaName","tbName","columns","statement","existingIndex"],redundantTableHeader = [],
      uselessColumObj = [],uselessHeader = ["schemaName","tbName","columns","statement"],uselessTableHeader = [],widthArray = ['34%','12%','12%','12%','5%','5%','5%','15%']
      advisorHeader.forEach((item,Index) => {
        advisorColumObj = {
          title: formatTableTitle(item),
          dataIndex: item,
          key: item,
          ellipsis: true,
          width:widthArray[Index],
          render: (row, record) => {
            if(item === 'templates'){
              return <Button type="link"  onClick={() => this.isDetails(row, record)}>Details</Button>
            } else if(item === 'insert'){
              return <div><span>{record.insert}</span><div className='insertclass'><span style={{width:`${record.select * 0.96}%`,backgroundColor:'#FDC000'}}></span><span style={{width:`${record.delete * 0.96}%`,backgroundColor:'#F36900'}}></span><span style={{width:`${record.update * 0.96}%`,backgroundColor:'#50C291'}}></span><span style={{width:`${record.insert * 0.96}%`,backgroundColor:'#6D8FF0'}}></span></div></div>
            } else {
              return row
            }
          }
        }
        advisorTableHeader.push(advisorColumObj)
      })
      redundantHeader.forEach((item) => {
        redundantColumObj = {
          title: formatTableTitle(item),
          dataIndex: item,
          key: item,
          ellipsis: true,
          render: (row, record) => {
            if(item === 'existingIndex'){
              return row.toString()
            } else {
              return row
            }
          }
        }
        redundantTableHeader.push(redundantColumObj)
      })
      uselessHeader.forEach((item) => {
        uselessColumObj = {
          title: formatTableTitle(item),
          dataIndex: item,
          key: item,
          ellipsis: true,
        }
        uselessTableHeader.push(uselessColumObj)
      })
      if (data[0].advise_indexes || data[0].useless_indexes || data[0].redundant_indexes) {
        let res = [],redundantRes = [],uselessRes = [],
        dataObj = [{"rows":data[0]['advise_indexes'],"header":advisorHeader},{"rows":data[0]['redundant_indexes'],"header":redundantHeader},{"rows":data[0]['useless_indexes'],"header":uselessHeader}],
        arrayObj = [res,redundantRes,uselessRes]
        dataObj.forEach((item, index) => {
          item.rows.forEach((oitem, oindex) => {
            let tabledata = {}
            for (let i = 0; i < item.header.length; i++) {
              tabledata[item.header[i]] = oitem[item.header[i]]
              tabledata['key'] = oindex
            }
            arrayObj[index].push(tabledata)
          });
        })
        this.setState(() => ({
          loadingAdvisor: false,
          data: res,
          dataRedundant: redundantRes,
          dataUseless: uselessRes,
          columns: advisorTableHeader,
          columnsRedundant: redundantTableHeader,
          columnsUseless: uselessTableHeader,
          pageSize: this.state.pageSize,
          current: this.state.current,
          total:data[0].total,
          paginationRedundant: {
            total: redundantRes.length,
            defaultCurrent: 1
          },
          paginationUseless: {
            total: uselessRes.length,
            defaultCurrent: 1
          },
        }))
      } else {
        this.setState({
          loadingAdvisor: false,
        })
        message.warning('No data.')
      }
    } else {
      this.setState({
        loadingAdvisor: false,
      })
      message.error(msg)
    }
  }
  addTableData = (type) => {
    if (this.state.selValue === '') {
      message.warning('Please choose a database.')
    } else if (this.state.textareaVal === '' && type === 'type1') {
      message.warning('Please enter the content of SQL statements.')
    } else {
      if(type === 'type1'){
        this.getListIndexAdvisor(this.state.textareaVal,{pagesize: 10,current: 1})
      }
    }
  }
  isDetails(row, record) {
    let detailsColumObj = [],detailsTableHeader = [],widthArray = ['50%','25%','25%'],detailsHeader = ['template','count','improve']
    detailsHeader.forEach((item,Index) => {
      detailsColumObj = {
        title: formatTableTitle(item),
        dataIndex: item,
        key: item,
        ellipsis: true,
        width:widthArray[Index]
      }
      detailsTableHeader.push(detailsColumObj)
    })
    let res = []
    row.forEach((item, index) => {
      let tabledata = {}
      for (let i = 0; i < detailsHeader.length; i++) {
        tabledata[detailsHeader[i]] = item[detailsHeader[i]]
        tabledata['key'] = index
      }
      res.push(tabledata)
    });
    this.setState({
      dataDetails: res,
      columnsDetails: detailsTableHeader,
      paginationDetails: {
        total: res.length,
        defaultCurrent: 1
      },
      isDetailsVisible: true
    })
  }
  // 回调函数，切换下一页
  changePage(current,pageSize){
    let pageParams = {
      current: current,
      pagesize: pageSize,
    };
    this.setState({
      current: current,
    });
    this.getListIndexAdvisor(this.state.textareaVal,pageParams);
  }
    // 回调函数,每页显示多少条
  changePageSize(pageSize,current){
    // 将当前改变的每页条数存到state中
    this.setState({
      pageSize: pageSize
    });
    let pageParams = {
      current: current,
      pagesize: pageSize,
    };
    this.getListIndexAdvisor(this.state.textareaVal,pageParams);
  }
  changeVal (e) {
    this.setState({[e.target.name]: e.target.value})
  }
  changeSelVal (value) {
    this.setState({selValue: value})
  }
  beforeUpload = (file) => {
    const isLt10M = file.size / 1024 / 1024 < 10;
    if (!isLt10M) {
      message.warning('file must smaller than 10MB!');
      return false;
    }
    const reader = new FileReader()
    reader.readAsText(file)
    reader.onload = (result) => {
      let targetNum = '';
      targetNum = result.target.result
      let paramArr = '';
      if(this.state.flgNum === 0){
        paramArr = `${this.state.textareaVal}${targetNum}`
      } else {
        paramArr = targetNum
      }
      this.setState({textareaVal: paramArr,flgNum: this.state.flgNum + 1})
    }
    return false
  }
  handleSetting(){
    this.setState({
      maxIndexNum: db.ss.get('maxIndexNum'),
      maxIndexStorage: db.ss.get('maxIndexStorage'),
      minImprovedRate: db.ss.get('minImprovedRate'),
      isSettingVisible: true,
    },()=>{

    })
  }
  handleSettingOk(){
    if(this.state.maxIndexNum && this.state.maxIndexStorage && this.state.minImprovedRate){
      this.setState({
        isSettingVisible: false,
      },()=>{
        db.ss.set('maxIndexNum',this.state.maxIndexNum)
        db.ss.set('maxIndexStorage',this.state.maxIndexStorage)
        db.ss.set('minImprovedRate',this.state.minImprovedRate)
      })
    } else {
      message.warning('The input value is a positive integer greater than 0')
    }
  }
  handleDetailsCancel(){
    this.setState({
      isDetailsVisible: false,
    })
  }
  handleSettingReset(){
    this.setState({
      maxIndexNum: '',
      maxIndexStorage: '',
      minImprovedRate: '',
    })
  }
  handleSettingCancel(){
    this.setState({
      isSettingVisible: false,
    })
  }
  handleChangeRate = (e) => {
    if(e){
      this.setState({minImprovedRate: e})
    } else {
      message.warning('The input value is a positive integer greater than 0')
    }
  }
  handleChangeNum = (e) => {
    if(e){
      this.setState({maxIndexNum: e})
    } else {
      message.warning('The input value is a positive integer greater than 0')
    }
  }
  handleChangeStorage = (e) => {
    if(e){
      this.setState({maxIndexStorage: e})
    } else {
      message.warning('The input value is a positive integer greater than 0')
    }
  }
  componentDidMount () {
    this.getItemList()
    this.getDefaultValue()
  }
  render () {
    const paginationProps = {
      showSizeChanger: true,
      showQuickJumper: true,
      showTotal: () => `Total ${this.state.total} items`,
      pageSize: this.state.pageSize,
      current: this.state.current,
      total: this.state.total,
      onShowSizeChange: (current,pageSize) => this.changePageSize(pageSize,current),
      onChange: (current,pageSize) => this.changePage(current,pageSize)
    };
    return (
      <div className='indexadvisor bordmargin'>
        <Card className="mb-20" extra={<SettingFilled className="more_link" onClick={() => { this.handleSetting() }} />} title="Smart Index Recommendation" bordered={false} style={{ width: '100%', height: 430 }}>
          <div className="flexbox">

            <div className="flextitle1">Database List：</div>
            <Select value={this.state.selValue} onChange={(val) => { this.changeSelVal(val) }} showSearch
              optionFilterProp="children" filterOption={(input, option) =>
                option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 200 }} className="mb-20">
              {
                this.state.options.map(item => {
                  return (
                    <Option value={item} key={item}>{item}</Option>
                  )
                })
              }
            </Select>
          </div>
          <div className="flexbox uploadcss">
          <div className="flextitle1">Upload File：</div>
            <Upload
              name="file"
              maxCount={1}
              beforeUpload={(file) => this.beforeUpload(file)}
            >
              <Button className="mb-20" style={{backgroundColor:'#f6f6f6',borderColor:'#eeeeee'}} size="small" icon={<UploadOutlined style={{color:'#1892ff'}}/>}>Upload</Button>
            </Upload>
          </div>
          <div className="flexbox">
            <div className="flextitle2">SQL Statements：</div>
            <TextArea rows={8} name="textareaVal" value={this.state.textareaVal} onChange={(ev) => { this.changeVal(ev) }} placeholder={`# Please enter the SQL statement as the following, then the index advisor will return suggestions.
# SELECT * FROM t1 WHERE t1.id > 100`} />
          </div>
          <div className="flexbox">
          <Button type="primary" size="small" style={{ margin: '20px 0px 20px 120px' }} onClick={() => this.addTableData('type1')}>Adivse Index</Button>
          </div>
        </Card>
        <Card title="Recommended Set" className='recommended bordmargin' bordered={false} style={{ width: '100%', height: 'auto' }}>
          <Card title="Advised Indexes" className='backcolor'>
            <Table columns={this.state.columns} dataSource={this.state.data} rowKey={record => record.key}  pagination={paginationProps} loading={this.state.loadingAdvisor} />
          </Card>
          <Card title="Redundant Indexes" className='backcolor'>
            <Table columns={this.state.columnsRedundant} dataSource={this.state.dataRedundant} rowKey={record => record.key} pagination={this.state.paginationRedundant} loading={this.state.loadingAdvisor} />
          </Card>
          <Card title="Useless Indexes" className='backcolor'>
            <Table columns={this.state.columnsUseless} dataSource={this.state.dataUseless} rowKey={record => record.key} pagination={this.state.paginationUseless} loading={this.state.loadingAdvisor} />
          </Card>
        </Card>
        <Modal title="Details" width="40vw"  footer={null}
         destroyOnClose='true' visible={this.state.isDetailsVisible} maskClosable = {false} onCancel={() => this.handleDetailsCancel()}>
           <Table columns={this.state.columnsDetails} dataSource={this.state.dataDetails} rowKey={record => record.key} pagination={this.state.paginationDetails} />
        </Modal>
        <Modal title="Setting"  width="40vw" footer={<div style={{textAlign:'center'}}><Button key="submit" type="primary" onClick={() => this.handleSettingOk()}>Save</Button><Button key="back" onClick={() => this.handleSettingReset()}>Reset</Button></div>}
         destroyOnClose='true' visible={this.state.isSettingVisible} maskClosable = {false} onOk={() => this.handleSettingOk()}  onCancel={() => this.handleSettingCancel()}>
          <p><label style={labelStyle}>Min_improved_rate: </label><InputNumber style={inputStyle} min={0} onChange={(e) => this.handleChangeRate(e)} stringMode value={this.state.minImprovedRate} /><label style={{color:'#ADA6ED'}}><InfoCircleFilled /> Minimum improved rate of the cost for the indexes</label></p>
          <p><label style={labelStyle}>Max_index_num: </label><InputNumber style={inputStyle} min={1} onChange={(e) => this.handleChangeNum(e)}  value={this.state.maxIndexNum} /><label style={{color:'#ADA6ED'}}><InfoCircleFilled /> Maximum number of advised indexes</label></p>
          <p><label style={labelStyle}>Max_index_storage: </label><InputNumber style={inputStyle} min={1} onChange={(e) => this.handleChangeStorage(e)}  value={this.state.maxIndexStorage} /><label style={{color:'#ADA6ED'}}><InfoCircleFilled /> Maximum index storage (Mb)</label></p>
        </Modal>
      </div>
    )
  }
}
