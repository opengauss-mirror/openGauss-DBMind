import React from 'react';
import { Table, Input, Button, Card, Space, Modal, message, Collapse} from 'antd';
import '../../src/assets/css/main/dbmindSettings.css';
import { getSettingListInterface, putSettingDetailInterface } from '../api/dbmindSettings';
let saveArr = []
let timer = null
let ifChanged = 0
const { Panel } = Collapse;
export default class DbmindSettings extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      editArr: [],
      columns: [{
        title: 'name',
        dataIndex: 'name',
        fixed: true,
        width: 200,
      }, {
        title: 'value',
        dataIndex: 'value',
        editable: true,
        width: 200,
        render: (data, list, index) => {
          return <Input style={{width:200}} defaultValue={data} key={String(list.key) + String(Date.now())} onChange={(e) => this.inputChange(e, list, index, 'value')} onBlur={(e) => this.onBlurChange(e, list, index, 'value')} />
        },
        shouldCellUpdate: () => {
          return true
        },
      },{
        title: 'explain',
        dataIndex: 'explain',
      }],
      allDataSource: [],
      isModalVisible: false,
      editTableData: [],
      vectorKey:''
    };
  }
  inputChange = (e, record, index, field) => {
    let { editArr } = this.state;
    editArr[index] = record;
    record[field] = e.target.value;
    ifChanged = 1
    this.setState({ editArr });
  };
  onBlurChange = (e, record, index, field) => {
    let obj = {
      key: record.key,
      value: record[field],
      name: record.name
    }
    saveArr.push(obj)
  }
  async getSettingList () {
    const { success, data, msg } = await getSettingListInterface()
    if (success) {
      let tableAll = []
      Object.keys(data.dynamic).forEach(function (key, i) {
        let tableKeyData = []
        data.dynamic[key].forEach((it, index) => {
          let obj = {
            key: index + '@' + key,
            name: it[0],
            value: it[1],
            explain: it[2]
          }
          tableKeyData.push(obj)
        })
        let tableobj = {
          tableName: key,
          tableSource: [...tableKeyData],
          key: i
        }
        tableAll.push(tableobj)
      })
      this.setState({
        allDataSource: [...tableAll],
        isModalVisible: false,
        vectorKey:0,
      })
    } else {
      message.error(msg)
    }
  }
  async putSettingDetails (params) {
    let paramData = {
      ...params
    }
    const { success, msg } = await putSettingDetailInterface(paramData)
    if (success) {
      ifChanged = 2
      timer = setTimeout(() => {
        ifChanged = 0
        if(this.state.isModalVisible){
          this.setState({ isModalVisible: false })
          this.getSettingList()
        }
      }, 2000);
    } else {
      message.error(msg)
      return
    }
  }
  handleDataDeduplicate = (value) => {
    this.setState({
      editTableData: [],
    })
    let result = [];
    let obj = {};
    for (let i = value.length - 1; i >= 0; i--) {
      if (!obj[value[i].key]) {
        result.push(value[i]);
        obj[value[i].key] = true;
      }
    }
    this.setState({
      editTableData: result,
    })
  }
  handleSave () {
    if (this.state.editTableData.length > 0) {
      this.state.editTableData.forEach((item) => {
        if (item.value === '') {
          message.warning('The data cannot be empty.')
          this.getSettingList()
          this.setState({
            isModalVisible: false
          })
          ifChanged = 0
          return
        } else {
          let configFlag = item.key.split('@')
          let params = {
            config: configFlag[1],
            name: item.name,
            value: item.value,
            dynamic: true
          }
          this.putSettingDetails(params)
        }
      })
    } else {
      this.setState({
        isModalVisible: false
      })
    }
    this.setState({
      editTableData: []
    })
    saveArr = []
  }
  showModal () {
    this.setState({
      isModalVisible: true
    }, () => {
      this.handleDataDeduplicate(saveArr)
    })
  }
  handleCancel () {
    this.setState({
      isModalVisible: false
    })
  }
  handleDiscard () {
    ifChanged = 0
    saveArr = []
    this.getSettingList()
  }
  componentWillUnmount () {
    clearTimeout(timer)
  }
  componentDidMount () {
    this.getSettingList()
  }
  handleRender () {
    if (ifChanged === 0) {
      return <span ></span>
    } else if (ifChanged === 1) {
      return <span className="blink">changed</span>
    } else {
      return <span style={{ color: '#87d068' }}>saved</span>
    }
  }
  onChange = (key) => {
    this.setState({vectorKey:key})
  };
  render () {
    return (
      <div className="contentWrap settingstyle">
        <Card title="Dynamic Settings" extra={<Space>{this.handleRender()}<Button type="primary" size='small' onClick={() => this.showModal()}>Save</Button><Button size='small' type="primary" onClick={() => this.handleDiscard()}>Discard</Button></Space>} style={{ minHeight: 790 }}>
          <div className="dbmindTable">
          {
            this.state.allDataSource.map((item,index) => {
              return (
                <Collapse  activeKey={this.state.vectorKey}  onChange={(key)=>{this.onChange(key)}} expandIconPosition='end' >
                <Panel header={item.tableName.replace(/_/g, ' ')} key={index} forceRender={true} className='panelStyle'>
                  <Table size='small' bordered dataSource={item.tableSource} columns={this.state.columns} rowKey={record => record.key} rowClassName="tablecellclass"  pagination={false}/>
              </Panel>
              </Collapse>
              )
            })
          }
          </div>
        </Card>
        <Modal title="Save" visible={this.state.isModalVisible} onOk={() => this.handleSave()} onCancel={() => this.handleCancel()}>
          <p>Are you sure you want to save it?</p>
        </Modal>
      </div>
    );
  }
}
