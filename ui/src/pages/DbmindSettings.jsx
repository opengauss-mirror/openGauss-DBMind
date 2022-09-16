import React from 'react';
import { Table, Input, Button, Card, Space, Modal, message } from 'antd';
import '../../src/assets/css/main/dbmindSettings.css';
import { getSettingListInterface, putSettingDetailInterface } from '../api/dbmindSettings';
let saveArr = []
let timer = null
let ifChanged = 0
export default class EditableTable extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      editArr: [],
      columns: [{
        title: 'name',
        dataIndex: 'name',
        fixed: true,
      }, {
        title: 'value',
        dataIndex: 'value',
        editable: true,
        render: (data, list, index) => {
          return <Input style={{width:200}} defaultValue={data} key={String(list.key) + String(Date.now())} onChange={(e) => this.inputChange(e, list, index, 'value')} onBlur={(e) => this.onBlurChange(e, list, index, 'value')} />
        },
        shouldCellUpdate: () => {
          return true
        },
      }],
      allDataSource: [],
      isModalVisible: false,
      editTableData: [],
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
            value: it[1]
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
        this.setState({ isModalVisible: false })
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
      editTableData: result
    })
  }
  handleSave () {
    if (this.state.editTableData.length > 0) {
      let flag = 0
      this.state.editTableData.forEach((item) => {
        if (item.value === '') {
          flag = -1
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
      if (flag === -1) {
        message.warning('The data cannot be empty.')
        this.getSettingList()
        this.setState({
          isModalVisible: false
        })
        ifChanged = 0
      } else {
        this.getSettingList()
      }
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
  render () {
    return (
      <div className="contentWrap">
        <Card title="Dynamic Settings" extra={<Space>{this.handleRender()}<Button type="primary" onClick={() => this.showModal()}>Save</Button><Button type="primary" onClick={() => this.handleDiscard()}>Discard</Button></Space>} style={{ minHeight: 790 }}>
          <div className="dbmindTable" style={{display:'flex'}}>
          {
            this.state.allDataSource.map((item) => {
              return (
                <div key={item.tableName} style={{display:'block',marginBottom:20,width:'50%'}}>
                  <h3> {item.tableName.replace(/_/g, ' ')}</h3>
                  <Table bordered dataSource={item.tableSource} columns={this.state.columns} rowKey={record => record.key} rowClassName="tablecellclass"  pagination={{defaultPageSize:20}}/>
                </div>
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
