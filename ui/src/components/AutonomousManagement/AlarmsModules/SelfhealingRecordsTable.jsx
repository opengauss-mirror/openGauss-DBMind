import React, { Component } from 'react';
import { Card, Row, Col, message, Switch, Spin, Button, Modal, Form, Input , InputNumber, Space, Select, Popconfirm } from 'antd';
import { CloseOutlined, ReloadOutlined } from '@ant-design/icons';
import '../../../assets/css/main/alarm.css';
import Details from '../../../assets/imgs/Details.png';
import Setup from '../../../assets/imgs/Setup.png';
import { getSelfhealingface, getSelfhealingSetting, getSelfhealingSubmit, getSelfhealingDelete, getSelfhealingPause, getSelfhealingResumption } from '../../../api/autonomousManagement';

const { Option } = Select;
const formItemLayout = {
  labelCol:{ span: 7,offset: 4},
  wrapperCol:{ span: 12 }
}
export default class SelfhealingRecordsTable extends Component {
  constructor() {
    super()
    this.state = {
      showFlag: 0,
      allDataRegular:[],
      allData:[],
      isModalVisible: false,
      detectorVal: [],
      detectorIndex: 0,
      alarmtypeValue: '',
      alarmlevelValue: '',
      detectorName:'',
      metricName:'',
      host:'',
      detectorNameValue:'',
      sideOption:[],
      sideValue:'',
      aggOption:[],
      aggValue:'',
      maxCoefValue: '',
      alphaValue:'',
      windowValue:'',
      highValue:'',
      lowValue:'',
      periodValue:'',
      thresholdValue:'',
      freqValue:'',
      upperOutlier:'',
      lowerOutlier:'',
      modelTitle:'Create',
      alarmtypeOptions:[],
      alarmlevelOptions:[],
      detectors:[],
      detectorParams:{},
      formData:{},
      isDisabled:false
    }
  }
  async getSelfhealingData () {
    const { success, data, msg } = await getSelfhealingface('all')
    if (success) {
      this.setState({
        showFlag: 0,
        allDataRegular:Object.keys(data),
        allData:data
      })
    } else {
      message.error(msg)
    }
  }
  async getSelfhealingSetting (item) {
    const { success, data, msg } = await getSelfhealingSetting(item)
    if (success) {
      let newObj ={...data}
      delete newObj.AlarmInfo; 
      if(!item){
        this.FormRef.setFieldValue('alarm_type',data.AlarmInfo.alarm_type[0])
        this.FormRef.setFieldValue('alarm_level',data.AlarmInfo.alarm_level[0])
      }
      this.setState(()=>({
        alarmtypeOptions:data.AlarmInfo.alarm_type[1],
        alarmlevelOptions:data.AlarmInfo.alarm_level[1],
        detectors:Object.keys(data).slice(1),
        detectorParams:newObj
      }),()=>{
        this.getChildrenDefault(this.state.detectors[0])
      })
      this.getSelfhealingDetails(item)
    } else {
      message.error(msg)
    }
  }
  // 处理下拉框默认值
   getChildrenDefault(item){
        let detectorChildren=this.state.detectorParams[item];
        Object.keys(detectorChildren).forEach(key=>{
      if(key==='side'){
        this.setState(()=>({
          sideValue:detectorChildren[key][0],
          sideOption: detectorChildren[key][1]
        })
       )
      }else if(key==='agg'){
        this.setState(()=>({
          aggOption: detectorChildren[key][1],
          aggValue:detectorChildren[key][0]
          }))
      }else if(key==='max_coef'){
        this.setState(()=>({
        maxCoefValue:detectorChildren[key]
        }))
      }else if(key==='alpha'){
        this.setState(()=>({
          alphaValue:detectorChildren[key]
          }))
      }
      else if(key==='window'){
        this.setState(()=>({
          windowValue:detectorChildren[key]
          }))
      }
      else if(key==='high'){
        this.setState(()=>({
          highValue:detectorChildren[key]
          }))
      }
      else if(key==='low'){
        this.setState(()=>({
         lowValue:detectorChildren[key]
          }))
      }
      else if(key==='period'){
        this.setState(()=>({
          periodValue:detectorChildren[key]
          }))
      }
      else if(key==='high_ac_threshold'){
        this.setState(()=>({
          thresholdValue:detectorChildren[key]
          }))
      }
      else if(key==='min_seasonal_freq'){
        this.setState(()=>({
          freqValue:detectorChildren[key]
          }))
      }
    
      else if(key==='outliers'){
        this.setState(()=>({
          upperOutlier:detectorChildren[key][0],
          lowerOutlier:detectorChildren[key][1],
          }))
      }
        })
  }
  changeAlarmtypeVal (value) {
    this.setState({alarmtypeValue: value})
  }
  changeAlarmlevelVal (value) {
    this.setState({alarmlevelValue: value})
  }
  changeChecked (value,name) {
    if(value){
      this.usableOrNot(getSelfhealingResumption(name))
    } else {
      this.usableOrNot(getSelfhealingPause(name))
    }
  }
  async usableOrNot (url){
    const { success, data, msg } = await url
    if (success) {
      this.getSelfhealingData()
    } else {
      message.error(msg)
    }
  }
  changeDetectorVal (value,index) {
      this.setState(()=>({detectorNameValue: value}),()=>{
        this.getChildrenDefault(value)
      })
      let detectorChildren=this.state.detectorParams[value]
      let data = this.FormRef.getFieldValue("detectors")
      Object.keys(detectorChildren).forEach(key=>{
        if(key === 'side' || key === 'agg'){
          data[index][key] = detectorChildren[key][0]
        } else if(key === 'outliers') {
          data[index]['upperOutlier'] = detectorChildren[key][0]
          data[index]['lowerOutlier'] = detectorChildren[key][1]
        } else {
          data[index][key] = detectorChildren[key]
        }
      })
      this.FormRef.setFieldValue('detectors',data)
  }
  validateFieldValue(){
    this.FormRef.validateFields()
    .then((values)=>{
      let fieldsValue = this.FormRef.getFieldsValue()
      fieldsValue.detectors.forEach((item,index)=>{
        item['metric_filter'] = {}
        item['detector_kwargs'] = {}
        item['metric_filter']['from_instance'] = item['from_instance']
        item['detector_kwargs']['outliers'] = [item['upperOutlier'],item['lowerOutlier']]
        delete item['upperOutlier']
        delete item['lowerOutlier']
        delete item['from_instance']
        for (let key in item) {
          if(key !== 'metric_filter' && key !== 'metric_name' && key !== 'detector_kwargs' && key !== 'detector_name' ){
            item['detector_kwargs'][key] = item[key]
            delete item[key]
          }
        }
        if(item.detector_name === 'GradientDetector'){
          Object.keys(item['detector_kwargs']).forEach(key=>{
            if( key !== 'side' && key !== 'max_coef' && key !== 'percentage'){
              delete item['detector_kwargs'][key]
            }
          })
        } else if(item.detector_name === 'IncreaseDetector'){
          Object.keys(item['detector_kwargs']).forEach(key=>{
            if( key !== 'side' && key !== 'alpha'){
              delete item['detector_kwargs'][key]
            }
          })
        } else if(item.detector_name === 'InterQuartileRangeDetector'){
          Object.keys(item['detector_kwargs']).forEach(key=>{
            if( key !== 'outliers' ){
              delete item['detector_kwargs'][key]
            }
          })
        } else if(item.detector_name === 'LevelShiftDetector'){
          Object.keys(item['detector_kwargs']).forEach(key=>{
            if(key !== 'side' && key !== 'outliers' && key !== 'window'&& key !== 'agg'){
              delete item['detector_kwargs'][key]
            }
          })
        } else if(item.detector_name === 'SeasonalDetector'){
          Object.keys(item['detector_kwargs']).forEach(key=>{
            if( key !== 'side' && key !== 'outliers' && key !== 'window'&& key !== 'period'&& key !== 'high_ac_threshold'&& key !== 'min_seasonal_freq'){
              delete item['detector_kwargs'][key]
            }
          })
        } else if(item.detector_name === 'SpikeDetector'){
          Object.keys(item['detector_kwargs']).forEach(key=>{
            if( key !== 'side' && key !== 'outliers' && key !== 'window'&& key !== 'agg'){
              delete item['detector_kwargs'][key]
            }
          })
        } else if(item.detector_name === 'ThresholdDetector'){
          Object.keys(item['detector_kwargs']).forEach(key=>{
            if( key !== 'high' && key !== 'low' && key !== 'percentage'){
              delete item['detector_kwargs'][key]
            }
          })
        } else if(item.detector_name === 'VolatilityShiftDetector'){
          Object.keys(item['detector_kwargs']).forEach(key=>{
            if( key !== 'side' && key !== 'outliers' && key !== 'window' && key !== 'agg'){
              delete item['detector_kwargs'][key]
            }
          })
        } else if(item.detector_name === 'QuantileDetector'){
          Object.keys(item['detector_kwargs']).forEach(key=>{
            if( key !== 'high' && key !== 'low'){
              delete item['detector_kwargs'][key]
            }
          })
        } else if(item.detector_name === 'EsdTestDetector'){
          Object.keys(item['detector_kwargs']).forEach(key=>{
            if( key !== 'alpha'){
              delete item['detector_kwargs'][key]
            }
          })}
      })
      let param = {
        name : fieldsValue.name,
        detectors_info:{
          running:1,
          duration:Number(fieldsValue.duration),
          forecasting_seconds:0,
          alarm_info:{
            alarm_content: fieldsValue.alarm_content,
            alarm_type: fieldsValue.alarm_type,
            alarm_level: fieldsValue.alarm_level,
            alarm_cause: fieldsValue.alarm_cause,
            extra:fieldsValue.extra,
          },
          detector_info:fieldsValue.detectors
        }
      }
      this.handleOk(param)
    })
    .catch((errInfo)=>{return false})
  }
  async handleOk(param){
    const { success, data, msg } = await getSelfhealingSubmit(param)
    if (success) {
      this.getSelfhealingData()
      message.success('Adding succeeded')
    } else {
      message.error(msg)
    }
    this.setState({
      isModalVisible: false
    })
  }
  create(item,index) {
    let data = ['Create','Update','Details'],title = ''
    if(item && index === 1){
      title = data[1]
    }  else if(item && index === 2){
      title = data[2]
    } else {
      title = data[0]
    }
    this.setState({
      isModalVisible: true,
      modelTitle:title,
      isDisabled:index === 2 ? true : false
    },()=>{
      this.getSelfhealingSetting(item)
    })
  }
 async getSelfhealingDetails (settingName) {
  this.state.allDataRegular.forEach(key=>{
      if(key === settingName){
        let newObj = this.state.allData[key]
        Object.keys(newObj.alarm_info).forEach(key=>{
          this.FormRef.setFieldValue(key,newObj.alarm_info[key])
        })
        this.FormRef.setFieldValue('duration',newObj['duration'])
        this.FormRef.setFieldValue('name',key)
        let detectorArray = [],detectorObj = {}, formData = {}
        newObj.detector_info.forEach(item=>{
          detectorObj = Object.assign({}, item.metric_filter, item.detector_kwargs,{metric_name:item['metric_name']},{detector_name:item['detector_name']});
          detectorArray.push(detectorObj)
        })
        this.FormRef.setFieldValue('detectors',detectorArray)
      }
    })
  }
  async deleteDetector(item) {
    const { success, data, msg } = await getSelfhealingDelete(item)
    if (success) {
      this.getSelfhealingData()
      message.success('Deleted successfully')
    } else {
      message.error(msg)
    }
  }
  cancel(){}
  handleCancel(){
    this.setState({
      isModalVisible: false,
    })
  }
  componentDidMount () {
    this.getSelfhealingData()
  }
  render () {
    return (
      <div className='selfhealing'>
        <Card title="Anomaly detections"  className="mb-10">
          <Row gutter={[10,10]} className='childstyle'>
          {this.state.showFlag === 0 ? this.state.allDataRegular.map((item) => {
                return (
                  <Col className="gutter-row antclopercent_20" >
                  <Card title={<Switch defaultChecked={this.state.allData[item]['running']}  onChange={(val) => { this.changeChecked(val,item) }} />} style={{ height: 90}} extra={<Popconfirm
                    title="Delete the Card" description="Are you sure to delete this Card?" onConfirm={() => {this.deleteDetector(item)}}
                    onCancel={this.cancel()} okText="Yes" cancelText="No" > <CloseOutlined style={{fontSize:10,color:'#CCCCCC',cursor:'pointer'}} />
                  </Popconfirm>}>
                    <div className='lockstyle'><span className='spanleft'>{item}</span><span className='spanright'><img src={Details} title='Details' alt="" disabled style={{marginRight:12}} onClick={() => {}} ></img><img src={Setup} title='Setup' alt="" disabled onClick={() => {}} ></img></span></div>
                  </Card>
                </Col>
                )
              })
            : <div style={{ textAlign: 'center' }}><Spin style={{ margin: '100px auto' }} /> </div>}
            <Col className="gutter-row antclopercent_20" >
              <Card title={<Switch defaultChecked={false} disabled={true}  onChange={() => {}} />} style={{ height: 90}}>
                <p style={{textAlign:'center'}}><Button style={{borderRadius:'11px'}} type="primary" size='small' ghost onClick={() => {this.create('',0)}}>Additions</Button></p>
              </Card>
            </Col>
          </Row>
        </Card>
        {!this.state.isModalVisible ? null :<Modal title={this.state.modelTitle}  width="40vw" destroyOnClose={true} bodyStyle={{overflowY: "auto",overflowX:"none",height: "60vh"}} visible={this.state.isModalVisible} maskClosable = {false} centered='true' 
        onOk={() => this.validateFieldValue()} onCancel={() => this.handleCancel()} okButtonProps={{disabled: this.state.isDisabled}} className='formlistclass'>
              <Form
                layout='horizontal'
                disabled={this.state.isDisabled}
                style={{ maxWidth: 520 }}
                ref={(e) => {
                  this.FormRef = e
                }}
                autoComplete="off"
              >
                <Form.Item {...formItemLayout} name="name" label="name" rules={[{required: true, message: 'Missing name'}]}>
                  <Input placeholder="" allowClear={true} />
                </Form.Item>
                <Form.Item {...formItemLayout} name="duration" label="duration(second)" rules={[{required: true, message: 'Missing duration'}]}>
                  <InputNumber min={1} style={{ width: 260 }} />
                </Form.Item>
                <Form.Item {...formItemLayout} name="alarm_type" label="alarm type"  rules={[{required: true, message: 'Missing alarm type'}]}>
                  <Select value={this.state.alarmtypeValue} onChange={(val) => { this.changeAlarmtypeVal(val) }} showSearch
                      optionFilterProp="children" filterOption={(input, option) =>
                        option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 260 }}>
                      {
                        this.state.alarmtypeOptions.map(item => {
                          return (
                            <Option value={item} key={item}>{item}</Option>
                          )
                        })
                      }
                  </Select>
                </Form.Item>
                <Form.Item {...formItemLayout} name="alarm_level" label="alarm level" rules={[{required: true, message: 'Missing alarm level'}]}>
                  <Select value={this.state.alarmlevelValue} onChange={(val) => { this.changeAlarmlevelVal(val) }} showSearch
                      optionFilterProp="children" filterOption={(input, option) =>
                        option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0} style={{ width: 260 }}>
                      {
                        this.state.alarmlevelOptions.map(item => {
                          return (
                            <Option value={item} key={item}>{item}</Option>
                          )
                        })
                      }
                  </Select>
                </Form.Item>
                <Form.Item {...formItemLayout} name="alarm_content" label="alarm content">
                  <Input placeholder="" allowClear={true} />
                </Form.Item>
                <Form.Item {...formItemLayout} name="alarm_cause" label="alarm cause">
                  <Input placeholder="" allowClear={true} />
                </Form.Item>
                <Form.Item {...formItemLayout} name="extra" label="extra">
                  <Input placeholder="" allowClear={true} />
                </Form.Item>
                <Form.Item shouldUpdate >
                  {() => (
                      <Form.List name="detectors" >
                      {(fields, { add, remove }) => (
                        <>
                          {fields.map((field,index) => (
                            <Space key={field.key} align="baseline" className="selfthealing">
                              <CloseOutlined onClick={() => remove(field.name)} style={{display:this.state.isDisabled ? 'none' :'block',textAlign:'end',marginBottom:24}} />
                                  <Form.Item
                                    {...field}
                                    {...formItemLayout}
                                    style={{ width: 520 }}
                                    label="detectorName"
                                    name={[field.name, 'detector_name']}
                                    key={[field.key, 'detector_name']}
                                    initialValue={this.state.detectors[0]}
                                    rules={[
                                      {
                                        required: true,
                                        message: 'Missing detectorName',
                                      },
                                    ]}
                                  >
                                    <Select value={this.state.detectorNameValue} style={{ width: 260 }} onChange={(val) => { this.changeDetectorVal(val,index) }} showSearch
                                              optionFilterProp="children" filterOption={(input, option) =>
                                                option.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}>
                                      {this.state.detectors.map((item) => (
                                        <Option key={item} value={item}>
                                          {item}
                                        </Option>
                                      ))}
                                    </Select>
                                  </Form.Item>
                              <Form.Item
                                {...field}
                                {...formItemLayout}
                                style={{ width: 520 }}
                                label="metricName"
                                name={[field.name, 'metric_name']}
                                key={[field.key, 'metric_name']}
                                rules={[
                                  {
                                    required: true,
                                    message: 'Missing metricName',
                                  },
                                ]}
                              >
                                <Input />
                              </Form.Item>
                              <Form.Item
                                {...field}
                                {...formItemLayout}
                                style={{ width: 520 }}
                                label="Host"
                                name={[field.name, 'from_instance']}
                                key={[field.key, "from_instance"]}
                                rules={[
                                  {
                                    required: true,
                                    message: 'Missing Host',
                                  },
                                ]}
                              >
                                <Input />
                              </Form.Item>
                                  {
                                        this.FormRef !== undefined && this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) && 
                                        (
                                          <>
                                          {this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'GradientDetector' ||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'IncreaseDetector' ||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'LevelShiftDetector' ||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'SeasonalDetector' ||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'SpikeDetector' ||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'VolatilityShiftDetector' ?<Form.Item
                                            {...field}
                                            label='side'
                                            {...formItemLayout}
                                            name={[field.name, 'side']}
                                            key={[field.key, 'side']}
                                            rules={[
                                              {
                                                required: true,
                                                message: 'Missing side',
                                              },
                                            ]}
                                          >
                                          <Select style={{ width: 260 }}  key={this.state.sideValue}>
                                      { this.state.sideOption.map((item) => (
                                        <Option key={item} value={item}>
                                          {item}
                                        </Option>
                                      ))}
                                    </Select>
                                          </Form.Item>:null}
                                          {this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'GradientDetector' ?<Form.Item
                                            {...field}
                                            {...formItemLayout}
                                            label='max_coef'
                                            name={[field.name, 'max_coef']}
                                            key={[field.key, 'max_coef']}
                                            rules={[
                                              {
                                                required: true,
                                                message: 'Missing max_coef',
                                              },
                                            ]}
                                          >
                                            <Input  key={this.state.maxCoefValue} style={{width:260}}/>
                                          </Form.Item>:null}
                                          {this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'EsdTestDetector' ||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'IncreaseDetector' ? <Form.Item
                                            {...field}
                                            {...formItemLayout}
                                            label='alpha'
                                            name={[field.name, 'alpha']}
                                            key={[field.key, 'alpha']}
                                            rules={[
                                              {
                                                required: true,
                                                message: 'Missing alpha',
                                              },
                                            ]}
                                            initialValue={this.state.alphaValue}
                                          >
                                            <Input key={this.state.alphaValue} style={{width:260}} />
                                          </Form.Item>:null}
                                          {this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'InterQuartileRangeDetector' ||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'LevelShiftDetector'|| 
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) ==='SeasonalDetector'|| 
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) ==='SpikeDetector'?<Form.Item
                                            {...field}
                                            {...formItemLayout}
                                            label='upperOutlier'
                                            name={[field.name, 'upperOutlier']}
                                            key={[field.key, 'upperOutlier']}
                                            rules={[
                                              {
                                                required: true,
                                                message: 'Missing upperOutlier',
                                              },
                                            ]}
                                          >
                                            <InputNumber style={{width:260}}  key={this.state.upperOutlier}/>
                                          </Form.Item>:null}
                                          {this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'InterQuartileRangeDetector' ||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'LevelShiftDetector'|| 
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) ==='SeasonalDetector'|| 
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) ==='SpikeDetector'?<Form.Item
                                            {...field}
                                            {...formItemLayout}
                                            label='lowerOutlier'
                                            name={[field.name, 'lowerOutlier']}
                                            key={[field.key, 'lowerOutlier']}
                                            rules={[
                                              {
                                                required: true,
                                                message: 'Missing lowerOutlier',
                                              },
                                            ]}
                                          >
                                            <InputNumber style={{width:260}}  key={this.state.lowerOutlier}/>
                                          </Form.Item>:null}
                                          {this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'LevelShiftDetector'||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'SeasonalDetector'||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'SpikeDetector'||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'VolatilityShiftDetector' ? <Form.Item
                                            {...field}
                                            {...formItemLayout}
                                            label='window'
                                            name={[field.name, 'window']}
                                            key={[field.key, 'window']}
                                            rules={[
                                              {
                                                required: true,
                                                message: 'Missing window',
                                              },
                                            ]}
                                          >
                                            <Input  key={this.state.windowValue} style={{width:260}}/>
                                          </Form.Item>:null}
                                          {this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'QuantileDetector'||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'ThresholdDetector' ? <Form.Item
                                            {...field}
                                            {...formItemLayout}
                                            label='high'
                                            name={[field.name, 'high']}
                                            key={[field.key, 'high']}
                                            rules={[
                                              {
                                                required: true,
                                                message: 'Missing high',
                                              },
                                            ]}
                                          >
                                            <Input  key={this.state.highValue} style={{width:260}}/>
                                          </Form.Item>:null}
                                          {this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'QuantileDetector'||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'ThresholdDetector' ? <Form.Item
                                            {...field}
                                            {...formItemLayout}
                                            label='low'
                                            name={[field.name, 'low']}
                                            key={[field.key, 'low']}
                                            rules={[
                                              {
                                                required: true,
                                                message: 'Missing low',
                                              },
                                            ]}
                                          >
                                            <Input key={this.state.lowValue} style={{width:260}} />
                                          </Form.Item>:null}
                                          {this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'SeasonalDetector' ? <Form.Item
                                            {...field}
                                            {...formItemLayout}
                                            label='high_ac_threshold'
                                            name={[field.name, 'high_ac_threshold']}
                                            key={[field.key, 'high_ac_threshold']}
                                            rules={[
                                              {
                                                required: true,
                                                message: 'Missing high_ac_threshold',
                                              },
                                            ]}
                                          >
                                            <Input key={this.state.thresholdValue} style={{width:260}}/>
                                          </Form.Item>:null}
                                          {this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'SeasonalDetector' ? <Form.Item
                                            {...field}
                                            {...formItemLayout}
                                            label='min_seasonal_freq'
                                            name={[field.name, 'min_seasonal_freq']}
                                            key={[field.key, 'min_seasonal_freq']}
                                            rules={[
                                              {
                                                required: true,
                                                message: 'Missing min_seasonal_freq',
                                              },
                                            ]}
                                          >
                                            <Input  key={this.state.freqValue} style={{width:260}}/>
                                          </Form.Item> : null}
                                          {this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'SeasonalDetector' ? <Form.Item
                                            {...field}
                                            {...formItemLayout}
                                            label='period'
                                            name={[field.name, 'period']}
                                            key={[field.key, 'period']}
                                            rules={[
                                              {
                                                required: true,
                                                message: 'Missing period',
                                              },
                                            ]}
                                          >
                                            <Input key={this.state.periodValue} style={{width:260}}/>
                                          </Form.Item>:null}
                                          {this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'LevelShiftDetector'||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'SpikeDetector'||
                                            this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'VolatilityShiftDetector' ? <Form.Item
                                            {...field}
                                            {...formItemLayout}
                                            label='agg'
                                            name={[field.name, 'agg']}
                                            key={[field.key, 'agg']}
                                            rules={[
                                              {
                                                required: true,
                                                message: 'Missing agg',
                                              },
                                            ]}
                                          >
                                            <Select style={{ width: 260 }} key={this.state.aggValue}>
                                              { this.state.aggOption.map((item) => (
                                                <Option key={item} value={item}>
                                                  {item}
                                                </Option>
                                              ))}
                                            </Select>
                                          </Form.Item>:null}
                                          {this.FormRef.getFieldValue(['detectors', field.name, 'detector_name' ]) === 'ThresholdDetector' ? <Form.Item
                                            {...field}
                                            {...formItemLayout}
                                            label='percentage'
                                            name={[field.name, 'percentage']}
                                            key={[field.key, 'percentage']}
                                            rules={[
                                              {
                                                required: true,
                                                message: 'Missing percentage',
                                              },
                                            ]}
                                          >
                                           <InputNumber min={0} max={1} step={0.1} style={{width:260}}/>
                                          </Form.Item>:null}
                                          </>
                                        )
                                  }
                            </Space>
                          ))}
              
                          <Form.Item style={{textAlign:'end'}}>
                            <Button style={{borderRadius:'11px'}} type="primary" size='small' ghost onClick={() => {add()}}>Additions</Button>
                          </Form.Item>
                        </>
                      )}
                    </Form.List>
                  )}</Form.Item>
              </Form>
        </Modal>}
      </div>
    )
  }
}
