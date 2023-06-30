import React, { Component } from "react";
import { Button, Modal, Card, Descriptions } from "antd";
import ReactEcharts from "echarts-for-react";

let datas = [],
  link = []
export default class SqlPlan extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showDetails: false,
      isShowNodeInfo: false,
      nodeData: {},
      dataFlg:false,
      autoHeight:500
    };
  }
  addId(data){
    data.forEach((item, index) => {
      item["id"] = Math.random().toString()
      if (item.children) {
        item.children.forEach((oitem, oindex) => {
          item["id"] = Math.random().toString()
        })
        this.addId(item.children);
      }
    })
  }
  dealDataItem(data) {
    data.forEach((item, index) => {
      let dataItem = {
        name: item.name,
        key: item.level+1,
        id: item.id,
      };
      Object.keys(item.detail).forEach(elem=>{
        if(elem !== "properties"){
          dataItem[elem] = item.detail[elem]
        }
      })
      Object.keys(item.detail.properties).forEach(elem=>{
        dataItem[elem] = item.detail.properties[elem]
      })
      datas.push(dataItem);
      if (item.children) {
        item.children.forEach((oitem, oindex) => {
          let linkItem = {
            source: item.children[oindex].id,
            target: item.id,
          };
          link.push(linkItem);
        })
        this.dealDataItem(item.children);
      }
    });
  }
  compare(property){
    return function(a,b){
        var value1 = a[property];
        var value2 = b[property];
        return value1 - value2;
    }
  }
  dealData() {
      let firstX = 300,firstY = 100;
        if (this.props.planData && this.props.planData.length) {
          this.addId(this.props.planData);
          this.dealDataItem(this.props.planData);
          datas.sort(this.compare('key'))
          let array = []
          for (let i = 0; i < datas[datas.length-1].key; i++) {
            array.push([])
          }
          datas.forEach((item, index) => {
            array[item.key-1].push(item)
          })
          array.forEach((item, index) => {
            if (index < 1) {
              item[0].x = firstX;
              item[0].y = firstY;
            }
            // 商数是偶数
            if (item.length % 2 === 0) {
              let centerIndex = parseInt(item.length / 2)
              item[centerIndex].x = firstX + 50
              item[centerIndex].y = firstY + index*100
              item.forEach((oitem, oindex) => {
                if(Math.abs(item[centerIndex].x + (oindex-centerIndex)*100-300)*(oindex-centerIndex)/2 < oitem.name.length*3){
                  oitem.x = item[centerIndex].x + (oindex-centerIndex)*(100 + oitem.name.length)
                } else {
                  oitem.x = item[centerIndex].x + (oindex-centerIndex)*100
                }
                oitem.y = item[centerIndex].y
              })
            } else {
              let centerIndex = parseInt(item.length-1 / 2)
              item[centerIndex].x = firstX
              item[centerIndex].y = firstY + index*100
              item.forEach((oitem, oindex) => {
                if(Math.abs(item[centerIndex].x + (oindex-centerIndex)*100-300)*(oindex-centerIndex)/2 < oitem.name.length*3){
                  oitem.x = item[centerIndex].x + (oindex-centerIndex)*(100 + oitem.name.length)
                } else {
                  oitem.x = item[centerIndex].x + (oindex-centerIndex)*100
                }
                oitem.y = item[centerIndex].y
              })
            }
          })
          this.setState({
            dataFlg: true,
            autoHeight:datas.length < 5 ? 500 : (datas[datas.length-1].key+1)*100
          });
        } else {
          datas = [];
          link = [];
          this.setState({
            dataFlg: false,
          });
        }
  }

  getOption() {
    return {
      tooltip: {
        trigger:'item',
        formatter: function (params) {
          let res=""
         Object.keys(params.data).forEach(elem=>{
           if(elem!=='x' && elem!=='y' && elem!=='id' && elem!=='key'){
            let data = params.data[elem] && typeof params.data[elem] !== 'number' && isNaN(params.data[elem]) ? params.data[elem].replace(/(.{30})/g, '$1<br>') : params.data[elem]
           res+=  `
          <span style="display:inline-block;width:120px;margin:2px 0;margin-right:10px;font-weight:600;text-align:right;"> ${elem}</span>: ${data}<br/>`
           }
         })
        return res
        },
      },
      animationDurationUpdate: 1500,
      animationEasingUpdate: "quinticInOut",
      series: [
        {
          type: "graph",
          layout: "none",
          symbolSize: function(value,params){
            return [params.name.length*7+4,40]
          },
          roam: true,
          label: {
            show: true,
          },
          symbol: "roundRect",
          edgeSymbol: ["circle", "arrow"],
          edgeSymbolSize: [4, 10],
          edgeLabel: {
            fontSize: 20,
          },
          data: datas,
          links: link,
          lineStyle: {
            opacity: 0.9,
            width: 2,
            curveness: 0,
          },
        },
      ],
    };
  }
  handleCancelInfo() {
    this.setState({
      isShowNodeInfo: false,
    });
  }
  clickNode(e) {
    this.setState({
      isShowNodeInfo: true,
      nodeData: e.data,
    });
  }
  onClick = {
    click: this.clickNode.bind(this),
  };
  componentDidMount() {
    this.dealData()
  }
  componentWillUnmount(){
    datas = [];
    link = [];
  }
  render() {
    return (
      <div>
      <ReactEcharts
        ref={(e) => {
          this.echartsElement = e;
        }}
        style={{ width: 1000, height: this.state.autoHeight, margin: "0 auto" }}
        option={this.getOption()}
        onEvents={this.onClick}
        lazyUpdate={true}
      ></ReactEcharts>
    
    <Modal
      title="nodeInfo"
      destroyOnClose="true"
      bodyStyle={{ overflowY: "auto", overflowX: "none" }}
      visible={this.state.isShowNodeInfo}
      maskClosable={true}
      centered="true"
      footer={[]}
      onCancel={() => this.handleCancelInfo()}
    >
      <div></div>
      <Descriptions bordered column={1}>
          {
            Object.keys(this.state.nodeData).map((item) => {
              return item!=='x' && item!=='y' && item!=='id' && item!=='key' && (
                <Descriptions.Item label={item}>{this.state.nodeData[item]}</Descriptions.Item>
              )
            })
          }
      </Descriptions>
    </Modal>
  </div>
    );
  }
}