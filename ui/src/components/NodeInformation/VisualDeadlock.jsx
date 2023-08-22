import React, { Component } from "react";
import { Modal, Descriptions, Spin } from "antd";
import ReactEcharts from "echarts-for-react";
import { getTreeDetails } from "../../api/autonomousManagement";
let datas = [],
  link = [];
export default class VisualDeadlock extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showDetails: false,
      isShowNodeInfo: false,
      nodeName: "",
      dataFlg: false,
    };
  }
  dealDataItem(data) {
    data.forEach((item, index) => {
      let dataItem = {
        name: item.name,
      };
      Object.keys(item.details).forEach((elem) => {
        dataItem[elem] = item.details[elem];
      });

      datas.push(dataItem);
      if (item.children.length) {
        let linkItem = {
          source: typeof item.name==='number' ?item.name.toString():item.name,
          target: typeof item.children[0].name==='number'?item.children[0].name.toString():item.children[0].name,
        };
        link.push(linkItem);
        this.dealDataItem(item.children);
      }
    });
  }
  async dealData() {
    const { success, data, msg } = await getTreeDetails(
      this.props.detailsParam.sessionid
    );

    let firstX = 300,
      firstY = 100;
    if (success) {
      if (data.length) {
        this.dealDataItem(data);
        datas.forEach((ele, index) => {
          if (index < 1) {
            ele.x = firstX;
            ele.y = firstY;
          }
          let indexNum = index + 1;
          const s = parseInt(indexNum / 4);
          const l = indexNum % 4;
          // 商数是偶数
          if (s % 2 === 0) {
            let XArr = [100, 200, 300, 400];
            if (l < 1) {
              ele.x = XArr[0];
            } else {
              ele.x = XArr[l - 1];
            }
          } else {
            let XArr = [400, 300, 200, 100];
            // 商数是基数
            if (l < 1) {
              ele.x = XArr[0];
            } else {
              ele.x = XArr[l - 1];
            }
          }

          if (l === 0) {
            ele.y = firstY + 50 * (s - 1);
          } else {
            ele.y = firstY + 50 * s;
          }
        });
        this.setState({
          dataFlg: true,
        });
      } else {
        datas = [];
        link = [];
        this.setState({
          dataFlg: false,
        });
      }
    }
  }

  getOption() {
    return {
      tooltip: {
        trigger:'item',
        formatter: function (params) {
          let res=""
         Object.keys(params.data).forEach(elem=>{
           if(elem!=='x' && elem!=='y' && elem!=='name'){
           res+=  `
          <span style="display:inline-block;width:120px;margin:2px 0;margin-right:10px;font-weight:600;text-align:right;"> ${elem}</span>: ${params.data[elem]}<br/>`
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
          symbolSize: 50,
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

  componentDidMount() {
    this.dealData();
  }
  componentWillUnmount(){
    datas = [];
        link = [];
  }
  render() {
    return (
      <>
        {this.state.dataFlg ? (
          <ReactEcharts
            ref={(e) => {
              this.echartsElement = e;
            }}
            style={{ width: 1000, height: 600, margin: "0 auto" }}
            option={this.getOption()}
            lazyUpdate={true}
          ></ReactEcharts>
        ) : (
          <div style={{ textAlign: "center" }}>
            <Spin style={{ margin: "100px auto" }} />{" "}
          </div>
        )}
      </>
    );
  }
}
