import React, { Component } from 'react';
import { Table, Card } from 'antd';
import { UpOutlined, DownOutlined } from '@ant-design/icons';
import NodeEchartFormWork from '../../../NodeInformation/NodeModules/NodeEchartFormWork';
import ResizeableTitle from '../../../common/ResizeableTitle';
import { formatTableTitle } from '../../../../utils/function';

export default class SystemStorage extends Component {
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
      echartData: [],

    }
  }

  components = {
    header: {
      cell: ResizeableTitle,
    },
  };

  async getStorageDataAll() {
    let result = [[{
      timestamps: [
        1686190269661,

      ],
      labels: { devices: 'eth0', instance: '7.194.130.42:9100', mountpoint: './' },
      values: [
        211242930176
      ],
    },],
    [
      {
        timestamps: [
          1686190269661,
          1686190299661,
          1686190329661,
          1686190359661,
          1686190389661,
          1686190419661,
          1686190449661,
          1686190479661,
          1686190509661,
          1686190539661,
        ],
        labels: { devices: 'eth0', from_instance: '7.194.130.42' },
        values: [
          0.0082,
          0.00819,
          0.113343,
          0.003243545,
          0.0124324,
          0.123445,
          0.0124325435,
          0.024354,
          0.02343654,
          0.0089,
        ],
      },

    ]]

    if (result[0]) {
      let tableHeader = [], historyColumObj = {}, tableData = [], echartsData = [], lastData = [], res = [], echartData = [],
        header = ['Disk name', 'Mountpoint', 'Total space (GB)', 'Used space (GB)', 'Usage rate']
      result[0].forEach((aitem, aindex) => {
        result[1].forEach((bitem, bindex) => {
          if (aitem.labels.instance.split(':')[0] === bitem.labels.from_instance && aitem.labels.devices === bitem.labels.devices) {
            tableData.push(aitem)
            echartsData.push(bitem)
          }
        });
      });
      echartsData.forEach((bitem, bindex) => {
        let seriesData = []
        bitem.values.forEach((item, index) => {
          seriesData.push((item * 100).toFixed(2))
        });
        let echartsitem = {
          'legend': [{ image: "", description: 'Usage Rate' }],
          'xAxisData': bitem.timestamps,
          'seriesData': [{ data: seriesData, description: 'Usage Rate', colors: '#2DA769' }], 'flg': 1, 'legendFlg': 1, 'unit': '%'
        }
        echartData.push(echartsitem)
        lastData.push(bitem.values[bitem.values.length - 1])
      });
      tableData.forEach((item, index) => {
        let tabledata = {}
        tabledata["Disk name"] = item.labels.device
        tabledata["Mountpoint"] = item.labels.mountpoint
        tabledata["Total space (GB)"] = (item.values / 1024 / 1024 / 1024).toFixed(2)
        tabledata["Used space (GB)"] = (item.values / 1024 / 1024 / 1024 * lastData[index]).toFixed(2)
        tabledata["Usage rate"] = (lastData[index] * 100).toFixed(2) + '%'
        tabledata['key'] = index
        res.push(tabledata)
      });
      header.forEach((item, index) => {
        historyColumObj = {
          title: formatTableTitle(item),
          dataIndex: item,
          ellipsis: true,
          width: 180,
          key: index,
          ...(index > 1 && {
            sorter: (a, b) => {
              let aVal = a[item]
              let bVal = b[item]
              let c = isFinite(aVal),
                d = isFinite(bVal);
              return (c !== d && d - c) || (c && d ? aVal - bVal : aVal.localeCompare(bVal));
            }
          })
        }
        tableHeader.push(historyColumObj)
      })
      this.setState(() => ({
        loadingLock: false,
        lockDataSource: res,
        columns: tableHeader,
        echartData: echartData,
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
      return <a style={{ color: 'black' }} onClick={e => {
        props.onExpand(props.record, e);
      }}><UpOutlined /></a>
    } else {
      return <a style={{ color: 'black' }} onClick={e => {
        props.onExpand(props.record, e);
      }}><DownOutlined /></a>
    }
  }
  componentDidMount() {
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
      <Card title="Storage" className="mb-10">
        <Table
          rowKey={record => record.key} columns={columns} components={this.components} dataSource={this.state.lockDataSource}
          pagination={this.state.lockPagination} loading={this.state.loadingLock} scroll={{ x: '100%' }}
          expandIcon={(props) => this.customExpandIcon(props)}
          expandable={{
            expandedRowRender: (record, index) => (
              <NodeEchartFormWork echartData={this.state.echartData[index]} />
            ),
            // rowExpandable: (record) => record.name !== 'Not Expandable',
          }}
        />
      </Card>
    )
  }
}
