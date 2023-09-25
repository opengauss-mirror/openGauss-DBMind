import React, { Component } from 'react';
import { Card, Table } from 'antd';
import { UpOutlined, DownOutlined } from '@ant-design/icons';
import NodeEchartFormWork from '../../../NodeInformation/NodeModules/NodeEchartFormWork';
import ResizeableTitle from '../../../common/ResizeableTitle';
import { formatTableTitle } from '../../../../utils/function';


export default class DBCapacityMetric extends Component {
    static propTypes = {
        dbCapacityMetric: PropTypes.object.isRequired,
    };
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

    getdbCapacity(data) {
        let result = data.pg_database_size_bytes ?? [];
        if (result[0]) {
            let tableHeader = [], historyColumObj = {}, res = [], echartData = [],
                header = ['Database', 'Used Space (MB)']
            result.forEach((item, index) => {
                let seriesData = [], tabledata = {}
                item.values.forEach((bitem, index) => {
                    seriesData.push(bitem.toFixed(2))
                });
                let echartsitem = {
                    'legend': [{ image: '', description: 'Usage Space (MB)' }],
                    'xAxisData': item.timestamps,
                    'seriesData': [{ data: seriesData, description: 'Usage Space (MB)', colors: '#5990FD' }], 'flg': 0, 'legendFlg': 2, title: 'Usage Space (MB)', 'unit': ''
                }
                echartData.push(echartsitem)
                tabledata["Database"] = item.labels.datname
                tabledata["Used Space (MB)"] = (item.values[item.values.length - 1]).toFixed(2)
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
                    ...(index && {
                        sorter: (a, b) => {
                            let aVal = a[item]
                            let bVal = b[item]
                            let c = isFinite(aVal),
                                d = isFinite(bVal);
                            return (c !== d && d - c) || (c && d ? aVal - bVal : aVal.localeCompare(bVal));
                        }
                    }),

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
            return <span style={{ color: 'black' }} onClick={e => {
                props.onExpand(props.record, e);
            }}><UpOutlined /></span>
        } else {
            return <span style={{ color: 'black' }} onClick={e => {
                props.onExpand(props.record, e);
            }}><DownOutlined /></span>
        }
    }
    UNSAFE_componentWillReceiveProps(nextProps) {
        this.getdbCapacity(nextProps.dbCapacityMetric);
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
            <Card title="Capacity Metric" className="mb-10">
                <Table
                    rowKey={record => record.key} columns={columns} components={this.components} dataSource={this.state.lockDataSource}
                    pagination={this.state.lockPagination} loading={this.state.loadingLock} scroll={{ x: '100%' }}
                    expandIcon={(props) => this.customExpandIcon(props)}
                    expandable={{
                        expandedRowRender: (record, index) => (
                            <NodeEchartFormWork echartData={this.state.echartData[index]} />
                        ),
                    }}
                />
            </Card>
        )
    }
}
