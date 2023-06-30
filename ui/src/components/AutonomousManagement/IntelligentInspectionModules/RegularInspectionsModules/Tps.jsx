import React, { Component } from 'react';
import { Card, Table } from 'antd';
import PropTypes from 'prop-types';
import ResizeableTitle from '../../../common/ResizeableTitle';
import '../../../../assets/css/main/databaseOptimization.css';
import { formatTableTitle } from '../../../../utils/function';

export default class Tps extends Component {
  static propTypes={
    tpsData:PropTypes.object.isRequired
  }
  constructor(props) {
    super(props)
    this.state = {
      dataSource: [],
  
    }
  }
  components = {
    header: {
      cell: ResizeableTitle,
    },
  };
  handleTableData (data) {
    let tpsArr = [];

    for (let i in data) {
      let activeObj = {
        name: i,
        value: data[i],
      };
      tpsArr.push(activeObj);
    }
    this.setState(() => ({
      dataSource: tpsArr,
     
    }))
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
  UNSAFE_componentWillReceiveProps (nextProps) {
    this.handleTableData(nextProps.tpsData)
  }
  render () {
    
    return (
      <div className='tableDiv mb-10'>
        <Card title="TPS" style={{height:110}} className="tps">
         
          <ul style={{width:'100%',display:'flex'}}>
          {this.state.dataSource.map((item) => {
              return (
                <li className="connectionTitle">
                  <p>{item.name}</p>
                  <h4>{item.value}</h4>
                </li>
              );
            })}
         </ul>
        </Card>
      </div>
    )
  }
}
