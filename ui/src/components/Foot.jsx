import React, { Component } from 'react';

export default class Foot extends Component {
  constructor() {
    super()
    this.state = {
      footerData: [
        {
          key: 1,
          name: 'About openGauss DBMind'
        }, {
          key: 2,
          name: 'Community'
        }, {
          key: 3,
          name: 'More products'
        }, {
          key: 4,
          name: 'Helps'
        }
      ]
    }
  }
  render () {
    return (
      <div className="footer" style={{ width: '100%', height: 40, display: 'flex', justifyContent: 'center', position: 'absolute', bottom: 0}}>
          <div className="top" style={{ display: 'flex', justifyContent: 'space-between',alignItems:'center', width: '28%',fontSize:14, color:'#5f5f5f' }}>
            {
              this.state.footerData.map(item => {
                return (
                  <span className="pointer" key={item.key}>{item.name}</span>
                )
              })
            }
          </div>
      </div>
    )
  }
}
