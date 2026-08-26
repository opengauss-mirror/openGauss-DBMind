import React from 'react';
import { Result, Button } from 'antd';
import { withRouter } from 'react-router-dom';

function NotFound({ history }) {
  const goHome = () => history.push('/overview');
  return (
    <div style={{ padding: '40px 0' }}>
      <Result
        status="404"
        title="404"
        subTitle="Sorry, the page you visited does not exist."
        extra={<Button type="primary" onClick={goHome}>Back to Overview</Button>}
      />
    </div>
  );
}

export default withRouter(NotFound);

