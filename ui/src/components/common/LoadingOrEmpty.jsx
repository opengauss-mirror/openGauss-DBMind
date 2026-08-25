import React from 'react';
import { Spin, Empty } from 'antd';

function LoadingOrEmpty({
  loading = false,
  hasData = true,
  height = 200,
  emptyDescription = '',
  children,
  style = {},
}) {
  const containerStyle = {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    width: '100%',
    height,
    ...style,
  };

  if (loading) {
    return (
      <div style={containerStyle} data-testid="loading">
        <Spin />
      </div>
    );
  }

  if (!hasData) {
    return (
      <div style={containerStyle} data-testid="empty">
        <Empty description={emptyDescription} />
      </div>
    );
  }

  return <>{children}</>;
}

export default React.memo(LoadingOrEmpty);
