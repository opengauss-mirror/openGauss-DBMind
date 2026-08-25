import React from 'react';
import { Skeleton, Card, Row, Col } from 'antd';

// 轻量整页骨架，用于 Suspense fallback；不依赖业务数据
export default function SkeletonPage() {
  return (
    <div style={{ padding: 16 }}>
      <Row gutter={[16, 16]}>
        <Col xs={24} md={12}>
          <Card bordered={false} bodyStyle={{ padding: 16 }}>
            <Skeleton active title paragraph={{ rows: 4 }} />
          </Card>
        </Col>
        <Col xs={24} md={12}>
          <Card bordered={false} bodyStyle={{ padding: 16 }}>
            <Skeleton active title paragraph={{ rows: 4 }} />
          </Card>
        </Col>
        <Col span={24}>
          <Card bordered={false} bodyStyle={{ padding: 16 }}>
            <Skeleton active title paragraph={{ rows: 6 }} />
          </Card>
        </Col>
      </Row>
    </div>
  );
}

