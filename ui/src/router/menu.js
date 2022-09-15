import React from 'react';
import { DashboardOutlined, ToolOutlined, CalculatorOutlined, ConsoleSqlOutlined, ApartmentOutlined, SecurityScanOutlined, CodeSandboxOutlined } from '@ant-design/icons';
const menusList = [
  {
    label: 'Overview',
    icon: <ConsoleSqlOutlined />,
    key: '/overview',
  }, {
    label: 'Node Information',
    icon: <ApartmentOutlined />,
    key: '/nodeinfor',
  }, {
    label: 'Autonomouse Management',
    icon: <DashboardOutlined />,
    key: '/autonomousmanagement',
  }, {
    label: 'Database Optimization',
    icon: <CalculatorOutlined />,
    key: '/databaseoptimization',
  }, {
    label: 'Security Management',
    icon: <SecurityScanOutlined />,
    key: '/securitymanagement',
  }, {
    label: 'AI-Toolkit ',
    icon: <ToolOutlined />,
    key: '/aitoolkit',
  }, {
    label: 'DBMind Settings ',
    icon: <CodeSandboxOutlined />,
    key: '/dbmind-settings',
  }
];
export default menusList
