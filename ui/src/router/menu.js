import React from 'react';
import { DashboardOutlined, ToolOutlined, CalculatorOutlined, ConsoleSqlOutlined, ApartmentOutlined, SecurityScanOutlined, CodeSandboxOutlined } from '@ant-design/icons';
const menusList = [
  {
    label: 'Overview',
    icon: <ConsoleSqlOutlined />,
    key: '/overview',
  }, {
    label: 'Autonomouse Management',
    icon: <ApartmentOutlined />,
    key: '/AutonomouseManagement',
    children:[{
      label: 'Metric',
      icon: '',
      key: '/AutonomouseManagement/nodeinfor',
    },{
      label: 'Alarm',
      icon: '',
      key: '/AutonomouseManagement/alarms',
    },{
      label: 'Security Management',
      icon: '',
      key: '/AutonomouseManagement/securitymanagement',
    }]
  }, {
    label: 'Database Optimization',
    icon: <CalculatorOutlined />,
    key: '/DatabaseOptimization',
    children:[{
      label: 'Database Tuning',
      icon: '',
      key: '/DatabaseOptimization/metric',
    },{
      label: 'Slow Query Diagnosis',
      icon: '',
      key: '/DatabaseOptimization/slowqueryanalysis',
    },{
      label: 'Regular Inspections',
      icon: '',
      key: '/DatabaseOptimization/regularinspections',
    },{
      label: 'SQL Intelligent Collection',
      icon: '',
      key: '/DatabaseOptimization/intelligentsqlcondition',
    }]
  }, {
    label: 'AI-Toolkit ',
    icon: <ToolOutlined />,
    key: '/Aitoolkit',
    children:[{
      label: 'Index Advisor',
      icon: '',
      key: '/Aitoolkit/indexadvisor',
    },{
      label: 'Query Tuning',
      icon: '',
      key: '/Aitoolkit/querytuning',
    },{
      label: 'Intelligent Sql Analysis',
      icon: '',
      key: '/Aitoolkit/intelligentsqlanalysis',
    },{
      label: 'Risk Analysis',
      icon: '',
      key: '/Aitoolkit/riskanalysis',
    }]
  }, {
    label: 'DBMind Settings ',
    icon: <CodeSandboxOutlined />,
    key: '/dbmind-settings',
  }
];
export default menusList
