/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-DBMind is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

import React from "react";
import {
  ToolOutlined,
  CalculatorOutlined,
  ConsoleSqlOutlined,
  ApartmentOutlined,
  CodeSandboxOutlined,
} from "@ant-design/icons";
const menusList = [
  {
    label: "Overview",
    icon: <ConsoleSqlOutlined />,
    key: "/overview",
  },
  {
    label: "Autonomous Management",
    icon: <ApartmentOutlined />,
    key: "/AutonomousManagement",
    children: [
      {
        label: "Metric",
        icon: "",
        key: "/AutonomousManagement/nodeinfor",
      },
      {
        label: "Alarm",
        icon: "",
        key: "/AutonomousManagement/alarms",
      },
    ],
  },
  {
    label: "Database Optimization",
    icon: <CalculatorOutlined />,
    key: "/DatabaseOptimization",
    children: [

      {
        label: "Slow Query Diagnosis",
        icon: "",
        key: "/DatabaseOptimization/slowqueryanalysis",
      },
      {
        label: "SQL Intelligent Collection",
        icon: "",
        key: "/DatabaseOptimization/intelligentsqlcondition",
      },
    ],
  },
  {
    label: "AI-Toolkit ",
    icon: <ToolOutlined />,
    key: "/Aitoolkit",
    children: [
      {
        label: "Index Advisor",
        icon: "",
        key: "/Aitoolkit/indexadvisor",
      },
      {
        label: "Query Tuning",
        icon: "",
        key: "/Aitoolkit/querytuning",
      },
      {
        label: "Intelligent Sql Analysis",
        icon: "",
        key: "/Aitoolkit/intelligentsqlanalysis",
      },
      {
        label: "Risk Analysis",
        icon: "",
        key: "/Aitoolkit/riskanalysis",
      },
    ],
  },
  {
    label: "DBMind Settings ",
    icon: <CodeSandboxOutlined />,
    key: "/dbmind-settings",
  },
];
export default menusList;
