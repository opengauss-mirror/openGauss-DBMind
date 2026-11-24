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

// 仅做全局 antd 配置（入口引用），保持最小职责
import { message } from 'antd';

// 避免错误提示堆叠轰炸；与拦截器的去重策略互补
message.config({ maxCount: 1 });

export default message;

