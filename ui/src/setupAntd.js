// 仅做全局 antd 配置（入口引用），保持最小职责
import { message } from 'antd';

// 避免错误提示堆叠轰炸；与拦截器的去重策略互补
message.config({ maxCount: 1 });

export default message;

