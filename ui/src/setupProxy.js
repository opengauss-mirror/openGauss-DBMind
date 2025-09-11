const { createProxyMiddleware } = require('http-proxy-middleware');
const fs = require('fs');
const path = require('path');
const net = require('net');

module.exports = function (app) {
  // 动态获取后端服务地址
  const getBackendUrl = async () => {
    // 优先级1: 环境变量
    if (process.env.REACT_APP_BASE_URL) {
      const baseUrl = process.env.REACT_APP_BASE_URL;
      return baseUrl.startsWith('http') ? baseUrl : `http://${baseUrl}`;
    }

    // 优先级2: 从DBMind配置文件读取
    const configPort = await readDBMindConfig();
    if (configPort) {
      return `http://127.0.0.1:${configPort}`;
    }

    // 优先级3: 端口扫描检测
    const detectedPort = await detectRunningPort();
    if (detectedPort) {
      return `http://127.0.0.1:${detectedPort}`;
    }

    // 优先级4: 默认端口
    console.warn('[Proxy] 无法检测到后端端口，使用默认端口 8088');
    return 'http://127.0.0.1:8088';
  };

  // 动态发现DBMind配置文件路径
  const findDBMindConfigPaths = async () => {
    const configPaths = [];
    
    // 1. 优先级最高：环境变量指定的配置路径
    if (process.env.DBMIND_CONFIG_DIR) {
      configPaths.push(path.join(process.env.DBMIND_CONFIG_DIR, 'dbmind.conf'));
    }
    
    // 2. 从运行中的DBMind进程推断配置路径
    const processConfigPath = await getConfigFromRunningProcess();
    if (processConfigPath) {
      configPaths.push(processConfigPath);
    }
    
    // 3. 在当前项目目录及其父目录中搜索所有可能的配置文件
    const projectRoot = process.cwd();
    const searchDirs = [
      projectRoot,
      path.dirname(projectRoot),
      path.dirname(path.dirname(projectRoot))
    ];
    
    for (const dir of searchDirs) {
      // 搜索所有以 conf 结尾的目录
      try {
        const entries = fs.readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
          if (entry.isDirectory()) {
            const dirName = entry.name.toLowerCase();
            if (dirName.includes('conf') || dirName.includes('config') || dirName === 'dbmindconf') {
              const confPath = path.join(dir, entry.name, 'dbmind.conf');
              if (fs.existsSync(confPath)) {
                configPaths.push(confPath);
              }
            }
          }
        }
      } catch (error) {
        // 忽略无法读取的目录
      }
    }
    
    // 4. 回退到常见的配置文件位置
    const fallbackPaths = [
      path.join(__dirname, '../../dbmindconf/dbmind.conf'),
      path.join(process.cwd(), 'dbmindconf/dbmind.conf'),
      path.join(process.cwd(), '../dbmindconf/dbmind.conf'),
      '/etc/dbmind/dbmind.conf',
      path.join(process.env.HOME || '', '.dbmind/dbmind.conf')
    ];
    
    configPaths.push(...fallbackPaths);
    
    // 去重并只返回存在的配置文件
    const uniquePaths = [...new Set(configPaths)];
    return uniquePaths.filter(p => fs.existsSync(p));
  };

  // 从运行中的进程获取配置路径
  const getConfigFromRunningProcess = async () => {
    try {
      const { execSync } = require('child_process');
      const psOutput = execSync('ps aux | grep "gs_dbmind\\|dbmind" | grep -v grep', { encoding: 'utf8' });
      
      const lines = psOutput.split('\n').filter(line => line.trim());
      for (const line of lines) {
        // 查找 -c 参数
        const match = line.match(/-c\s+([^\s]+)/);
        if (match) {
          const configDir = match[1];
          const configFile = path.join(configDir, 'dbmind.conf');
          if (fs.existsSync(configFile)) {
            console.log(`[Proxy] 从运行进程发现配置文件: ${configFile}`);
            return configFile;
          }
        }
      }
    } catch (error) {
      // 忽略进程检测失败
    }
    return null;
  };

  // 读取DBMind配置文件
  const readDBMindConfig = async () => {
    try {
      const configPaths = await findDBMindConfigPaths();
      
      if (configPaths.length === 0) {
        console.log('[Proxy] 未找到任何DBMind配置文件');
        return null;
      }
      
      console.log(`[Proxy] 发现 ${configPaths.length} 个配置文件候选:`, configPaths);

      for (const configPath of configPaths) {
        try {
          const content = fs.readFileSync(configPath, 'utf8');
          
          // 查找[WEB-SERVICE]段落下的port配置
          const webServiceMatch = content.match(/\[WEB-SERVICE\]([\s\S]*?)(?=\[|$)/);
          if (webServiceMatch) {
            const webServiceSection = webServiceMatch[1];
            const portMatch = webServiceSection.match(/^\s*port\s*=\s*(\d+)/m);
            if (portMatch) {
              const port = parseInt(portMatch[1]);
              console.log(`[Proxy] 从配置文件读取Web服务端口: ${port} (${configPath})`);
              return port;
            }
          }
          
          // 回退：尝试匹配任何port配置（但跳过TSDB的端口）
          const lines = content.split('\n');
          let inWebService = false;
          
          for (const line of lines) {
            const trimmed = line.trim();
            if (trimmed === '[WEB-SERVICE]') {
              inWebService = true;
              continue;
            }
            if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
              inWebService = false;
              continue;
            }
            
            if (inWebService && trimmed.match(/^port\s*=\s*(\d+)/)) {
              const match = trimmed.match(/^port\s*=\s*(\d+)/);
              if (match) {
                const port = parseInt(match[1]);
                console.log(`[Proxy] 从配置文件读取Web服务端口: ${port} (${configPath})`);
                return port;
              }
            }
          }
        } catch (error) {
          console.warn(`[Proxy] 读取配置文件失败: ${configPath}`, error.message);
        }
      }
    } catch (error) {
      console.warn('[Proxy] 配置文件检测失败:', error.message);
    }
    return null;
  };

  // 检测正在运行的端口
  const detectRunningPort = async () => {
    const candidatePorts = [8088, 8080, 9090, 8000, 3000];
    
    for (const port of candidatePorts) {
      if (await isPortOpen('127.0.0.1', port)) {
        // 进一步检测是否是DBMind服务
        if (await isDBMindService('127.0.0.1', port)) {
          console.log(`[Proxy] 检测到DBMind服务运行在端口: ${port}`);
          return port;
        }
      }
    }
    return null;
  };

  // 检查端口是否开启
  const isPortOpen = (host, port) => {
    return new Promise((resolve) => {
      const socket = new net.Socket();
      const timeout = 1000;
      
      socket.setTimeout(timeout);
      socket.on('connect', () => {
        socket.destroy();
        resolve(true);
      });
      
      socket.on('timeout', () => {
        socket.destroy();
        resolve(false);
      });
      
      socket.on('error', () => {
        resolve(false);
      });
      
      socket.connect(port, host);
    });
  };

  // 检查是否是DBMind服务
  const isDBMindService = async (host, port) => {
    try {
      const http = require('http');
      return new Promise((resolve) => {
        const req = http.get(`http://${host}:${port}/v1/api/summary/real-time-inspection/list`, {
          timeout: 2000
        }, (res) => {
          // 只要有响应就认为是DBMind服务（包括401认证错误）
          resolve(true);
        });
        
        req.on('error', () => resolve(false));
        req.on('timeout', () => {
          req.destroy();
          resolve(false);
        });
      });
    } catch {
      return false;
    }
  };

  // 异步初始化代理
  const initializeProxy = async () => {
    const backendUrl = await getBackendUrl();
    console.log(`[Proxy] 代理配置: /transpond -> ${backendUrl}/v1/api`);

    return createProxyMiddleware({
      target: backendUrl,
      changeOrigin: true,
      pathRewrite: {
        '^/transpond': '/v1/api'
      },
      onError: (err, req, res) => {
        console.error('[Proxy] 代理请求失败:', err.message);
        res.status(500).json({
          success: false,
          msg: `代理请求失败: ${err.message}`
        });
      },
      onProxyReq: (proxyReq, req, res) => {
        console.log(`[Proxy] ${req.method} ${req.url} -> ${backendUrl}${req.url.replace('/transpond', '/v1/api')}`);
      },
      logLevel: process.env.NODE_ENV === 'development' ? 'info' : 'warn'
    });
  };

  // 初始化代理中间件
  initializeProxy().then(proxyMiddleware => {
    app.use('/transpond', proxyMiddleware);
  }).catch(error => {
    console.error('[Proxy] 初始化代理失败:', error);
    // 使用默认配置作为回退
    app.use('/transpond', createProxyMiddleware({
      target: 'http://127.0.0.1:8088',
      changeOrigin: true,
      pathRewrite: {
        '^/transpond': '/v1/api'
      }
    }));
  });
};