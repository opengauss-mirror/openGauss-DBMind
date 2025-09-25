# DBMind
[中文](#dbmind-中文) | [English](#dbmind-engish)

Maintainer: [openGauss AI-SIG](mailto:ai@opengauss.org)


# DBMind-中文
DBMind作为openGauss数据库的一部分，为openGauss数据库提供了自动驾驶能力，是一款领先的开源数据库自治运维平台。通过DBMind, 您可以很容易地发现数据库的问题，同时可以实现秒级的数据库问题根因分析。

DBMind的特点：
- DBMind采用了先进的插件化的架构形式，支持海量插件扩展；
- 支持多种运行模式，具备命令行交互式运行、服务式运行；
- 面向云原生进行设计，支持Prometheus，并提供多种丰富的exporter插件；
- 提供丰富的对接模式，可以很容易地与现有管理系统进行对接，支持RESTful API、Python SDK、命令行、Prometheus协议等模式；
- 支持端到端全流程的数据库自治运维能力，包括慢SQL根因分析、workload索引推荐、多指标关联挖掘、故障自修复、异常检测与根因分析等功能；

DBMind支持的主要能力：
- 索引推荐
- 异常检测与分析
- 多指标关联分析
- 慢SQL根因分析
- 风险分析
- 参数调优与推荐
- SQL改写与优化
- 集群故障诊断

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'lineColor': '#00BFFF' }}}%%
flowchart LR
    subgraph _[ ]
        client(client) & Grafana(Grafana)
    end
    style _ fill:none, stroke:none
    style Grafana stroke:#333,stroke-width:3px,stroke-dasharray:5

    DBMind_Service(DBMind<br>Service)
    style DBMind_Service fill:#0077be,stroke:#000,color:#fff

    client --> |"pull results"| DBMind_Service
   
    subgraph __[" "]
        Prometheus_server[(Prometheus<br>server)] 
        metadata_storage[(metadata<br>storage)]
    end
    style __ fill:none, stroke:none
    
    DBMind_Service --> |"pull metrics"| Prometheus_server
    DBMind_Service --> |"read & write results"|metadata_storage
    
    joint[.]
    style joint width:0, height:0

    Prometheus_server --> |"scrape: pull metrics"| joint
    
    subgraph "metrics collector"
        openGauss[(openGauss)] & opengauss_exporter(opengauss-<br>exporter) & node_exporter(node-exporter) & cmd_exporter(cmd-exporter)
    end
    
    subgraph "secondary operation"
        reprocessing_exporter(reprocessing<br>exporter)
    end
    
    joint --> reprocessing_exporter
    joint --> openGauss
    joint --> opengauss_exporter
    joint --> node_exporter
    joint --> cmd_exporter
    DBMind_Service --> |"control"| openGauss
    linkStyle 9 stroke-dasharray:5;
```

图中各关键组件说明：
- DBMind Service：DBMind后台服务，可用于定期离线计算，包括慢SQL根因分析、时序预测等；
- Prometheus-server：存储Prometheus监控指标的服务器；
- metadatabase：DBMind在离线计算结束后，将计算结果存储在此处，支持openGauss、SQLite等数据库；
- client：用户读取DBMind离线计算结果的客户端，目前该客户端仅支持命令行操作；若采用支持openGauss等数据库存储DBMind的计算结果，则用户可以自行配置Grafana等可视化工具，用于对该结果进行可视化；
- opengauss-exporter：用户从支持openGauss数据库节点上采集监控指标，供DBMind服务进行计算；
- node-exporter：Prometheus官方提供的exporter，可用于监控对应节点的系统指标，如CPU和内存使用情况；
- cmd-exporter：在用户安装数据库的环境上执行命令行，并采集该命令行的执行结果，同时，也尝试将数据库日志内容转化为监控指标；例如通过执行cm_ctl命令，查看数据库实例的状态；
- reprocessing-exporter：用于对Prometheus采集到的指标进行二次加工处理，例如计算CPU使用率等。

除上述服务架构外，DBMind也支持以无状态的微服务模式启动。在该模式下，DBMind不进行实例管理，即不对实例进行纳管，运行中不存储实例信息。当需要针对特定的实例执行特定的功能时，需要从接口传入对应的实例连接信息。这意味着，DBMind以微服务模式启动时，所依赖的外部状态全部由接口传入或是配置在元数据库中，因此可以分布式地部署多个DBMind服务，从而在调用功能时实现负载均衡、服务高可用与单节点异常时的秒级切换。

除上述使用的Prometheus-server外，DBMind现已支持从InfluxDB-server中提取依赖的指标数据。但是采集组件Exporter当前不支持向InfluxDB写入指标。因此，需要确保DBMind所配置的TSDB中有全量DBMind正常工作所依赖的指标数据。

## 开始使用DBMind
### 下载并安装DBMind
DBMind基于Python语言实现，在使用DBMind时，需要运行环境具备Python虚拟机，同时安装好所需的第三方依赖。

#### 方式一：直接下载代码部署
DBMind主要使用Python语言进行编写，因此，可以在下载获取DBMind的源代码后，使用操作系统上安装的Python虚拟机直接运行，不过该过程中的第三方依赖需要用户手动安装。

用户可以通过 `git clone` 命令从gitcode上下载代码，例如：

```
git clone https://gitcode.com/opengauss/openGauss-DBMind.git -b master-dev openGauss-DBMind
```

下载DBMind后，会产生一个名为 `openGauss-DBMind` 的目录， 将该目录的路径添加到环境变量`PATH`中，即可调用该目录中的可执行文件。例如可以执行下述命令完成：
```
chmod +x openGauss-DBMind/gs_dbmind

echo PATH=`pwd`/openGauss-DBMind:'$PATH' >> ~/.bashrc
echo 'export PATH' >> ~/.bashrc

source ~/.bashrc
```

#### 关于Python运行环境
需要至少为Python3.7的版本。虽然在DBMind的实现中对Python3.7以下的环境尽可能地进行了兼容，但是这些低版本的Python环境疏于测试，可能会引发意料之外的异常。同时，在DBMind启动时，也会尝试校验Python版本，如果Python版本不符合要求，则默认不会继续执行后续的动作。

*DBMind的Python版本由根目录下的constant文件中的变量做约束*

如果您的环境需要安装多个版本的Python运行时，并且它们可能会引起冲突，那么我们建议您将DBMind所需的Python运行环境安装到DBMind根目录下的 `python` 目录中，DBMind会优先选择使用在其根目录下 `python` 目录中的环境。即 `gs_dbmind` 命令会首先在`python/bin` 目录下寻找 `python3` 命令执行后续的Python功能。


#### 关于第三方依赖
DBMind所使用的第三方依赖通过DBMind根目录下的 `requirements-xxx.txt` 文件指定。对于x86架构（amd64）以及ARM架构（aarch64），使用了不同的文件名进行标识。这是因为ARM平台对于某些第三方依赖并不友好，必须指定特定的版本才可以安装。

可以使用pip工具对第三方依赖进行安装。与前文所述的情况类似，如果您当前的操作系统不得不安装多个Python运行环境，那么，DBMind也支持对第三方依赖进行优先选择。即可以将第三方依赖库存储到DBMind根目录下的 `3rd` 目录中。 在通过 `gs_dbmind` 命令使用DBMind功能时，会优先选择该目录下的 `3rd` 目录中的第三方依赖库进行加载。

以x86环境为例，可以使用下述`pip`命令安装DBMind的第三方依赖库：

```
python3 -m pip install -r requirements-x86.txt
```

如果希望指定下载的第三方依赖库地址，则可以通过 `--target` 或 `-t` 选项进行指定，例如
```
python3 -m pip install -r requirements-x86.txt -t 3rd
```

#### 使用 setup_env.sh 快速准备依赖和前端

如果希望在仓库根目录内自动完成 Python 依赖安装与前端构建，可以使用新增的 `setup_env.sh` 脚本（仅支持 `x86_64` 与 `aarch64` 平台）：

```
chmod +x setup_env.sh
./setup_env.sh
```

脚本会执行以下操作：
- 下载并安装 Miniconda3（安装目录为仓库根目录下的 `python/`），并将其加入当前会话的 `PATH`；
- 下载并解压 Node.js v16.9.0（目录为仓库根目录下的 `node-v16.9.0-linux-<arch>/`）；
- 使用 `python/bin/python -m pip install -r requirements-<arch>.txt` 安装对应架构的第三方库；
- 进入 `ui/` 目录执行 `npm install` 与 `npm run build`，产物会同步到后端静态目录。

如已自行安装 Python 或 Node.js，可以跳过脚本的相关部分；若需要刷新依赖或重新构建前端，重复执行脚本即可。

### 使用DBMind
#### 部署Prometheus
可以通过 Prometheus 官方网站（https://prometheus.io/download/）或企业内部镜像获取适配操作系统与架构的安装包。选择与部署环境 CPU 架构匹配的压缩包解压后，目录中包含默认的 `prometheus.yml` 配置文件，可按照业务需求将 openGauss exporter、node exporter 等目标写入 `scrape_configs`。完成配置后，可在解压目录中执行以下命令启动 Prometheus 服务并加载自定义配置：
```
./prometheus --config.file=prometheus.yml --web.listen-address=0.0.0.0:9090
```
其中 `--config.file` 指定 Prometheus 的配置文件路径，`--web.listen-address` 用于声明监控 UI 与接口的监听地址与端口。首次部署建议在前台运行观察日志，确认采集目标正常后再结合 `systemd`、`supervisor` 或容器化方式转入后台运行。

#### 部署Node Exporter
Node Exporter 需要安装在每台被监控的 Linux 服务器（或容器）上，用于采集 CPU、内存、磁盘等系统指标，是 Prometheus 采集 openGauss 运行环境状态的前提。可从官方页面（https://prometheus.io/download/#node_exporter）下载对应平台的压缩包，选择合适的架构后解压至本地目录，进入解压目录即可执行二进制文件。默认监听 `9100` 端口，并提供 `/metrics` 接口供 Prometheus 抓取：
```
./node_exporter --web.listen-address=0.0.0.0:9100
```
建议将上述命令写入系统自启动脚本或 `systemd` 单元中，保证随主机启动；若部署在容器内，则需在宿主机执行端口映射并确保防火墙允许访问。Prometheus 配置中应添加对应主机的 `<host>:9100` 作为抓取目标，以便 DBMind 的 AI 功能能够使用到这些系统指标。

### 启动 DBMind 组件
如果希望将DBMind作为后台服务运行，则下面的DBMind组件是必须安装的，否则获取不到数据库的监控信息。为了获得更高的安全机制，DBMind提供的exporter默认是使用Https协议的，如果您觉得您的场景中不需要使用Https协议，则可以通过 `--disable-https` 选项禁用。

#### 部署openGauss Exporter
openGauss exporter 从openGauss数据库中读取系统表（或系统视图）的数据，并通过Prometheus存储起来。由于openGauss exporter需要读取监控数据库的系统表信息，因此至少应该具备 **monadmin** 权限。例如，可以通过下述SQL语句为名为 `dbmind_monitor` 用户赋予权限：
```
ALTER USER dbmind_monitor monadmin;
```

使用 `gs_dbmind component opengauss_exporter ...` 命令即可启动该openGauss exporter组件。例如，可以通过下述命令监控某个数据库，通过 `--url` 参数指定被监控的数据库实例地址：
```
gs_dbmind component opengauss_exporter --url postgresql://user:password@ip:port/dbname --web.listen-address 192.168.1.100 --ssl-keyfile server.key --ssl-certfile server.crt --ssl-ca-file server.crt
```
opengauss-exporter多节点部署模式示例，URL参数中包含多个节点地址，下面示例使用默认侦听端口号9187，侦听地址为192.168.1.100，URL参数中包含三个节点地址，采用HTTPS协议，则命令可以为：
```
gs_dbmind component opengauss_exporter --url postgresql://user:password@ip1:port1,ip2:port2,ip3:port3/dbname --web.listen-address 192.168.1.100 --ssl-keyfile server.key --ssl-certfile server.crt --ssl-ca-file server.crt
```

`--url` 表示的是数据库的DSN地址，其格式可以[参考此处](#dsn的格式说明)。

可以通过下述命令检查openGauss exporter是否已经启动：
```
curl -vv http://localhost:9187/metrics
```

#### Reprocessing Exporter
reprocessing exporter 是一个用于二次加工处理数据的exporter. 由于node exporter、openGauss exporter保存到Prometheus中的数据是即时的监控信息，而只通过这些信息是无法反应某些指标的瞬时增量信息的，例如TPS、iops信息等。因此，reprocessing exporter可以用来计算增量信息或者聚合结果等。

由于reprocessing是从Prometheus中获取指标数据，进行二次加工处理后再返回给Prometheus. 因此，它与Prometheus是一一对应的，即如果只有一个Prometheus服务，则只需要一个reprocessing exporter即可。例如，可以通过下述命令启动reprocessing exporter:
```
gs_dbmind component reprocessing_exporter 192.168.1.100 9090 --web.listen-address 192.168.1.101 --ssl-keyfile server.key --ssl-certfile server.crt --ssl-ca-file server.crt
```
如果您的Prometheus使用了`basic authorization`方式进行登录校验，则需要额外指定 `--prometheus-auth-user` 以及 `--prometheus-auth-password` 选项的值。

#### Cmd Exporter
cmd_exporter是一个用来执行cmd命令并获取返回结果以及采集日志信息的exporter。应当使用数据库用户在每一个实例节点下启动cmd_exporter，例如，可以通过下述命令启动cmd_exporter:

```
gs_dbmind component cmd_exporter --ssl-keyfile server.key --ssl-certfile server.crt --ssl-ca-file server.crt --pg-log-dir /path/to/pglog
```

可以通过下述命令检查cmd exporter是否已经启动：
```
curl -vv http://localhost:8181/metrics
```

#### 高可用接口
为保证DBMind云上使用时高可靠，exporter组件提供了组件状态查询和部分异常修复接口

API | 入参 | 参数介绍 | 请求方法 | 功能描述与预期返回结果
----|------|---------|---------|----------------------
/v1/api/check-status | cmd | 组件启动命令，String，必选。| POST | 获取exporter组件的状态信息并返回状态详情。
/v1/api/repair | cmd | 组件启动命令，String，必选。| POST | 修复exporter组件并返回修复结果。

### 配置以及启动
DBMind后台服务是常驻内存的。因此，您需要首先配置一个配置文件目录，在该目录中保存多个DBMind的配置文件。可以通过 `gs_dbmind service` 命令来进行配置文件目录的生成以及服务的启动。该命令的使用说明为：

    $ gs_dbmind service --help
    usage:  service [-h] -c DIRECTORY
                    [--only-run {discard_expired_results,anomaly_detection,cluster_diagnose,agent_update_detect,update_statistics,knob_recommend,slow_query_killer,slow_query_diagnosis,calibrate_security_metrics,check_security_metrics}]
                    [--dry-run] [-f] [--interactive | --initialize]
                    {setup,start,stop,restart,reload}
    
    positional arguments:
      {setup,start,stop,restart,reload}
                            perform an action for service
    
    optional arguments:
      -h, --help            show this help message and exit
      -c DIRECTORY, --conf DIRECTORY
                            set the directory of configuration files
      --only-run {discard_expired_results,anomaly_detection,cluster_diagnose,agent_update_detect,update_statistics,knob_recommend,slow_query_killer,slow_query_diagnosis,calibrate_security_metrics,check_security_metrics}
                            explicitly set a certain task running in the backend
      --dry-run             run the backend task(s) once. the task to run can be specified by the --only-run argument
      -f, --force           force to stop the process and cancel all in-progress tasks
      --interactive         configure and initialize with interactive mode
      --initialize          initialize and check configurations after configuring.

下面，分别介绍配置文件目录生成，以及服务的启停操作。
#### 配置DBMind
DBMind提供两种方式进行配置文件的生成。一种是交互式的，通过 `--interactive` 选项指定；另一种则需要用户自己手动来修改，这也是默认方式。

**交互式配置方式**

下面是一些使用示例，这里我们用 `CONF_DIRECTORY` 标识我们的配置文件目录：
```
gs_dbmind service setup -c CONF_DIRECTORY --interactive
```
通过上述命令，用户可以在交互式界面中，根据提示信息输入需要监控的openGauss实例信息和参数。


**手动配置方式**

下面的命令演示了如何通过手动方式进行DBMind配置：
```
gs_dbmind service setup -c CONF_DIRECTORY
```
在执行完上述命令后，会生成一个名为 `CONF_DIRECTORY` 的目录，这个目录里面包含有很多的配置文件。不过，用户需要配置 `CONF_DIRECTORY/dbmind.conf` 文件即可。当用户配置完该文件后，则需要执行一下下述命令，DBMind会根据用户刚刚配置的信息初始化DBMind系统：
```
gs_dbmind service setup -c CONF_DIRECTORY --initialize
```

#### 启动与停止DBMind服务
当用户配置完DBMind数据库后，则可以直接通过下述命令启动DBMind后台服务：
```
gs_dbmind service start -c CONF_DIRECTORY
```
通过下述命令关闭DBMind服务：
```
gs_dbmind service stop -c CONF_DIRECTORY
```

#### 重启DBMind服务
在DBMind运行过程中，则可以直接通过下述命令重启DBMind后台服务：
```
gs_dbmind service restart -c CONF_DIRECTORY
```

#### 实时加载动态参数
在DBMind运行过程中，则可以直接通过下述命令实时加载动态参数并使其生效：
```
gs_dbmind service reload -c CONF_DIRECTORY
```

### DBMind的组件 
如前文所述，DBMind基于一种插件化设计，这个组件（component）即为DBMind提供的插件（plugin）。通过插件式设计，DBMind可以任意进行功能扩展。如果想要使用某个组件的功能，则需要执行`component`子命令:
```
usage:  component [-h] COMPONENT_NAME ...

positional arguments:
  COMPONENT_NAME  choice a component to start. ['anomaly_detection', 'cluster_diagnosis', 'cmd_exporter', 'dkr', 'extract_log', 'index_advisor', 'opengauss_exporter', 'reprocessing_exporter', 'slow_query_diagnosis',
                  'sql_rewriter', 'sqldiag', 'xtuner']
  ARGS            arguments for the component to start

optional arguments:
  -h, --help      show this help message and exit
```

例如其中`xtuner`的组件可以进行数据的参数调优，那么可以执行下述命令来使用`xtuner`的功能：
```
gs_dbmind component xtuner --help
```


## 常见问题
### DSN的格式说明
DSN是Database Source Name的缩写，这里支持两种格式，一种是K-V格式，如`dbname=postgres user=username password=password_value port=6789 host=127.0.0.1`；另一种是URL形式，例如`postgresql://username:password_value@127.0.0.1:6789/postgres`；对于采用URL格式的DSN，由于`@`等特殊字符用来分割URL串中各个部分的内容，故需要URL编码（URL encode）。例如某个用户`dbmind`的密码为`DBMind@123`，则URL形式的DSN可以是`postgresql://dbmind:DBMind%40123@127.0.0.1:6789`，即将`@`字符编码为`%40`. 类似地，需要编码的字符还包括其他可能引起歧义的字符，如`/`, `\`, `?`, `&`.

---

# DBMind-Engish
DBMind is a part of openGauss, which empowers openGauss to carry the autonomous operations and maintenance capabilities. DBMind is leading and open-source. Through DBMind, users can easily discover database problems and the root causes of the problems in seconds.

```mermaid
%%{init: {'theme': 'base', 'themeVariables': { 'lineColor': '#00BFFF' }}}%%
flowchart LR
    subgraph _[ ]
        client(client) & Grafana(Grafana)
    end
    style _ fill:none, stroke:none
    style Grafana stroke:#333,stroke-width:3px,stroke-dasharray:5

    DBMind_Service(DBMind<br>Service)
    style DBMind_Service fill:#0077be,stroke:#000,color:#fff

    client --> |"pull results"| DBMind_Service
   
    subgraph __[" "]
        Prometheus_server[(Prometheus<br>server)] 
        metadata_storage[(metadata<br>storage)]
    end
    style __ fill:none, stroke:none
    
    DBMind_Service --> |"pull metrics"| Prometheus_server
    DBMind_Service --> |"read & write results"|metadata_storage
    
    joint[.]
    style joint width:0, height:0

    Prometheus_server --> |"scrape: pull metrics"| joint
    
    subgraph "metrics collector"
        openGauss[(openGauss)] & opengauss_exporter(opengauss-<br>exporter) & node_exporter(node-exporter) & cmd_exporter(cmd-exporter)
    end
    
    subgraph "secondary operation"
        reprocessing_exporter(reprocessing<br>exporter)
    end
    
    joint --> reprocessing_exporter
    joint --> openGauss
    joint --> opengauss_exporter
    joint --> node_exporter
    joint --> cmd_exporter
    DBMind_Service --> |"control"| openGauss
    linkStyle 9 stroke-dasharray:5;
```

## Getting Started

### Prerequisites
In order to run DBMind, the following components should be configured and running.

#### Python Runtime
At least Python 3.7.

#### Third-party Dependencies
Use `pip3 install` to install the python dependencies.
Type the `pip3 install` command with dependencies according to the environment you are running:
```
pip install -r requirements-aarch64.txt | requirements-x86.txt
```

#### Quick setup via setup_env.sh

To automatically install the Python dependencies and build the frontend inside the repository, run the `setup_env.sh` script (available for `x86_64` and `aarch64` platforms only):

```
chmod +x setup_env.sh
./setup_env.sh
```

The script performs the following steps:
- Downloads and installs Miniconda3 to `python/` in the repository root, then appends it to `PATH` for the current session;
- Downloads and extracts Node.js v16.9.0 to `node-v16.9.0-linux-<arch>/` under the project root;
- Installs the architecture-specific Python dependencies via `python/bin/python -m pip install -r requirements-<arch>.txt`;
- Enters the `ui/` directory to run `npm install` and `npm run build`, so the generated assets sync to the backend static directory automatically.

If you already have Python or Node.js installed globally, feel free to skip the relevant parts. Re-run the script whenever you need to refresh dependencies or rebuild the frontend.

#### Prometheus up and Running
Download the [Prometheus] time-series database (v2.42.0) from the official release page, pick the package matching your target CPU architecture, and extract it locally. The archive ships with a default `prometheus.yml`. Update `scrape_configs` to include the openGauss exporter, node exporter, or other targets you plan to collect. Start Prometheus inside the extracted directory with:
```
./prometheus --config.file=prometheus.yml --web.listen-address=0.0.0.0:9090
```
Here `--config.file` points to the configuration file and `--web.listen-address` exposes the UI and API endpoint. Run it in the foreground first to verify the scrape targets before moving it under a service manager.

#### Node Exporter
Download and run the [Prometheus node exporter] (v1.8.1). Deploy one instance on every Linux host (or container) you want to monitor. Select the archive by architecture, extract it, and then launch the exporter from the extracted directory:
```
./node_exporter --web.listen-address=0.0.0.0:9100
```
Add `<host>:9100` to Prometheus `scrape_configs`, and keep the process managed by `systemd`, `supervisor`, or a container entrypoint so it restarts automatically after host reboots.

### DBMind Components
The following DBMind components are required:

**Note: If you want to get higher security, you should use the HTTPS scheme.** 

#### openGauss Exporter
The openGauss-exporter reads data from the database and places it on the Prometheus time-series database.
OpenGauss-exporter is to monitor only one database instance. So if your deployment environment has not only one instance, you should start multiple openGauss-exporters to correspond to monitor multiple database instances.
It needs database access with a user having the role of at least **monadmin** (monitoring administrator) granted to run it. For example, you can grant monadmin privilege to role dbmind as below:
```
ALTER USER dbmind monadmin;
``` 
Use the following command with the parameters below:

```
gs_dbmind component opengauss_exporter ...
```
You can get detailed explanations of this component through passing `--help`:
```
gs_dbmind component opengauss_exporter --help
```

For example, the following command starts it:
```
gs_dbmind component opengauss_exporter --url postgresql://user:password@ip:port/dbname --web.listen-address 192.168.1.100 --ssl-keyfile server.key --ssl-certfile server.crt --ssl-ca-file server.crt
```
An example of opengauss-exporter multi-node deployment mode. The URL parameter contains multiple node addresses. The following example uses the default listening port number 9187, the listening address is 192.168.1.100, the URL parameter contains three node addresses, and the HTTPS protocol is used. The command can be:
```
gs_dbmind component opengauss_exporter --url postgresql://user:password@ip1:port1,ip2:port2,ip3:port3/dbname --web.listen-address 192.168.1.100 --ssl-keyfile server.key --ssl-certfile server.crt --ssl-ca-file server.crt
```

To test that the exporter is up, type the following command on its host (or use change the localhost to the server address):
```
curl -vv http://localhost:9187/metrics
```

#### Reprocessing Exporter
Reprocessing-exporter is a re-processing module for metrics stored in the Prometheus server. It helps Prometheus to reprocess the metric data then dump the new data into Prometheus. Therefore, only one needs to be started in a deployment environment.
To run it use the command below:
```
gs_dbmind component reprocessing_exporter ...
```
Users can see usage by using `--help` too.

See this example for running the exporter in a single machine development environment:
```
gs_dbmind component reprocessing_exporter 127.0.0.1 9090 --web.listen-address 0.0.0.0 --web.listen-port 9189
```
Use the following command to check that the service is up:
```
curl http://127.0.0.1:9189/metrics
```

#### Cmd Exporter
cmd_exporter is an exporter used to execute cmd commands and obtain returned results and collect log information. The database user should be used to start cmd_exporter on each instance node. For example, you can start cmd_exporter with the following command:

```
gs_dbmind component cmd_exporter --ssl-keyfile server.key --ssl-certfile server.crt --ssl-ca-file server.crt --pg-log-dir /path/to/pglog
```

Use the following command to check that the service is up:
```
curl -vv http://localhost:8181/metrics
```

#### High Available Interface
To ensure High Available when used on DBMind Cloud, the exporter component provides component status query and some exception repair interfaces.

API | parameter | param introduction | method | Description and returned results
----|------|---------|---------|----------------------
/v1/api/check-status | cmd | startup command，String，Required| POST | Get the status information of the exporter component and return the status details.
/v1/api/repair | cmd | startup command，String，Required| POST | Repair the exporter component and return the repair result.


### Configure, Start and Stop the DBMind Service 
DBMind service is a memory-resident backend service. Therefore, users should configure it first then start or stop the service by using the configuration.

Service usages:

    $ gs_dbmind service --help
    usage:  service [-h] -c DIRECTORY
                    [--only-run {discard_expired_results,anomaly_detection,cluster_diagnose,agent_update_detect,update_statistics,knob_recommend,slow_query_killer,slow_query_diagnosis,calibrate_security_metrics,check_security_metrics}]
                    [--dry-run] [-f] [--interactive | --initialize]
                    {setup,start,stop,restart,reload}
    
    positional arguments:
      {setup,start,stop,restart,reload}
                            perform an action for service
    
    optional arguments:
      -h, --help            show this help message and exit
      -c DIRECTORY, --conf DIRECTORY
                            set the directory of configuration files
      --only-run {discard_expired_results,anomaly_detection,cluster_diagnose,agent_update_detect,update_statistics,knob_recommend,slow_query_killer,slow_query_diagnosis,calibrate_security_metrics,check_security_metrics}
                            explicitly set a certain task running in the backend
      --dry-run             run the backend task(s) once. the task to run can be specified by the --only-run argument
      -f, --force           force to stop the process and cancel all in-progress tasks
      --interactive         configure and initialize with interactive mode
      --initialize          initialize and check configurations after configuring.




#### Configure
DBMind offers two methods to configure. The one is an interactive mode by using `--interactive` argument, the other is a modification by hands. 

See this example for configuring in the interactive mode:
```
gs_dbmind service setup -c CONF_DIRECTORY --interactive
```
Then users can type parameters into the shell terminal.

See the following example for configuring by hands:
```
gs_dbmind service setup -c CONF_DIRECTORY
```
After executing the above command, the directory `CONF_DIRECTORY` will generate too many configuration files. Therefore, users should modify these parameters in the `CONF_DIRECTORY/dbmind.conf`. While users finish configuring, this command needs to be run to initialize DBMind according to the `CONF_DIRECTORY/dbmind.conf`.
```
gs_dbmind service setup -c CONF_DIRECTORY --initialize
```

#### Start or Stop the DBMind Service
After configuring, specify your CONF_DIRECTORY, users can start or stop the service directly. 
```
gs_dbmind service start/stop -c CONF_DIRECTORY
```

#### Restart the DBMind Service
While DBMind is running, specify your CONF_DIRECTORY, users can restart the service directly. 
```
gs_dbmind service restart -c CONF_DIRECTORY
```

#### Reload the dynamic params
While DBMind is running, specify your CONF_DIRECTORY, users can reload the dynamic params. 
```
gs_dbmind service reload -c CONF_DIRECTORY
```

### Component 
If users want to use a specific component offline. They can use the sub-command `component`:
```
component usages:
    $ gs_dbmind component --help
    
    usage:  component [-h] COMPONENT_NAME ...
    
    positional arguments:
      COMPONENT_NAME  choice a component to start. ['anomaly_detection', 'cluster_diagnosis', 'cmd_exporter', 'dkr', 'extract_log', 'index_advisor', 'opengauss_exporter', 'reprocessing_exporter', 'slow_query_diagnosis',
                      'sql_rewriter', 'sqldiag', 'xtuner']
      ARGS            arguments for the component to start
    
    optional arguments:
      -h, --help      show this help message and exit
```
For example, the component `xtuner` can perform data parameter tuning. You can execute the following command to use the function of `xtuner`：
```
gs_dbmind component xtuner --help
```

# LICENSE
Mulan PSL v2
