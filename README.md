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

![DBMind架构图](docs/dbmind.png)

## 开始使用DBMind
### 下载并安装DBMind
DBMind基于Python语言实现，在使用DBMind时，需要运行环境具备Python虚拟机，同时安装好所需的第三方依赖。

#### 方式一：直接下载代码部署
DBMind主要使用Python语言进行编写，因此，可以在下载获取DBMind的源代码后，使用操作系统上安装的Python虚拟机直接运行，不过该过程中的第三方依赖需要用户手动安装。

用户可以通过 `git clone` 命令从Gitee或者Github上下载代码，例如：

```
git clone --depth 1 https://gitee.com/opengauss/openGauss-DBMind.git
```

也可以通过Gitee或者Github提供的zip包下载路径进行下载，而后解压缩该zip包即可。

下载DBMind后，会产生一个名为 `openGauss-DBMind` 的目录， 将该目录的路径添加到环境变量`PATH`中，即可调用该目录中的可执行文件。例如可以执行下述命令完成：
```
chmod +x openGauss-DBMind/gs_dbmind

echo PATH=`pwd`/openGauss-DBMind:'$PATH' >> ~/.bashrc
echo 'export PATH' >> ~/.bashrc

source ~/.bashrc
```

#### 方式二：使用安装包进行部署
DBMind会定期在openGauss-DBMind项目的release页面发布DBMind的安装包，可以通过下载该DBMind安装包进行安装部署。该安装包会自动将DBMind解压到指定目录，并配置好环境变量。

安装包的下载地址为：

https://gitee.com/opengauss/openGauss-DBMind/releases


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

### 使用DBMind
#### 部署Prometheus
可以通过 [Prometheus](https://prometheus.io/) 官方网站获取下载方式，下载并部署Prometheus，以便汇集对openGauss实例的监控结果。

#### 部署Node Exporter
下载并启动 [Prometheus node exporter](https://prometheus.io/download/#node_exporter). 

Node exporter 可以用于监控Linux系统，因此每个Linux环境（或容器内）只需要部署一个实例即可。

### 启动 DBMind 组件
如果希望将DBMind作为后台服务运行，则下面的DBMind组件是必须安装的，否则获取不到数据库的监控信息。为了获得更高的安全机制，DBMind提供的exporter默认是使用Https协议的，如果您觉得您的场景中不需要使用Https协议，则可以通过 `--disable-https` 选项禁用。

#### openGauss Exporter
openGauss exporter 从openGauss数据库中读取系统表（或系统视图）的数据，并通过Prometheus存储起来。由于openGauss exporter需要读取监控数据库的系统表信息，因此至少应该具备 **monadmin** 权限。例如，可以通过下述SQL语句为名为 `dbmind_monitor` 用户赋予权限：
```
ALTER USER dbmind_monitor monadmin;
```

使用 `gs_dbmind component opengauss_exporter ...` 命令即可启动该openGauss exporter组件。例如，可以通过下述命令监控某个数据库，通过 `--url` 参数指定被监控的数据库实例地址：
```
gs_dbmind component opengauss_exporter --url postgresql://username:password@host:port/database --web.listen-address 0.0.0.0 --web.listen-port 9187 --log.level warn --disable-https ...
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
gs_dbmind component reprocessing_exporter 127.0.0.1 9090 --web.listen-address 0.0.0.0 --web.listen-port 9189
```
如果您的Prometheus使用了`basic authorization`方式进行登录校验，则需要额外指定 `--prometheus-auth-user` 以及 `--prometheus-auth-password` 选项的值。

### 配置以及启动
DBMind后台服务是常驻内存的。因此，您需要首先配置一个配置文件目录，在该目录中保存多个DBMind的配置文件。可以通过 `gs_dbmind service` 命令来进行配置文件目录的生成以及服务的启动。该命令的使用说明为：

    $ gs_dbmind service --help
    usage:  service [-h] -c DIRECTORY [--only-run {...}] [--interactive | --initialize] {setup,start,stop}
    
    positional arguments:
      {setup,start,stop}    perform an action for service
    
    optional arguments:
      -h, --help            show this help message and exit
      -c DIRECTORY, --conf DIRECTORY
                            set the directory of configuration files
      --only-run {slow_query_diagnosis,forecast}
                            explicitly set a certain task running in the backend
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

### DBMind的组件 
如前文所述，DBMind基于一种插件化设计，这个组件（component）即为DBMind提供的插件（plugin）。通过插件式设计，DBMind可以任意进行功能扩展。如果想要使用某个组件的功能，则需要执行`component`子命令。例如某个名为`xtuner`的组件可以进行数据的参数调优，那么可以执行下述命令来使用`xtuner`的功能。

```
gs_dbmind component xtuner --help
```

### 使用Docker运行DBMind
DBMind支持Docker, 同时也会在Docker Hub上定期发布openGauss-DBMind的docker镜像，镜像的地址是：

https://hub.docker.com/r/dbmind/opengauss_dbmind

可以通过下述命令拉取该镜像：

```
docker pull dbmind/opengauss_dbmind
```

#### 创建Docker镜像
在某些情况下，您可能希望手动创建DBMind的docker镜像，例如想要创建基于最新代码的镜像时。那么，可以通过DBMind代码根目录下的 Dockerfile 文件创建。例如在DBMind的根目录中执行下述命令，即可创建名为 `opengauss_dbmind` 的镜像：

```
docker build -t opengauss_dbmind .
```

#### Docker 镜像的使用
DBMind的docker镜像的默认执行文件是 `docker_run.py`，该启动脚本可以在容器中启动DBMind所需的大多数依赖服务，包括Prometheus, openGauss exporter, reprocessing exporter. 但是，却无法在该镜像容器内运行node exporter来监控远端服务器上的信息。

用户可以通过下述环境变量，将需要监控的openGauss服务信息传递给DBMind的docker镜像：

    OPENGAUSS_DSNS: 需要监控的openGauss数据库实例的DSN信息，多个DSN信息用逗号(,)隔开
    NODE_EXPORTERS: openGauss数据库实例所在机器的node exporter地址，多个地址用逗号(,)隔开
    METADATABASE: 可选，将DBMind的离线计算结果存储起来的位置，用DSN形式标识数据库的连接信息；若为空，则默认使用SQLite进行存储
    SCRAPE_INTERVAL: 可选，指标信息的采集间隔，单位是秒；默认为15秒
    MASTER_USER: 可选，具有管理员权限的数据库用户名，可以用来执行某些数据库变更动作或者查询当前数据库的即时状态信息；若为空，则采用 OPENGAUSS_DSNS 中提供的用户
    MASTER_USER_PWD: 可选，上述 MASTER_USER 对应的用户密码
    
注：DSN的配置格式可以参考[常见问题](#dsn%E7%9A%84%E6%A0%BC%E5%BC%8F%E8%AF%B4%E6%98%8E)中的说明。

使用`docker run`的`-v`参数可以将路径进行映射，docker容器内的日志统一写到 `/log` 目录中，持久化的数据统一存放在 `/data` 目录中。使用 `-p` 参数可以将容器内的端口号进行映射，Prometheus的容器内端口是9090, DBMind的web服务则使用8080端口。下面是个启动docker服务的例子：
```
docker run -it \
    -e OPENGAUSS_DSNS="dbname=postgres user=dbmind_monitor password=DBMind@123 port=6789 host=192.168.1.100, dbname=postgres user=dbmind_monitor password=DBMind@123 port=6789 host=192.168.1.101, dbname=postgres user=dbmind_monitor password=DBMind@123 port=6789 host=192.168.1.102" \
    -e NODE_EXPORTERS="http://192.168.1.100:9100,http://192.168.1.101:9100,http://192.168.1.102:9100" \
    -e METADATABASE='postgresql://dbmind_metadb:DBMind%40123@192.168.1.100:6789/dbmind_metadb' \
    -e MASTER_USER='dbmind_sys' \
    -e MASTER_USER_PWD='DBMind@123' \
    -e SCRAPE_INTERVAL=30 \
    -p 38080:8080 -p 39090:9090 \
    -v `pwd`/data:/data -v `pwd`/log:/log \
    dbmind/opengauss_dbmind 
```

上面的例子是一主二备节点的部署形态，他们的IP地址分别是`192.16.1.100`、`192.16.1.101`以及`192.16.1.102`，数据库的端口号都是6789. 上面我们使用了三个用户，为了方便演示，它们的密码都设置为`DBMind@123`。其中`dbmind_monitor`负责从openGauss数据库中抓取指标监控，需要具备 `monitor admin`权限；`dbmind_sys` 至少需要具备 `monitor admin`权限，以便可以获取数据库的即时状态，如果具备`sysadmin`权限，则可以完成一些数据库变更动作，如慢SQL查杀；`dbmind_metadb` 只是负责数据保存，具备指定数据库的使用权限即可；同时，这里也进行了端口和目录的映射。

如果希望使用命令行的形式运行DBMind，则可以直接在该docker镜像内调用 `gs_dbmind` 命令即可，Python运行时和第三方依赖等都已经打包在docker镜像中了，无需再次安装。例如，希望使用DBMind的参数调优组件提供的功能，则可以执行下述命令：
```
docker run -it dbmind/opengauss_dbmind \
   gs_dbmind component xtuner recommend \
   --database tpcds \
   --db-host 192.168.1.100 \
   --host-user omm \
   --db-user tpcds \
   --db-port 16000
```

注：在使用`docker run` 命令运行 `gs_dbmind` 时，需要指定 `-it` 参数，以便创建一个tty.


## 常见问题
### DSN的格式说明
DSN是Database Source Name的缩写，这里支持两种格式，一种是K-V格式，如`dbname=postgres user=username password=password_value port=6789 host=127.0.0.1`；另一种是URL形式，例如`postgresql://username:password_value@127.0.0.1:6789/postgres`；对于采用URL格式的DSN，由于`@`等特殊字符用来分割URL串中各个部分的内容，故需要URL编码（URL encode）。例如某个用户`dbmind`的密码为`DBMind@123`，则URL形式的DSN可以是`postgresql://dbmind:DBMind%40123@127.0.0.1:6789`，即将`@`字符编码为`%40`. 类似地，需要编码的字符还包括其他可能引起歧义的字符，如`/`, `\`, `?`, `&`. 

## 相关资料
- [openGauss 在线手册](https://docs.opengauss.org/zh/docs/latest/docs/Developerguide/AI4DB-%E6%95%B0%E6%8D%AE%E5%BA%93%E8%87%AA%E6%B2%BB%E8%BF%90%E7%BB%B4.html)
- [DBMind wiki](https://gitee.com/opengauss/openGauss-DBMind/wikis)
- [openGauss AI-SIG](mailto:ai@opengauss.org)

---

# DBMind-Engish
DBMind is a part of openGauss, which empowers openGauss to carry the autonomous operations and maintenance capabilities. DBMind is leading and open-source. Through DBMind, users can easily discover database problems and the root causes of the problems in seconds.

![DBMind overview](docs/dbmind.png)

## Getting Started

### Prerequisites
In order to run DBMind, the following components should be configured and running.

#### Python Runtime
At least Python 3.7.

#### Third-party Dependencies
Use `pip install` to install the python dependencies.
Type the `pip install` command with dependencies according to the environment you are running:
```
pip install -r requirements-aarch64.txt | requirements-x86.txt
```

#### Prometheus up and Running
Download and run the [Prometheus](https://prometheus.io/) time-series database.

#### Node Exporter
Download and run the [Prometheus node exporter](https://prometheus.io/download/#node_exporter). Node-exporter is to monitor the Linux system. Hence, one Linux environment only needs to deploy one node-exporter.

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
gs_dbmind component opengauss_exporter --url postgresql://username:password@host:port/database --web.listen-address 0.0.0.0 --web.listen-port 9187 --log.level warn --disable-https ...
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

### Configure, Start and Stop the DBMind Service 
DBMind service is a memory-resident backend service. Therefore, users should configure it first then start or stop the service by using the configuration.

Service usages:

    $ gs_dbmind service --help
    usage:  service [-h] -c DIRECTORY [--only-run {...}] [--interactive | --initialize] {setup,start,stop}
    
    positional arguments:
      {setup,start,stop}    perform an action for service
    
    optional arguments:
      -h, --help            show this help message and exit
      -c DIRECTORY, --conf DIRECTORY
                            set the directory of configuration files
      --only-run {slow_query_diagnosis,forecast}
                            explicitly set a certain task running in the backend
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

### Component 
If users want to use a specific component offline. They can use the sub-command `component`:
```
gs_dbmind component xxx ...
```

`xxx` is the name of a component. Users can also get the component list by using the `--help` argument.

For example, use the following component to tune the knobs of a database:
```
gs_dbmind component xtuner --help
```

# LICENSE
Mulan PSL v2

# Reference
1. https://en.wikipedia.org/wiki/Percent-encoding
2. https://dba.stackexchange.com/questions/243219/in-postgresql-url-i-cant-use-a-password-containing-special-characters
