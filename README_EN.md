# DBMind
[Chinese](#dbmind-Chinese) | [English](#dbmind-English)

Maintainer: [openGauss AI-SIG](mailto:ai@opengauss.org)


# DBMind
As part of openGauss, DBMind is a leading open-source database autonomous O&M platform that provides self-driving capabilities for openGauss. With DBMind, you can easily detect database problems and perform root cause analysis in seconds.

DBMind features:
- DBMind uses an advanced plug-in architecture and supports massive plug-in extensions.
- It supports multiple running modes, including interactive CLI and service-based modes.
- Designed for cloud-native environments, it supports Prometheus and provides a variety of exporter plug-ins.
- It provides flexible interconnection modes, such as RESTful API, Python SDK, CLI, and Prometheus, to easily integrate with existing management systems.
- It supports end-to-end database autonomous O&M capabilities, including slow SQL root cause analysis, workload index recommendation, multi-metric association mining, automatic fault recovery, and anomaly detection.

DBMind supports the following capabilities:
- Index recommendation
- Anomaly detection and analysis
- Multi-metric association analysis
- Slow SQL root cause analysis
- Time series forecasting
- Parameter tuning and recommendation
- SQL rewriting and optimization
- Automatic fault recovery


![DBMind architecture](docs/dbmind.png)

## Getting Started with DBMind
### Downloading and Installing DBMind
DBMind is implemented in Python. To use DBMind, ensure your environment has a Python interpreter and all required third-party dependencies installed.

#### Method 1: Deployment from Source Code
DBMind is primarily written in Python. You can download the DBMind source code and run it directly using the Python interpreter installed on your OS. However, you must manually install the third-party dependencies.

You can use the `git clone` command to download the code from Gitee or GitHub. For example:

```
git clone --depth 1 https://gitee.com/opengauss/openGauss-DBMind.git
```

Alternatively, you can download the .zip package from Gitee or GitHub and decompress it.

After downloading, a directory named `openGauss-DBMind` will be created. Add this directory to your `PATH` environment variable to invoke the executable files. For example:
```
chmod +x openGauss-DBMind/gs_dbmind

echo PATH=`pwd`/openGauss-DBMind:'$PATH' >> ~/.bashrc
echo 'export PATH' >> ~/.bashrc

source ~/.bashrc
```

#### Method 2: Deployment via Installation Package
DBMind installation packages are periodically released on the openGauss-DBMind release page. You can download these packages for deployment. The installer automatically decompresses DBMind to a specified directory and configures the necessary environment variables.

Download links for installation packages and checksums:

| Name              | Download                                                                                                                                                                        | Remarks                 |
|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| DBMind X86        | [dbmind-installer-x86_64.tar.gz](https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/dbmind/x86/dbmind-installer-x86_64.tar.gz)               | DBMind package for x86 architecture        |
| DBMind X86 SHA256 | [dbmind-installer-x86_64.tar.gz.sha256](https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/dbmind/x86/dbmind-installer-x86_64.tar.gz.sha256) | SHA256 checksum for x86 package|
| DBMind ARM        | [dbmind-installer-aarch64.tar.gz](https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/dbmind/arm/dbmind-installer-aarch64.tar.gz)               | DBMind package for Arm architecture        |
| DBMind ARM SHA256 | [dbmind-installer-aarch64.tar.gz.sha256](https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/dbmind/arm/dbmind-installer-aarch64.tar.gz.sha256) | SHA256 checksum for Arm package|

Integrity check:

To ensure the software package is not corrupted during transmission, for example, due to network fluctuations or storage media issues, you must verify its integrity. Only verified packages can be deployed. Follow the steps below to proceed:

1. Calculate the SHA256 value of the downloaded package (using `dbmind-installer-aarch64.tar.gz` as an example. The operations for other versions are the same.)

~~~
 sha256sum dbmind-installer-aarch64.tar.gz
~~~

2. Download the corresponding SHA256 file and compare the values. If they match, the package is complete; otherwise, download it again.

Using the installation package:

Decompress: `tar zxvf dbmind-installer-x86_64.tar.gz`

Install: `sh dbmind-installer-x86_64-python3.12.sh`


#### Python Runtime Environment
Python 3.7 or later is required. While DBMind attempts to maintain compatibility with versions below 3.7, these are not extensively tested and may cause unexpected exceptions. Upon startup, DBMind verifies the Python version. If it does not meet the requirements, execution will stop by default.

*Note: Python version constraints are defined by variables in the `constant` file within the `root` directory.*

If your environment requires multiple Python versions that may conflict, we recommend installing the required Python environment into the `python` directory within the DBMind root. DBMind prioritizes this local directory. Specifically, the `gs_dbmind` command will first look for the `python3` executable in `python/bin`.


#### Third-party Dependencies
Dependencies are specified in `requirements-xxx.txt` files in the root directory. Different files are used for x86 (amd64) and Arm (AArch64) architectures because some dependencies on Arm platforms require specific versions for compatibility.

Use `pip` to install these dependencies. If you are managing multiple Python environments, DBMind also supports prioritizing local dependencies stored in a `3rd` directory in the root. When using `gs_dbmind`, libraries in the `3rd` directory will be loaded preferentially.

Example for x86 environments:

```
python3 -m pip install -r requirements-x86.txt
```

To install directories to which third-party dependencies are downloaded, use the `--target` or `-t` flag:
```
python3 -m pip install -r requirements-x86.txt -t 3rd
```

### Using DBMind
#### Deploying Prometheus
Download and deploy [Prometheus](https://prometheus.io/) from the official website to aggregate monitoring results from openGauss instances.

#### Deploying Node Exporter
Download and start [Prometheus node exporter](https://prometheus.io/download/#node_exporter).

Node Exporter is used to monitor Linux systems; therefore, only one instance needs to be deployed per Linux environment (or container).

### Starting DBMind Components
To run DBMind as a background service, the following components must be installed to collect database monitoring metrics. For enhanced security, DBMind exporters use HTTPS by default. If your environment does not require HTTPS, you can disable it using the `--disable-https` option.

#### openGauss Exporter
The openGauss Exporter reads data from system catalogs (or system views) and exports it to Prometheus. Because it requires access to system catalogs, it must have at least `monadmin` permissions. For example, grant permissions to the `dbmind_monitor` user using the following SQL statement:
```
ALTER USER dbmind_monitor monadmin;
```

Start the exporter using the `gs_dbmind component opengauss_exporter` command. Use the `--url` parameter to specify the database instance to be monitored, for example:
```
gs_dbmind component opengauss_exporter --url postgresql://username:password@host:port/database --web.listen-address 0.0.0.0 --web.listen-port 9187 --log.level warn --disable-https ...
```

The `--url` represents the database DSN. For format details, see [DSN Format Description](#dsn-format-description).

Run the following command to verify that the openGauss exporter is running:
```
curl -vv http://localhost:9187/metrics
```

#### Reprocessing Exporter
The Reprocessing Exporter performs secondary data processing. Node Exporter and openGauss Exporter provide real-time snapshots. They cannot directly represent instantaneous increments for metrics like TPS or IOPS. The Reprocessing Exporter calculates these incremental values or aggregated results.

It retrieves metrics from Prometheus, processes them, and serves them back to Prometheus. There is a one-to-one mapping between this exporter and your Prometheus service. If you have one Prometheus instance, you only need one Reprocessing Exporter. For example, you can start the Reprocessing Exporter by running the following command:
```
gs_dbmind component reprocessing_exporter 127.0.0.1 9090 --web.listen-address 0.0.0.0 --web.listen-port 9189
```
If your Prometheus instance uses `basic authorization`, you must provide the `--prometheus-auth-user` and `--prometheus-auth-password` options.

### Configuration and Startup
The DBMind background service is memory-resident. Therefore, you must first configure a directory to store multiple DBMind configuration files. You can use the `gs_dbmind service` command to generate this directory and manage the service. The command usage is as follows:

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

The following sections describe how to generate the configuration directory and perform service start/stop operations.
#### Configuring DBMind
DBMind provides two methods to generate configuration files: an interactive mode via the `--interactive option`, and the default manual mode.

**Interactive configuration**

The following is an example, using `CONF_DIRECTORY` to represent the configuration directory:
```
gs_dbmind service setup -c CONF_DIRECTORY --interactive
```
By running this command, you can enter the connection details and parameters for the openGauss instances you wish to monitor via the interactive prompts.


**Manual configuration**

The following command demonstrates how to set up DBMind manually:
```
gs_dbmind service setup -c CONF_DIRECTORY
```
Executing this command generates a directory named `CONF_DIRECTORY` containing several configuration files. You generally only need to modify the `CONF_DIRECTORY/dbmind.conf` file. Once configured, run the following command to initialize the DBMind system based on your settings:
```
gs_dbmind service setup -c CONF_DIRECTORY --initialize
```

#### Starting and Stopping the DBMind Service
Once the configuration is complete, you can run the following command to start the DBMind background service:
```
gs_dbmind service start -c CONF_DIRECTORY
```
Run the following command to stop the DBMind service:
```
gs_dbmind service stop -c CONF_DIRECTORY
```

### DBMind Components
As mentioned previously, DBMind is built on a plug-in architecture where a "component" represents a DBMind plug-in. This design allows for flexible functional expansion. To use a specific component's features, use the `component` subcommand. For example, to use the `xtuner` component for parameter tuning, run the following command:

```
gs_dbmind component xtuner --help
```

### Running DBMind Using Docker
DBMind supports Docker and periodically releases images for openGauss-DBMind on Docker Hub. The image repository is:

https://hub.docker.com/r/dbmind/opengauss_dbmind

You can run the following command to pull the image:

```
docker pull dbmind/opengauss_dbmind
```

#### Creating a Docker Image
In some cases, you may want to manually build the DBMind Docker image, for example, to include the latest code changes. You can build it using the `Dockerfile` in the root directory. For example, run the following command from the root directory to create an image named `opengauss_dbmind`:

```
docker build -t opengauss_dbmind .
```

#### Using Docker Images
The default executable file for the DBMind Docker image is `docker_run.py`. This startup script launches most dependency services required by DBMind within the container, including Prometheus, openGauss Exporter, and Reprocessing Exporter. However, note that the Node Exporter cannot be run inside this container to monitor remote servers.

Use the following environment variables to pass openGauss instance information to the container:

    OPENGAUSS_DSNS: DSN information of the openGauss instances to be monitored. Separate multiple DSNs by commas (,).
    NODE_EXPORTERS: Addresses of Node Exporters on the hosts where the openGauss instances reside. Separate multiple addresses by commas (,).
    METADATABASE: (Optional) Location to store DBMind's offline computation results, identified by DSN. If empty, SQLite is used by default.
    SCRAPE_INTERVAL: (Optional) Metric collection interval in seconds. Default: 15 seconds.
    MASTER_USER: (Optional) Database user with administrator privileges, used for database changes or querying real-time status. If empty, the user from OPENGAUSS_DSNS is used.
    MASTER_USER_PWD: (Optional) Password for the MASTER_USER.

Note: For details about the DSN configuration format, see the description in [FAQs](#faqs).

Use the `-v` parameter of `docker run` for path mapping. Docker container logs are written to `/log` and persistent data is stored in `/data`. Use the `-p` parameter for port mapping. Internally, Prometheus uses port 9090 and the DBMind web service uses port 8080. The following is an example of starting the Docker service:
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

The example above depicts a deployment with one primary and two standby nodes (IP addresses: `192.168.1.100`, `192.168.1.101`, and `192.168.1.102`) using port 6789. Three users are defined with the password `DBMind@123`: `dbmind_monitor` captures metrics, requiring `monitor admin` permissions. `dbmind_sys` queries real-time status, requiring `monitor admin` permissions, and performs changes such as killing slow SQL, requiring `sysadmin` permissions. `dbmind_metadb` saves data, requiring usage permissions on the specified database. In addition, ports and directories are mapped.

To use the DBMind CLI within Docker, invoke the `gs_dbmind` command directly inside the image. All Python runtimes and dependencies are pre-installed in the Docker image. For example, to use the parameter tuning component, run the following command:
```
docker run -it dbmind/opengauss_dbmind \
   gs_dbmind component xtuner recommend \
   --database tpcds \
   --db-host 192.168.1.100 \
   --host-user omm \
   --db-user tpcds \
   --db-port 16000
```

Note: When running `gs_dbmind` via `docker run`, always include the `-it` flag to allocate a TTY.


## FAQs
### DSN Format Description
DSN stands for Database Source Name. Two formats are supported: key-value (K-V) and URL. For example, `dbname=postgres user=username password=password_value port=6789 host=127.0.0.1` is a DSN in K-V format, and `postgresql://username:password_value@127.0.0.1:6789/postgres` is a DSN in URL format. In the URL format, since special characters such as the at sign (@) are used to delimit different parts of the URL string, they must be URL-encoded. For example, if the password for the user `dbmind` is `DBMind@123`, the URL-formatted DSN would be `postgresql://dbmind:DBMind%40123@127.0.0.1:6789`, where the `@` character is encoded as `%40`. Similarly, other characters that may cause ambiguity, such as slashes (/), backslashes (\), question marks (?), and ampersands (&), also require encoding.

## References
- [openGauss Documentation](https://docs.opengauss.org/en/)
- [DBMind wiki](https://gitee.com/opengauss/openGauss-DBMind/wikis)
- [openGauss AI-SIG](mailto:ai@opengauss.org)

---
