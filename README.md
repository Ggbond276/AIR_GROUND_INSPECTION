# 空地协同巡检平台

> **Air-Ground Inspection Platform** —— 无人机与机器狗协同工作的演示级全栈 IoT 数据管道，适用于工业 / 园区 / 巡线场景。

![status](https://img.shields.io/badge/status-active--development-yellow)
![jdk](https://img.shields.io/badge/JDK-17-blue)
![spring-boot](https://img.shields.io/badge/Spring%20Boot-3.2.5-brightgreen)
![vue](https://img.shields.io/badge/Vue-3-42b883)
![kafka](https://img.shields.io/badge/Kafka-3.5-black)
![mongo](https://img.shields.io/badge/MongoDB-7-success)
![elasticsearch](https://img.shields.io/badge/Elasticsearch-8.13.4-005571)
![docker](https://img.shields.io/badge/Docker-Compose-2496ed)
![platform](https://img.shields.io/badge/platform-Windows%20%7C%20macOS-lightgrey)

---

## 目录

1. [项目概述](#1-项目概述)
2. [技术栈](#2-技术栈)
3. [架构](#3-架构)
4. [前置环境与软件准备（Windows + macOS）](#4-前置环境与软件准备windows--macos)
5. [本地部署步骤](#5-本地部署步骤)
6. [验证清单](#6-验证清单)
7. [已知问题与故障排查](#7-已知问题与故障排查)
8. [项目结构](#8-项目结构)
9. [常用命令速查表](#9-常用命令速查表)

---

## 1. 项目概述

**空地协同巡检平台** 模拟一组 **无人机（DRONE）** 和 **机器狗（ROBOTDOG）** 协同执行工业 / 巡线 / 园区巡检任务的场景。每个设备会周期性向中心后端上报遥测数据（位置、电量、状态等），后端负责：

- **持久化**：将每一条上报数据写入 **MongoDB**，作为设备最新状态的权威来源。
- **检索**：将关键 / 告警事件索引到 **Elasticsearch**，提供全文检索能力。
- **消息流**：通过 **Kafka** 实时转发事件，GM（Ground-station Manager，地面管控）控制台可订阅而无需轮询。
- **任务下发**：接收来自 GM 控制台的任务（如 "无人机飞往 X,Y" / "机器狗返航"），通过 Kafka 下发给设备。
- **(实验性)** 通过 **Hadoop HDFS** 存储巡检图片。

### 仓库的三个组件

| 模块 | 路径 | 职责 |
|---|---|---|
| `inspection-server` | Spring Boot 3 后端 | REST API + Kafka 消费者/生产者 + MongoDB + Elasticsearch + HDFS |
| `device-simulator` | 独立 Java 模拟器 | 模拟 DRONE / ROBOTDOG，向 Kafka 发送伪造遥测数据 |
| `docker-compose.yml` | 根目录 | 本地基础设施：Mongo / ZK / Kafka / ES / HDFS |

> **目标**：一个自包含、单宿主机、Docker 友好的 IoT 数据管道演示，**约 10 分钟内可完成端到端跑通**。

---

## 2. 技术栈

| 层级 | 技术 | 版本 |
|---|---|---|
| 语言 | Java | **17**（Spring Boot 3.x 强制要求） |
| 构建 | Maven | **3.8+** |
| 后端框架 | Spring Boot | **3.2.5** |
| Web 层 | Spring MVC + 内嵌 Tomcat | （随 Boot 提供） |
| 消息队列 | Apache Kafka（Confluent Platform 镜像） | **7.5.0**（broker 3.5+） |
| 协调服务 | Apache ZooKeeper | **7.5.0** |
| 文档数据库 | MongoDB | **7** |
| 搜索引擎 | Elasticsearch | **8.13.4**（单节点，禁用安全特性） |
| 文件系统 | Hadoop HDFS（单节点，bde2020 镜像） | Hadoop **3.2.1**（服务端）+ hadoop-client **3.3.6**（Java SDK） |
| 前端 | Vue 3 + Element Plus（CDN）+ axios | — |
| 容器化 | Docker + Docker Compose | Docker **24+** / Compose **v2** |

> 前端是放在 `inspection-server/src/main/resources/static/index.html` 下的 **静态 HTML**，由 Spring Boot 内嵌 Tomcat 直接对外提供，**没有独立的前端构建步骤**。

---

## 3. 架构

```
                        +-------------------+
                        |   无人机 / 机器狗 |
                        | (device-simulator) |
                        +---------+---------+
                                  │  Kafka publish
                                  │  topic: device-telemetry
                                  ▼
                          +---------------+
                          |   Kafka 9092  |
                          +-------+-------+
                                  │
                +-----------------+-----------------+
                ▼                                   ▼
        +---------------+                +------------------+
        | MongoDB 27017 |                | Elasticsearch    |
        | (DeviceData)  |                | 9200 (Alerts)    |
        +-------+-------+                +---------+--------+
                ▲                                   ▲
                │                                   │
                │       +-------------------+       │
                └───────┤ inspection-server ├───────┘
                        |   Spring Boot 3   |
                        |   :8080           |
                        +---------+---------+
                                  ▲  HTTP (GM 控制台)
                                  │
                        +---------+---------+
                        |    浏览器          |
                        |  Vue 3 + ElPlus   |
                        +-------------------+
```

### 核心 Topic 与接口

| Topic / 接口 | 方向 | 用途 |
|---|---|---|
| Kafka topic `device-telemetry` | 模拟器 → 后端 | 设备状态上报 |
| Kafka topic `device-alerts` | 模拟器 → 后端 | 告警事件（低电量等） |
| Kafka topic `device-tasks` | 后端 → 模拟器（或未来的真实设备） | GM 下发的任务指令 |
| `GET /api/devices` | 后端 → 浏览器 | 列出所有设备的最新状态 |
| `GET /api/alerts/search?keyword=...` | 后端 → 浏览器 | 通过 ES 做告警全文检索 |
| `POST /api/tasks/dispatch` | 浏览器 → 后端 | GM 任务下发 |
| `POST /api/images/upload` | 浏览器 → 后端 → HDFS | 图片上传（实验性） |

---

## 4. 前置环境与软件准备（Windows + macOS）

> ⚠️ **Maven 3.8+ 和 JDK 17 是硬性要求**。Spring Boot 3.x **无法**在 JDK 8 / 11 上构建。开始前请务必确认。

### 4.1 JDK 17

#### macOS（Homebrew）

```bash
brew install openjdk@17

# 写入 ~/.zshrc（macOS Catalina 之后默认 shell 是 zsh）
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 17)' >> ~/.zshrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

> 旧版 macOS 默认 shell 是 bash，把 `~/.zshrc` 换成 `~/.bash_profile` 即可。

验证：
```bash
java -version
# openjdk version "17.0.x" ...

javac -version
# javac 17.0.x

echo $JAVA_HOME
# /Library/Java/JavaVirtualMachines/openjdk-17/...
```

#### Windows（手动安装 + 系统环境变量）

**步骤 1：下载并安装 JDK 17**

1. 打开 [Adoptium Temurin 17 下载页](https://adoptium.net/temurin/releases/?version=17)，选 **Windows ×64 MSI** 下载。
2. 双击 `.msi` 安装，默认会装到 `C:\Program Files\Eclipse Adoptium\jdk-17.x.x.x.x-hotspot\`。
3. 安装向导里 **"Set JAVA_HOME variable"** 和 **"Add to PATH"** 两个选项**务必勾选**。

> 如果你装的是 Oracle JDK 或其他发行版，安装路径通常为 `C:\Program Files\Java\jdk-17\`。

**步骤 2：手动设置系统环境变量（如果安装时没勾选）**

1. 按 `Win + S`，输入 **"编辑系统环境变量"**（或 "Environment Variables"），回车。
2. 在 **系统属性** → **高级** → **环境变量** 中：

   - **新建系统变量**：
     - 变量名：`JAVA_HOME`
     - 变量值：`C:\Program Files\Eclipse Adoptium\jdk-17.x.x.x.x-hotspot\`
       （按你实际安装路径填写）
   - **编辑系统变量 `Path`** → **新建** → 添加：
     ```
     %JAVA_HOME%\bin
     ```

3. 一路点 **确定** 保存。

**步骤 3：让新环境变量生效**

- **重新打开** 一个新的 PowerShell / CMD 窗口（旧的窗口不会自动加载新环境变量）。
- 或者在当前窗口执行 `refreshenv`（如果装了 Chocolatey）。

**验证**（PowerShell 或 CMD）：

```powershell
java -version
# openjdk version "17.0.x" ...

javac -version
# javac 17.0.x

echo %JAVA_HOME%
# C:\Program Files\Eclipse Adoptium\jdk-17.x.x.x.x-hotspot\
```

> 如果 IDE（IntelliJ / Eclipse / VS Code）出现红线报错，**请同时把 IDE 的项目 SDK 也设为 17**：
> - IntelliJ：`File → Project Structure → Project → SDK`
> - VS Code：`Ctrl+Shift+P` → `Java: Configure Java Runtime` → 选 JDK 17

---

### 4.2 Maven 3.8+

#### macOS

```bash
brew install maven
mvn -v
# Apache Maven 3.9.x ...
```

#### Windows（Chocolatey）

```powershell
choco install maven -y
mvn -v
# Apache Maven 3.9.x ...
```

#### Windows（手动安装）

1. 在 [Apache Maven 官网](https://maven.apache.org/download.cgi) 下载 `apache-maven-x.y.z-bin.zip`。
2. 解压到 `C:\Program Files\Apache\maven\`（路径**不要有空格和中文**）。
3. **新建系统变量**：
   - 变量名：`MAVEN_HOME`
   - 变量值：`C:\Program Files\Apache\maven\apache-maven-x.y.z`
4. **编辑系统变量 `Path`** → 新建 → 添加：
   ```
   %MAVEN_HOME%\bin
   ```
5. **新开一个 PowerShell / CMD 窗口**，执行 `mvn -v` 验证。

> 如果全局 Maven 版本较旧，也可以使用项目自带的 **Maven Wrapper**（`./mvnw` 或 `mvnw.cmd`），无需全局安装。

---

### 4.3 Docker Desktop

| 操作系统 | 安装方式 |
|---|---|
| macOS | [Docker Desktop for Mac](https://www.docker.com/products/docker-desktop/)（≥ 4.x） |
| Windows | [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/)（≥ 4.x） |

#### macOS 安装

1. 下载 `.dmg`，拖入 Applications。
2. 启动 Docker Desktop，等待 **Docker Desktop is running** 出现在菜单栏。
3. 首次启动会请求 **特权访问权限**，输入系统密码授权。

#### Windows 安装（**强烈建议走 WSL 2 后端**）

1. **启用 WSL**（管理员 PowerShell）：
   ```powershell
   dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
   dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
   ```
2. **重启电脑**。
3. **设置 WSL 2 为默认**：
   ```powershell
   wsl --set-default-version 2
   ```
4. **安装 Ubuntu**（Microsoft Store 搜 "Ubuntu" 装一个即可）。
5. **下载并安装** [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/)。
6. 启动 Docker Desktop → **Settings → General**，确认勾选 **"Use the WSL 2 based engine"**。
7. **Settings → Resources → WSL Integration**，**启用** 你安装的 Ubuntu 发行版。
8. 点击 **"Apply & Restart"**。

> ⚠️ **不要** 在 Settings 里把 Docker 引擎切换到 Hyper-V / Windows 容器模式，否则 Linux 镜像（Kafka / ES / Mongo）无法运行。

#### 验证（两个系统通用）

```bash
# macOS / Linux（bash / zsh）
docker --version        # Docker version 24.x 或更高
docker compose version  # Docker Compose version v2.x（**不是**旧的 `docker-compose`）

# Windows PowerShell
docker --version
docker compose version
```

> ⚠️ 本仓库的 `docker-compose.yml` 使用的是 **Compose v2** 语法（`docker compose ...`，没有短横线）。如果只有旧的 v1 命令行，要么升级 Docker Desktop，要么安装 Compose v2。

---

### 4.4 资源需求

| 资源 | 最低 | 推荐 |
|---|---|---|
| CPU | 4 核 | 8 核 |
| 内存 | 8 GB 可用 | 16 GB 可用 |
| 磁盘 | 10 GB 可用 | 20 GB 可用 |
| 网络 | 互联网（首次拉镜像） | — |

> Docker 栈会启动 **6 个容器**（mongo、zookeeper、kafka、elasticsearch、namenode、datanode）。在 4 核 / 8GB 的机器上能跑，但 ES 首次启动会偏慢。
>
> **Windows 用户注意**：WSL 2 默认只占用 50% 物理内存和部分 CPU，建议到 Docker Desktop → **Settings → Resources → Advanced** 里把内存调到 **6 GB 以上**。

### 4.5（可选）IDE 设置

| IDE | 推荐插件 |
|---|---|
| IntelliJ IDEA Ultimate / Community | Lombok、Spring Boot Assistant、Docker、Vue.js |
| VS Code | Extension Pack for Java、Spring Boot Extension Pack、Vue (Official)、Docker |

---

## 5. 本地部署步骤

> **TL;DR —— 5 条命令搞定**：
>
> **macOS / Linux（bash / zsh）**
> ```bash
> git clone <仓库地址> air-ground-inspection && cd air-ground-inspection
> docker compose up -d
> (cd inspection-server && mvn spring-boot:run)        # 终端 2
> (cd device-simulator && java -jar target/device-simulator-0.0.1-SNAPSHOT.jar)  # 终端 3
> open http://localhost:8080                            # 浏览器
> ```
>
> **Windows（PowerShell）**
> ```powershell
> git clone <仓库地址> air-ground-inspection
> cd air-ground-inspection
> docker compose up -d
> # 新开一个 PowerShell 窗口
> cd inspection-server; mvn spring-boot:run
> # 再开一个 PowerShell 窗口
> cd device-simulator; mvn clean package -DskipTests; java -jar target/device-simulator-0.0.1-SNAPSHOT.jar
> start http://localhost:8080
> ```

### 第 1 步 — 克隆仓库

#### macOS / Linux

```bash
git clone <你的 git 远程地址> air-ground-inspection
cd air-ground-inspection
```

#### Windows（PowerShell 或 Git Bash）

```powershell
git clone <你的 git 远程地址> air-ground-inspection
cd air-ground-inspection
```

> 推荐安装 [Git for Windows](https://git-scm.com/download/win)，提供 Git Bash；或者直接用 **PowerShell 7**（跨平台，自带 tab 补全，体验更好）。

期望的目录结构：

```
air-ground-inspection/
├── README.md                     ← 你正在读的文件
├── docker-compose.yml            ← 基础设施栈
├── .gitignore
├── inspection-server/            ← Spring Boot 后端
│   ├── pom.xml
│   └── src/main/...
└── device-simulator/             ← 独立模拟器
    ├── pom.xml
    └── src/main/...
```

如果想要浅克隆或指定 release：

```bash
# macOS / Linux
git clone --depth 1 --branch main <url>

# Windows（PowerShell）
git clone --depth 1 --branch main <url>
```

### 第 2 步 — 使用 Docker Compose 启动基础设施

#### macOS / Linux

```bash
docker compose up -d
```

#### Windows（PowerShell）

```powershell
docker compose up -d
```

> Windows 上首次启动 Docker Desktop 时**务必等状态栏显示 "Docker Desktop is running"** 再执行这条命令，否则会报 `Cannot connect to the Docker daemon`。

这一步会拉取并启动 **6 个容器**：

| 容器 | 镜像 | 端口 | 用途 |
|---|---|---|---|
| `inspection-mongodb` | `mongo:7` | 27017 | 设备数据持久化 |
| `inspection-zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | 2181 | Kafka 协调器 |
| `inspection-kafka` | `confluentinc/cp-kafka:7.5.0` | 9092, 29092 | 消息代理 |
| `inspection-elasticsearch` | `docker.elastic.co/elasticsearch/elasticsearch:8.13.4` | 9200 | 告警全文检索 |
| `inspection-hdfs-namenode` | `bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8` | 9000, 9870 | HDFS NameNode |
| `inspection-hdfs-datanode` | `bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8` | 9864 | HDFS DataNode |

> ⏱ **首次拉镜像需要 5–15 分钟**（Kafka + ES + HDFS 镜像合计约 2.5 GB），取决于网络带宽。

#### 验证所有服务健康

#### macOS / Linux
```bash
docker compose ps
```

#### Windows（PowerShell）
```powershell
docker compose ps
```

期望：所有服务都显示 `running (...)` 和 `(healthy)`（健康检查通过后）。

#### 单独验证每个组件

```bash
# MongoDB（两平台通用）
docker exec -it inspection-mongodb mongosh --quiet --eval 'db.runCommand({ ping: 1 }).ok'
# → 1

# Kafka（两平台通用）
docker exec -it inspection-kafka kafka-topics --bootstrap-server localhost:9092 --list
# → （空列表也正常，topic 会按需自动创建）

# Elasticsearch（两平台通用）
curl http://localhost:9200 | head -20
# → "cluster_name" : "inspection-cluster" ...

# Windows 专属：PowerShell 中测试 Elasticsearch
# (Invoke-WebRequest http://localhost:9200).Content
```

#### 查看日志（如果有服务不健康）

```bash
docker compose logs -f kafka
docker compose logs -f elasticsearch
docker compose logs -f namenode
```

> 💡 **提示**：用 `docker compose down -v` 停止并彻底清理数据卷。`-v` 很关键 —— 没有它的话，旧的 ES 索引 / Kafka topic 会保留下来，容易引起混淆。

### 第 3 步 — 编译并运行后端（`inspection-server`）

打开 **新的终端**（保持 docker compose 在跑）。

#### macOS / Linux（终端 2）

```bash
cd inspection-server

# 首次：构建 jar（会下载所有 Maven 依赖，约 3-5 分钟）
mvn clean package -DskipTests

# 运行
mvn spring-boot:run
```

#### Windows（PowerShell 窗口 2）

```powershell
cd inspection-server

# 首次：构建 jar（会下载所有 Maven 依赖，约 5-10 分钟）
mvn clean package -DskipTests

# 运行
mvn spring-boot:run
```

> ⚠️ **Windows 上 Maven 首次运行**可能比 macOS 慢 30%~50%（Windows 文件 I/O 较慢），请耐心等待 `BUILD SUCCESS` 后再执行 `mvn spring-boot:run`。
>
> 如果路径包含**中文或空格**（比如 `C:\Users\张三\Desktop\...`），可能导致部分插件异常 —— 建议把项目放在 **纯英文、无空格** 的路径下，例如 `C:\projects\air-ground-inspection`。

> 首次运行会下载 **约 500 MB** 的 Maven 制品（Spring Boot starter、hadoop-client、ES 客户端等）。后续运行会复用本地仓库缓存，快很多。

#### 验证后端已起来

应该能看到类似：
```
  .   ____          _            __ _ _
 /\\ / ___'_ __ _ _(_)_ __  __ _ \ \ \ \
( ( )\___ | '_ | '_| | '_ \/ _` | \ \ \ \
 \\/  ___)| |_)| | | | | || (_| |  ) ) ) )
  '  |____| .__|_| |_|_| |_\__, | / / / /
 =========|_|==============|___/=/_/_/_/
 :: Spring Boot ::                (v3.2.5)

... Tomcat started on port 8080 ...
... Started InspectionServerApplication in 7.4 seconds ...
```

#### 接口冒烟测试

#### macOS / Linux
```bash
curl -s http://localhost:8080/api/health
# → {"status":"UP"}
```

#### Windows（PowerShell）
```powershell
(Invoke-WebRequest http://localhost:8080/api/health).Content
# → {"status":"UP"}

# 或直接浏览器打开 http://localhost:8080/api/health
```

> 如果启动时出现 `StaticLoggerBinder` / `ClassCastException` / `NoSuchMethodError: LocationAwareLogger.log`，说明 **Hadoop SLF4J binding 冲突**又回来了。请确认 `pom.xml` 已经排除了 `slf4j-reload4j`、`reload4j`、`slf4j-log4j12`、`log4j`、`commons-logging`。

### 第 4 步 — 编译并运行设备模拟器

打开 **第三个终端**。

#### macOS / Linux（终端 3）

```bash
cd device-simulator

mvn clean package -DskipTests

java -jar target/device-simulator-0.0.1-SNAPSHOT.jar
```

#### Windows（PowerShell 窗口 3）

```powershell
cd device-simulator

mvn clean package -DskipTests

java -jar target\device-simulator-0.0.1-SNAPSHOT.jar
```

> Windows 上 `.jar` 路径分隔符用 `\` 或 `/` 都可以 —— `java -jar` 都接受。

你会看到模拟器开始产生遥测数据：
```
[Sim] DRONE-001 → (12.3, -5.1) battery=87% status=PATROLLING
[Sim] ROBOTDOG-001 → (5.0, 5.0) battery=72% status=IDLE
...
```

模拟器会 **向 Kafka 的 `device-telemetry` topic 发送消息**（偶尔也会向 `device-alerts` 发送告警）。后端的 `KafkaDataConsumer` 会消费这些消息并写入 MongoDB。

#### 验证数据已经流通

#### macOS / Linux
```bash
docker exec -it inspection-mongodb mongosh --quiet inspection --eval \
  'db.device_data.find().sort({_id:-1}).limit(3).toArray()'
```

#### Windows（PowerShell）
```powershell
docker exec -it inspection-mongodb mongosh --quiet inspection --eval "db.device_data.find().sort({_id:-1}).limit(3).toArray()"
```

应该能看到最新的 DeviceData 文档。

### 第 5 步 — 访问 GM 控制台

在浏览器打开：

```
http://localhost:8080
```

#### macOS 一键打开
```bash
open http://localhost:8080
```

#### Windows 一键打开
```powershell
start http://localhost:8080
```

> ⚠️ 修改静态文件后请 **强制刷新**（macOS 按 **Cmd + Shift + R**，Windows/Linux 按 **Ctrl + Shift + R**）以绕过浏览器缓存。

你应该能看到：

| 面板 | 需要验证的内容 |
|---|---|
| **设备状态总览** | 列出 `DRONE-001` / `ROBOTDOG-001`，电量、状态、坐标实时更新 |
| **任务下发** | 点击预设按钮（如 `DRONE-001 + PATROL`），然后点 "下发任务" —— 后端会打印 `[TASK DISPATCHED]`，模拟器收到任务 |
| **告警搜索** | 输入 `LOW` 或 `BATTERY`，点 "搜索" —— Elasticsearch 返回匹配的告警，表格里带时间戳 |
| **图片上传** *(实验性)* | 选择一张 `.png` / `.jpg`，后端上传到 HDFS，返回 `hdfs://localhost:9000/inspection-images/xxx.png` |

---

## 6. 验证清单

用这张清单逐项确认端到端跑通：

- [ ] `docker compose ps` → 所有服务都是 `(healthy)`
- [ ] `curl http://localhost:8080/api/health`（或 Windows 的 `(Invoke-WebRequest ...).Content`）→ `{"status":"UP"}`
- [ ] MongoDB 的 `inspection.device_data` 中有 ≥ 1 条文档
- [ ] 浏览器打开 `http://localhost:8080`，控制台无红色错误
- [ ] 启动模拟器 10 秒内，**设备状态总览** 有数据更新
- [ ] **任务下发** 的 "一键填充" 按钮能填表，点 "下发任务" 成功
- [ ] **告警搜索** 搜 `LOW` 返回 ≥ 1 行，时间戳正确
- [ ] **图片上传** 显示绿色成功卡片，包含 `hdfs://...` 路径

如果某一项失败，跳到 [故障排查](#7-已知问题与故障排查)。

---

## 7. 已知问题与故障排查

### 7.1 HDFS 图片上传 —— **实验性 / 不稳定**

> ⚠️ **HDFS 模块目前是尽力而为的单节点伪分布式实现，尚未达到生产可用标准。** 在本地 Docker 上有以下已知限制，其中最显著的是 **容器网络发现限制**：

| 已知问题 | 影响 | 缓解措施 |
|---|---|---|
| **容器网络发现限制（关键）** | Docker Compose 创建的容器在**同一虚拟网络**（本项目为 `inspection-net`）里可以通过服务名互相访问（如 `kafka:29092`），但**宿主机上的 Spring Boot 进程无法直接用服务名解析**到容器 IP。`FileSystem.get(URI, ...)` 收到的 `hdfs://localhost:9000` 是宿主机端口映射到的 NameNode；后续 DataNode 会回调返回**容器内部 IP**（如 `172.18.0.x:9866`），宿主机解析不到，导致 `Failed to connect to /172.18.0.x:9866` | `ImageController.java` 中通过 `dfs.client.socket-timeout` 超时降级 + 错误捕获返回 500。**生产环境必须换成基于主机名的 RPC（WebHDFS 或 Kerberized 集群）** |
| 单副本 block（`dfs_replication=1`） | 没有数据冗余；容器重启后如果没挂载卷，文件可能丢失 | 演示 OK；**不适合存真实数据** |
| `dfs.webhdfs.enabled=true` 开启 HTTP HDFS | 浏览器访问方便，但**无鉴权** | 仅限本地开发 |
| `bde2020` 镜像以 `root` 运行 Hadoop，且 `dfs.permissions.enabled=false` | 为了避免权限问题故意放开 | 演示可以，**生产环境严禁** |
| DataNode 启动可能与 NameNode 抢跑 | 首次 `docker compose up` 时，DataNode 偶尔会健康检查短暂失败 | 已配置 `depends_on: condition: service_healthy`，重试逻辑可吸收偶发抖动 |
| 上传后 HDFS UI / WebHDFS 读回有滞后 | 由于 NameNode 的 block-report 间隔，可能延迟几秒 | 刷新浏览器即可 |

**何时可以关掉它**：如果你的演示不需要图片存储功能，直接移除 `namenode`、`datanode` 容器以及 `hadoop-client` 依赖即可。平台会优雅降级 —— 上传接口会返回 500 而不会让整个服务崩溃。

**如何验证 HDFS 存储生效**：

#### macOS / Linux
```bash
docker exec inspection-hdfs-namenode hdfs dfs -ls /inspection-images
# → Found N items: <uuid>.png ...
```

#### Windows（PowerShell）
```powershell
docker exec inspection-hdfs-namenode hdfs dfs -ls /inspection-images
```

**常见 HDFS 错误**：

| 错误 | 可能原因 | 解决办法 |
|---|---|---|
| `Connection refused: localhost/127.0.0.1:9000` | namenode 容器没跑起来 | `docker compose up -d namenode` |
| `Failed to connect to /172.18.0.x:9866 : Connection refused` | DataNode 在容器网络里，宿主机无法直接访问（**典型容器网络发现问题**） | 等待上传接口超时回退；这是已知限制，**预期行为** |
| `UnknownHostException: namenode` | 后端试图解析容器主机名 | `ImageController.java` 中已经使用 `localhost:9000`，绕过此问题 |
| `SafeModeException: Cannot create file...` | NameNode 仍在安全模式 | 等 30 秒，或 `docker exec inspection-hdfs-namenode hdfs dfsadmin -safemode leave` |
| `NoSuchMethodError: org.slf4j.spi.LocationAwareLogger.log` | Hadoop SLF4J 1.7 binding 与 Spring Boot 3 的 SLF4J 2.0 冲突 | 检查 `pom.xml` 的排除列表（见第 3 步说明） |

### 7.2 其它常见问题

#### Elasticsearch 拒绝启动：`max virtual memory areas vm.max_map_count [65530] is too low`

仅 Linux / WSL 2。一次性执行：

```bash
# macOS: 不适用（macOS 不暴露 vm.max_map_count）

# Linux / WSL 2
sudo sysctl -w vm.max_map_count=262144

# 永久生效（写入 /etc/sysctl.conf 或 /etc/wsl.conf）
echo 'vm.max_map_count=262144' | sudo tee -a /etc/sysctl.conf
sudo sysctl -p
```

#### Windows 上 Docker Desktop 报 `Hardware assisted virtualization and data execution protection must be enabled`

需要在 **BIOS** 中开启 **VT-x**（Intel）或 **SVM**（AMD）+ **DEP**。每个主板 BIOS 入口不同，一般在 `Advanced → CPU Configuration` 里。

#### 8080 端口被占用

##### macOS / Linux
```bash
lsof -ti:8080 | xargs kill -9
```

##### Windows（PowerShell）
```powershell
# 查找占用 8080 端口的进程
Get-NetTCPConnection -LocalPort 8080 -State Listen | Select-Object OwningProcess
# 杀掉对应 PID
Stop-Process -Id <PID> -Force
```

或修改后端端口 `inspection-server/src/main/resources/application.yml`：
```yaml
server:
  port: 9090
```

#### Kafka 报 `Connection to node -1 could not be established`

确保 Kafka 完全启动后再让后端连接。Docker Compose 的 `depends_on` 已经够用；如果手动跑 Kafka，`docker compose up -d kafka` 后等约 15 秒再启后端。

#### Windows 上 Maven 构建卡在下载依赖

如果在公司代理后面：

- 在 `~/.m2/settings.xml` 中配置代理镜像（`%USERPROFILE%\.m2\settings.xml`）。
- 或者用：
  ```powershell
  mvn -U clean package -DskipTests   # 强制更新快照
  ```

#### Windows 上中文 / 空格路径导致 Maven 失败

把项目放到 **纯英文、无空格** 的路径下：
```powershell
# 推荐
C:\projects\air-ground-inspection

# 避免
C:\Users\张三\Desktop\air ground inspection\
```

#### Windows 上 PowerShell 报 `running scripts is disabled on this system`

以 **管理员** 打开 PowerShell，执行：
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

#### Vue 组件不渲染，控制台报 `Failed to resolve component: el-button`

HTML 使用 **CDN 引入** Vue 3 + Element Plus。首次打开 `http://localhost:8080` 时请确保机器能访问互联网（CDN 脚本由浏览器下载，不是后端）。

要完全离线使用，把 CDN 资源放到 `src/main/resources/static/lib/` 下自托管 —— 本 README 不展开。

#### Elasticsearch 索引映射陈旧 / 错误

清掉重建：

#### macOS / Linux
```bash
curl -X DELETE http://localhost:9200/device_alerts
docker compose restart elasticsearch
```

#### Windows（PowerShell）
```powershell
Invoke-WebRequest -Method DELETE http://localhost:9200/device_alerts
docker compose restart elasticsearch
```

或者全量重置（清空所有数据卷）：
```bash
docker compose down -v
docker compose up -d
```

#### `mvn spring-boot:run` 慢 / 每次保存都重新编译

开发期默认就是这样的。要想获得带热重载的快速迭代体验，加 Spring Boot DevTools：
```xml
<dependency>
  <groupId>org.springframework.boot</groupId>
  <artifactId>spring-boot-devtools</artifactId>
  <scope>runtime</scope>
  <optional>true</optional>
</dependency>
```

---

## 8. 项目结构

```
air-ground-inspection/
├── README.md                         ← 本文件
├── docker-compose.yml                ← Mongo / ZK / Kafka / ES / HDFS
├── .gitignore
│
├── inspection-server/                ← Spring Boot 3.2.5 后端
│   ├── pom.xml
│   └── src/main/
│       ├── java/com/inspection/server/
│       │   ├── InspectionServerApplication.java
│       │   ├── controller/
│       │   │   ├── DeviceController.java     ← /api/devices, /api/alerts/search, /api/tasks/dispatch
│       │   │   └── ImageController.java      ← /api/images/upload（HDFS）
│       │   ├── entity/
│       │   │   ├── DeviceData.java           ← MongoDB 文档
│       │   │   └── DeviceAlert.java          ← Elasticsearch 文档
│       │   ├── repository/
│       │   │   ├── DeviceDataRepository.java
│       │   │   └── DeviceAlertRepository.java
│       │   └── kafka/
│       │       └── KafkaDataConsumer.java    ← 消费 device-telemetry 和 device-alerts
│       └── resources/
│           ├── application.yml               ← 所有连接配置都在这里
│           └── static/index.html             ← Vue 3 GM 控制台（单页应用）
│
└── device-simulator/                 ← 独立 Java 模拟器
    ├── pom.xml
    └── src/main/java/com/inspection/simulator/
        └── DeviceSimulator.java       ← 向 Kafka 发布伪造遥测数据
```

---

## 9. 常用命令速查表

### Docker / 基础设施

#### macOS / Linux
```bash
docker compose up -d                # 启动所有服务（后台）
docker compose logs -f              # 跟踪所有服务的日志
docker compose logs -f kafka        # 跟踪单个服务的日志
docker compose restart elasticsearch# 重启单个服务
docker compose down                 # 停止所有服务（保留数据卷）
docker compose down -v              # 停止并清理所有数据（清空状态）
docker stats                        # 资源占用
```

#### Windows（PowerShell）
```powershell
docker compose up -d
docker compose logs -f
docker compose logs -f kafka
docker compose restart elasticsearch
docker compose down
docker compose down -v
docker stats
```

### 后端

#### macOS / Linux
```bash
cd inspection-server
mvn clean package -DskipTests          # 构建
mvn spring-boot:run                    # 开发模式运行
mvn spring-boot:run -Dspring-boot.run.profiles=dev
mvn -DskipTests package                # 构建 jar（跳过测试）
java -jar target/inspection-server-0.0.1-SNAPSHOT.jar
```

#### Windows（PowerShell）
```powershell
cd inspection-server
mvn clean package -DskipTests
mvn spring-boot:run
mvn spring-boot:run -Dspring-boot.run.profiles=dev
mvn -DskipTests package
java -jar target\inspection-server-0.0.1-SNAPSHOT.jar
```

### 设备模拟器

#### macOS / Linux
```bash
cd device-simulator
mvn clean package -DskipTests
java -jar target/device-simulator-0.0.1-SNAPSHOT.jar
```

#### Windows（PowerShell）
```powershell
cd device-simulator
mvn clean package -DskipTests
java -jar target\device-simulator-0.0.1-SNAPSHOT.jar
```

### 接口冒烟测试

#### macOS / Linux
```bash
curl http://localhost:8080/api/health
curl http://localhost:8080/api/devices | python3 -m json.tool
curl 'http://localhost:8080/api/alerts/search?keyword=LOW' | python3 -m json.tool

curl -X POST http://localhost:8080/api/tasks/dispatch \
  -H 'Content-Type: application/json' \
  -d '{"deviceId":"DRONE-001","command":"PATROL","targetX":12.5,"targetY":-3.2}' \
  | python3 -m json.tool
```

#### Windows（PowerShell）
```powershell
(Invoke-WebRequest http://localhost:8080/api/health).Content
(Invoke-WebRequest http://localhost:8080/api/devices).Content | ConvertFrom-Json | ConvertTo-Json -Depth 5
(Invoke-WebRequest 'http://localhost:8080/api/alerts/search?keyword=LOW').Content | ConvertFrom-Json | ConvertTo-Json -Depth 5

Invoke-WebRequest -Method POST -Uri http://localhost:8080/api/tasks/dispatch `
  -ContentType 'application/json' `
  -Body '{"deviceId":"DRONE-001","command":"PATROL","targetX":12.5,"targetY":-3.2}'
```

### 数据库

#### macOS / Linux
```bash
# MongoDB Shell
docker exec -it inspection-mongodb mongosh inspection

# 在 mongosh 内：
db.device_data.find().sort({_id:-1}).limit(5)
db.device_data.countDocuments()
show collections

# Elasticsearch
curl http://localhost:9200/_cat/indices?v
curl http://localhost:9200/device_alerts/_mapping?pretty
curl http://localhost:9200/device_alerts/_search?pretty&size=2

# Kafka topics
docker exec -it inspection-kafka kafka-topics \
  --bootstrap-server localhost:9092 --list
docker exec -it inspection-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic device-telemetry --from-beginning --max-messages 5

# HDFS
docker exec inspection-hdfs-namenode hdfs dfs -ls /
docker exec inspection-hdfs-namenode hdfs dfs -ls /inspection-images
docker exec inspection-hdfs-namenode hdfs dfs -df -h
```

#### Windows（PowerShell）
```powershell
# MongoDB Shell
docker exec -it inspection-mongodb mongosh inspection

# Elasticsearch
(Invoke-WebRequest http://localhost:9200/_cat/indices?v).Content
(Invoke-WebRequest http://localhost:9200/device_alerts/_mapping?pretty).Content
(Invoke-WebRequest 'http://localhost:9200/device_alerts/_search?pretty&size=2').Content

# Kafka topics
docker exec -it inspection-kafka kafka-topics --bootstrap-server localhost:9092 --list
docker exec -it inspection-kafka kafka-console-consumer `
  --bootstrap-server localhost:9092 `
  --topic device-telemetry --from-beginning --max-messages 5

# HDFS
docker exec inspection-hdfs-namenode hdfs dfs -ls /
docker exec inspection-hdfs-namenode hdfs dfs -ls /inspection-images
```

### 浏览器地址

| URL | 作用 |
|---|---|
| `http://localhost:8080` | GM 控制台（主应用） |
| `http://localhost:9200` | Elasticsearch 根 |
| `http://localhost:9870` | HDFS NameNode Web UI |
| `http://localhost:9870/explorer.html#/inspection-images` | HDFS 文件浏览器 |

---

## 附录 A — 配置文件参考

### `application.yml`（关键段落）

```yaml
server:
  port: 8080

spring:
  data:
    mongodb:
      uri: mongodb://localhost:27017/inspection
    elasticsearch:
      uris: http://localhost:9200
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: inspection-server
      auto-offset-reset: earliest

inspection:
  kafka:
    topic:
      device-tasks: device-tasks
```

### Kafka Topic 一览

| Topic | 生产者 | 消费者 |
|---|---|---|
| `device-telemetry` | 模拟器 | `inspection-server`（→ MongoDB） |
| `device-alerts` | 模拟器 | `inspection-server`（→ Elasticsearch） |
| `device-tasks` | GM 控制台（通过 `inspection-server`） | 模拟器 / 未来的真实设备 |

---

## 附录 B — Windows 与 macOS 命令对照速查

| 操作 | macOS / Linux | Windows（PowerShell） |
|---|---|---|
| 列文件 | `ls` | `dir` / `Get-ChildItem` |
| 切换目录 | `cd path` | `cd path` |
| 创建目录 | `mkdir -p path` | `New-Item -ItemType Directory -Path path -Force` |
| 删除文件 | `rm file` | `Remove-Item file` |
| 强制删除 | `rm -rf dir` | `Remove-Item dir -Recurse -Force` |
| 查看端口占用 | `lsof -ti:8080` | `Get-NetTCPConnection -LocalPort 8080` |
| 杀进程 | `kill -9 PID` | `Stop-Process -Id PID -Force` |
| 测试接口 | `curl ...` | `Invoke-WebRequest ...` |
| 解析 JSON | `python3 -m json.tool` | `ConvertFrom-Json \| ConvertTo-Json -Depth 5` |
| 打开浏览器 | `open http://...` | `start http://...` |
| 跟踪日志 | `docker compose logs -f` | `docker compose logs -f`（PowerShell 也支持） |

---

## 许可证

团队内部演示项目。**未经显著加固（鉴权、TLS、多副本 HDFS、Kafka SASL、ES 安全特性等）前，请勿用于生产环境。**

---

> **遇到问题？** 在本仓库提 issue 并 @ 平台组。如果只是本地环境问题（Docker、JDK、端口等），先翻 [第 7 节](#7-已知问题与故障排查)。
