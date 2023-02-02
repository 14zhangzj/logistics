---
stypora-copy-images-to: img
typora-root-url: ./
---



# Logistics_Day01：项目概述及Docker入门

[每个知识点学习目标：了解（know）、理解（understand）、掌握（grasp）、复习（review）]()

![1612049103044](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612049103044.png)



## 01-[理解]-客快物流大数据项目概述

> 整个客快物流项目：`Lambda 架构`，既有离线批处理，又有实时流计算。[离线+实时]()
>
> - 第一层、批处理层（Batch Layer），离线分析
> - 第二层、速度层（Speed Layer），实时计算
> - 第三层、服务层（Server Layer），将批处理层和速度层处理数据结构进行展示和报表。

![TIM截图20191101174020.png](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/QDlhSAaBxK4LEvk.png)

> 项目概述

![1612056313518](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612056313518.png)

![1612056340147](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612056340147.png)

![1612056351148](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612056351148.png)

![1612056357616](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612056357616.png)



## 02–[了解]-第1天课程内容提纲

> 整个项目划分为9章内容，具体细华如下：

![1612057522113](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612057522113.png)

> - 第一部分、第1章至第4章（day01-day03）-3天
>   - [项目概述、环境准备工作]()‘
> - 第二部分、第5章至第6章（day04-day09）- 6天
>   - [数据实时ETL存储（StructuredStreaming）和离线报表分析与即席查询]()
> - 第三部分、第7章至第9章（day10-day13）- 4天
>   - [实时OALP分析和数据服务开发（大屏展示）]()



> 今天主要讲解2个大方面内容：
>
> - 1）、项目介绍及解决方案
>   - 项目介绍（了解即可）
>   - `项目解决放（理解）`

![1612057771480](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612057771480.png)

> - 2）、Docker 基本入门
>   - Docker 容器技术，方便用户部署应用，实现跨机器集群迁移部署。
>   - 使用Docker 部署MySQL数据库和Oracle数据库。

![1612057921421](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612057921421.png)



## 03–[掌握]-项目整体介绍

> 针对客快物流项目来说，最终展示大屏，实时统计物流业务相关指标，基于ClickHouse OALP数据库。

![1612058034205](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612058034205.png)



> ​			在物流快读行业，最核心的数据：`快递单（express_bill）和运单(way_bill)`，由于电子商务和网购迅速发展，使得数据量剧增，传递数据库存储和分析技术已经满足不了需求，使用大数据技术，进行数据存储和数据分析。
>
> - 海量数据存储：数据量大
> - 海量数据分析：分析时效性，业务复杂性
>
> 基于大数据技术存储数据和分析计算，带来好处：
>
> [物流大数据可以根据市场进行数据分析，提高运营管理效率，合理规划分配资源，调整业务结构，确保每个业务均可盈利。根据数据分析结果，规划、预计运输路线和配送路线，环节运输高峰期的物流行为，提高客户的满意度，提高客户粘度。]()

![1612058605180](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612058605180.png)



## 04–[理解]-物流实时大屏系统

> 实时大屏后台思路

![1612059087468](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612059087468.png)

> 从实时OLAP数据库ClickHouse表中读取数据，大屏每隔10秒查询数据库表，将数据展示前端大屏

![1612059131170](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612059131170.png)



## 05–[理解]-项目核心业务流程

> 快递业务流程，整个来说，分为5个方面：
>
> - 1）、发货客户，[快递单express_bill]()
> - 2）、受理部分，[快递小哥手快递]()
> - 3）、发货网点仓库，[运单way_bill]()
> - 4）、中转仓库，[仓储、车辆、运输、网点等数据]()
> - 5）、收货客户
>
> ==客户依据快递单号或运单号，查询快递物流信息，比较重要业务。==

![1612059838426](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612059838426.png)



## 06–[理解]-项目逻辑架构

> 充分理解整个项目逻辑架构图，其中涉及到使用大数据技术，知道每个技术功能，主要在项目承担角色。

![1612062310549](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612062310549.png)

> ​		**即席查询Adhoc**，使用Hue与Impala集成，查询Kudu表或者Hive表数据，Impala基于内存分析引擎，诞生之初目标就是取代Hive框架（底层引擎MapReduce）。

![1612061765596](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612061765596.png)

> 目前来说，在企业中，SQL on HADOOP 技术发展路线如下所示：

![1612061947655](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612061947655.png)



> Flume，实时日志采集框架，主要采集日志文件数据，实时存储到HDFS文件系统中
>
> 官网：http://flume.apache.org
>
> 资料：https://blog.csdn.net/fseast/article/details/98989701

![Agent component diagram](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/DevGuide_image00.png)



## 07–[掌握]-项目数据流转及核心业务

![1612062344860](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612062344860.png)

> 项目数据流转图，主要分为个大部分：
>
> - 第一、业务系统数据部分
>
>   [业务数据存的地方：异构数据源存储，此处为了简化，业务数据存储在2个类型数据库：]()
>
>   - 第一个、Oracle数据库，存储物流（`Logistics`）系统相关业务数据
>   - 第二、MySQL数据库，存储客户关系管理系统（`CRM`)相关业务数据
>
>   ==无论Oracle数据库还是MYSQL数据库，采用Docker容器部署，方便轻松使用==
>
>   - 使用OGG采集Oracle表数据、使用Canal采集MySQL表数据，以JSON格式字符串存储Kafka Topic。
>
> ![1612062744979](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612062744979.png)
>
> - 第二、大数据系统数据部分，具体划分为4个方面内容
>
>   - 其一、数据实时ETL存储，[从Kakka 消费数据，经过ETL转换处理以后，存储至存储引擎。]()
>   - 将数据存储至：==Kudu(类似HBase数据库）、ClickHouse(OLAP 分析数据库）和ES（索引）==
>
>   ![1612062878448](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612062878448.png)
>
>   - 其二、离线报表和即席查询，[数据存储Kudu数据库]()
>     - 使用SparkSQL加载Kudu表数据，进行离线主题报表分析
>     - Hue为Impala提供SQL界面，底层Impala分析Kudu表数据，进行即席查询
>     - 使用Azkaban调度执行离线报表程序：定时调度和依赖调度
>
>   ![1612063034494](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612063034494.png)
>
>   - 其三、快速物流检索
>     - 将快递单数据和运单数据存储Es索引中，语句快递到查询物流信息
>
>   ![1612063082351](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612063082351.png)
>
>   - 其四、OALP 分析查询
>     - 将所有业务数据存储至ClickHouse表中，基于JDBC Client API提供服务接口，实时大屏每隔10秒查询数据，更新界面
>     - SparkCloud开发数据服务即可
>
>   ![1612063120342](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612063120342.png)



## 08–[理解]-项目技术选型及软件版本

> - 1）、采用Kafka作为消息传输中间介质

![1612064305058](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612064305058.png)

> - 2）、分布式计算采用Spark生态

![1612064329521](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612064329521.png)

> - 3）、ETL 数据存储
>   - ETL后的数据存储到Kudu中，供实时、准实时查询、分析；
>   - Elastic Search作为单据数据的存储介质，供顾客查询订单信息；
>   - ClickHouse作为实时数据的指标计算存储数据库

框架软件版本：

![1612064411924](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612064411924.png)



## 09–[理解]-项目非功能描述

> - 1）、框架版本选型

![1612065052014](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612065052014.png)



> - 2）、服务器选型，[安装软件是物理机还是云主机？？]()

![1612065241811](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612065241811.png)



> - 3）、集群规模

![1612065366143](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612065366143.png)



> - 4）、人员配置参考

![1612065455951](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612065455951.png)

> - 5）、开发周期，[在实际工业项目开发中，真正写代码时间最多50%时间，更多沟通对接，业务把控。]()

![1612065516245](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612065516245.png)



## 10–[了解]-技术亮点及服务器规划

> 技术亮点，[在编写简历时，有重点的突出两点，]()

![1612065686084](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612065686084.png)

> 服务器资源规划，为了简化，更多时间放在业务上，并不是环境上。

![1612065862668](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612065862668.png)



## 11–[了解]-第2章课程目标

> 今天来说，主要给大家：Docker 入门使用，重点在于==【Docker 组件和安装部署】==

![1612066175261](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612066175261.png)

> Docker 容器引擎：==Contanier容器==，类似Hadoop YARN中容器Contanier，封装资源（内存Memory和CPU Core），让任务Task（如MapTask、Executor）单独占用资源，进行使用。

![1612066527281](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612066527281.png)



## 12–[了解]-大数据项目为什么使用Docker

> ​		Docker会让大数据平台部署更加简单快捷、让研发和测试团队集成交付更加敏捷高效、让产线环境的运维更加有质量保障。

![1612075375705](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612075375705.png)

> ​		本次项目开发主要是给大家讲解Docker的应用，在**业务系统采用Docker部署**，大数据服务器依然采用虚拟机的方式进行部署。

## 13–[理解]-什么是虚拟化

> ​		虚拟化简单讲，就是==把一台物理计算机虚拟成多台逻辑计算机，每个逻辑计算机里面可以运行不同的操作系统，相互不受影响==，这样就可以充分利用硬件资源。

- 1）、虚拟化类型一：直接在硬件资源之上，安装虚拟化软件，虚拟化出虚拟机。

![1612075764769](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612075764769.png)

- 2）、虚拟化类型二：比在Win10上安装VMWare，创建虚拟机，安装操作系统。

![1612075813158](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612075813158.png)



## 14–[掌握]-初识Docker 容器

> Docker是一个开源的应用容器引擎，

![1612076138155](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612076138155.png)

> ​		Docker可以让开发者打包他们的应用以及依赖包到一个轻量级，可移植的容器中，然后发布到
> 任何流行的linux服务器上

![1612076280554](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612076280554.png)

> ==Docker是一种容器技术，解决软件跨环境迁移的问题==



## 15–[理解]-容器与虚拟机比较

> 虚拟机是一个计算机系统的仿真，简单来说，虚拟机可以实现在一台物理计算机上模拟多台计算机运行务。
>
> - 操作系统和应用共享一台或多台主机(集群)的硬件资源，每台VM有自己的OS，硬件资源是虚拟化的。
> - 管理程序(hypervisor)负责创建和运行VM，它连接了硬件资源和虚拟机，完成server的虚拟化。由于虚拟化技术和云服务的出现，IT部门通过部署VM可以可减少cost提高效率。

![1612076507991](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612076507991.png)



> ==容器是将操作系统虚拟化==：
>
> - 1）、容器是在操作系统之上，每个容器共享OS内核，执行文件和库等
> - 2）、容器是非常轻量的，仅仅MB水平并且几秒即可启动
> - 3）、与VM相比，容器仅需OS、支撑程序和库文件便可运行应用

![1612076653554](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612076653554.png)

> 虚拟机VM与容器Contanier比较：

![1612076900085](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612076900085.png)

> ###### ==与传统的虚拟化相比，Docker优势体现在启动速度快，占用体积小==



## 16–[掌握]-Docker 服务端和客户端

> ​		Docker是一个**客户端-服务端（Client/Server）架构程序**，Docker客户端只需要向Docker服务端或者守护进程发出请求，服务端或者守护进程完成所有工作返回结果。

![1612077125442](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612077125442.png)

> ​		Docker提供了一个命令行工具Docker以及一整套的Restful API，可以在同一台宿主机器上运行Docker守护进程或者客户端，也可以从本地的Docker客户端连接到运行在另一台宿主机上的远程Docker守护进程。
>
> [Docker 服务端Server和Client客户端，与MySQL数据库类似（Server服务端和客户端mysql）]()

![1612077210810](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612077210810.png)

> `Docker引擎是一个c/s结构的应用`，基本组成：
>
> - 1）、镜像Image，认为是创建Java类Class，就是模板
> - 2）、容器Contanier，认为是基于Java类Class创建对象，实例
> - 3）、网络network（端口号映射）和数据卷（磁盘挂载mount）

![1612077290191](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612077290191.png)

![1612077474957](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612077474957.png)



## 17–[掌握]-Docker 构架（镜像和容器）

> ==Docker使用C/S架构，Client 通过接口与Server进程通信实现容器的构建，运行和发布==
>
> - 1）、容器创建：run
> - 2）、镜像构建：build
> - 3）、运行容器：start
> - 4）、镜像发布：deploy

![1612078063935](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612078063935.png)

> - 镜像可以用来创建 Docker 容器，一个镜像可以创建很多容器。
> - 镜像（Image）就是一堆只读层（read-only layer）的统一视角

![1612078117957](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612078117957.png)



> ​		Docker 利用容器（Container）来运行应用,容器是从镜像创建的运行实例。它可以被启动、开始、停止、删除。每个容器都是相互隔离的、保证安全的平台。
>
> [Image跟Container的职责区别：Image负责APP的存储和分发，Container负责运行APP。]()

![1612078343772](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612078343772.png)

> **容器 = 镜像 + 读写层**



## 18–[掌握]-Docker 安装及服务启动

![1612079523297](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612079523297.png)

> Docker 官方文档：https://www.docker.com/，按照教案上步骤进行操作即可。
>
> [解压提供虚拟机【CentOS7.7.zip】包，导入虚拟机至VMWare之后，启动之前，需要先配置IP地址与主机名映射：etc/hosts]()，==需要注释掉其他映射==
>
> [192.168.88.10   node1.itcast.cn  node1]()
>
> [192.168.88.20   node2.itcast.cn  node2]()

![1612079749779](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612079749779.png)

> 导入虚拟机以后，启动虚拟机时，选择已经导入移动该虚拟机

![1612079957733](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612079957733.png)

> 修改虚拟机为命令行模式（启动后为桌面模式）
>
> ```ini
> [root@node1 ~]# systemctl  get-default
> graphical.target
> 
> [root@node1 ~]# systemctl  set-default multi-user.target
> Removed symlink /etc/systemd/system/default.target.
> Created symlink from /etc/systemd/system/default.target to /usr/lib/systemd/system/multi-user.target.
> 
> [root@node1 ~]# 
> [root@node1 ~]# systemctl  get-default      
> multi-user.target
> 
> [root@node1 ~]# shutdown -r now
> ```

Docker 服务启动与停止相关命令

![1612081006290](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612081006290.png)





## 19–[掌握]-Docker 配置阿里云镜像加速

> 为什么要配置镜像加速器，原因在于更好快速的拉取镜像image。

![1612081222388](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612081222388.png)

按照如下官方文档说明进行配置镜像加速器即可：

![1612081390841](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612081390841.png)



## 20–[掌握]-Docker 容器快速运行ContOS 系统

> 使用Docker拉取镜像，并且创建容器，运行CentOS7操作系统。

![1612081595120](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612081595120.png)

```
0. 搜索镜像 search
	https://hub.docker.com/
1. 镜像拉取pull
	docker pull centos:7
	
	[root@node1 ~]# docker images
    REPOSITORY   TAG       IMAGE ID       CREATED        SIZE
    centos       7         8652b9f0cb4c   2 months ago   204MB

2. 创建镜像run
    [root@node1 ~]# docker run -di --name=mycentos centos:7
	ac50485e9533ebe7b9d3e31bb6e8e241592807dd8e1340270e8c4714e1d35894
	
[root@node1 ~]# docker ps -a
CONTAINER ID   IMAGE      COMMAND       CREATED          STATUS          PORTS     NAMES
ac50485e9533   centos:7   "/bin/bash"   28 seconds ago   Up 26 seconds             mycentos

3. 进入镜像exec
[root@node1 ~]# docker exec -it mycentos /bin/bash
[root@ac50485e9533 /]# 
[root@ac50485e9533 /]# 
[root@ac50485e9533 /]# hostname
ac50485e9533
[root@ac50485e9533 /]# 
[root@ac50485e9533 /]# 
```






