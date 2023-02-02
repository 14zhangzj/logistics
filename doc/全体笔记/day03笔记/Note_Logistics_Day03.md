---
stypora-copy-images-to: img
typora-root-url: ./
---



# Logistics_Day03：业务服务器和大数据服务器



![1612173547954](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612173547954.png)



## 01-[复习]-上次课程内容回顾

> 上次课主要讲解：Docker 命令使用（镜像命令、容器命令、备份和迁移、Dockerfile文件、私有仓库）
>

![1612313600114](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612313600114.png)

> 思维导图：

![1612314888113](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612314888113.png)



## 02-[了解]-第3天：课程内容提纲

> 主要讲解：业务数据服务器（MySQL数据库和Oracle数据库）和大数据服务器（CM安装部署CDH）。

![1612315043152](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612315043152.png)

> - 1）、业务服务器，==项目业务数据存储如何实时同步至大数据平台==
>
>   [异构数据源，业务数据存储在不同地方，此处指的就是Orale数据库和MySQL数据库]()
>
>   - 物流系统Logistics数据：Oracle数据库，采用Docker 容器部署
>     - 部署`OGG`中间件，实时采集表中的数据到Kafka Topic中。
>   - 客户关系管理系统CRM数据：MySQL数据看看，采用Docker 容器部署
>     - 部署`Canal`工具，实时采集表中数据到Kafka Topic中。
>
>   [无论是OGG还是Canal采集数据都是以JSON字符串形式，发送到Kafka Topic中。]()

![1612315412114](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612315412114.png)

> - 2）、大数据服务器
>   - 使用ClouderaManager安装部署CDH软件，类似在线教育项目环境一致，使用一台机器即可。

![1612315441102](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612315441102.png)



## 03-[理解]-业务服务器环境概述

> 业务服务器（node1.itcast.cn，192.168.88.10）主要使用**Docker** 部署数据库：Oracle和MySQL数据库。
>
> [系统用户名：root、密码：123456]()

![1612316929449](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612316929449.png)

> 在业务服务器中，使用Docker 容器部署如下服务：
>
> - 1）、容器Contanier：mysql
>
>   [安装MySQL数据库，存储客户关系管理CRM相关业务数据，3张表数据]()
>
> - 2）、容器Contanier：canal-server
>
>   [安装Canal Server服务，实时监控MySQL数据库表的数据，一旦有数据变化，立即获取，发送Topic]()
>
> - 3）、容器Contanier：myoracle
>
>   [安装Oracle数据库，存储物流系统Logistics相关业务数据，48张表；并且按照OGG软件]()



## 04-[掌握]-业务服务器之Oracle 数据库

> 首先需要找到按照Oracle数据库容器，然后启动容器，最后进入容器中启动Oracle数据库。

```ini
[root@node1 ~]# docker ps -a
CONTAINER ID        IMAGE                             COMMAND                  CREATED             STATUS                      PORTS                                                     NAMES
28888fad98c9        canal/canal-server:v1.1.2         "/alidata/bin/main.s…"   8 months ago        Exited (255) 5 months ago   2222/tcp, 8000/tcp, 11112/tcp, 0.0.0.0:11111->11111/tcp   canal-server
8b5cd2152ed9        mysql:5.7                         "docker-entrypoint.s…"   8 months ago        Exited (0) 33 hours ago                                                               mysql
cb7a41433712        kungkk/oracle11g_centos7:latest   "/bin/bash"              8 months ago        Exited (255) 5 months ago   0.0.0.0:1521->1521/tcp                                    myoracle

[root@node1 ~]# 
[root@node1 ~]# docker start myoracle
myoracle
[root@node1 ~]# 
[root@node1 ~]# docker ps
CONTAINER ID        IMAGE                             COMMAND             CREATED             STATUS              PORTS                    NAMES
cb7a41433712        kungkk/oracle11g_centos7:latest   "/bin/bash"         8 months ago        Up 17 seconds       0.0.0.0:1521->1521/tcp   myoracle

[root@node1 ~]# docker exec -it myoracle /bin/bash
[root@server01 oracle]# 
```

> 当进入myoracle容器以后，需要启动Oracle数据库（启动过程相对比较麻烦），按照如下文件进行即可：
>
> [启动Oracle数据库服务时，需要启动：数据库服务Server和监听服务1521]()

![1612317478012](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612317478012.png)

```ini
# 1. 切换root用户为oracle用户
[root@server01 oracle]# su - oracle
Last login: Mon Aug 31 09:00:22 UTC 2020 on pts/2
-bash: warning: setlocale: LC_ALL: cannot change locale (en_US): No such file or directory
-bash: warning: setlocale: LC_ALL: cannot change locale (en_US): No such file or directory

# 2. 使用sqlplus，进入操作命令行
[oracle@server01 ~]$ which sqlplus
/u01/app/oracle/product/11.2.0/bin/sqlplus
[oracle@server01 ~]$ sqlplus "/as sysdba"

SQL*Plus: Release 11.2.0.1.0 Production on Wed Feb 3 01:59:03 2021

Copyright (c) 1982, 2009, Oracle.  All rights reserved.

Connected to an idle instance.

SQL> 

# 3. 启动Oracle数据库服务
SQL> startup
ORACLE instance started.

Total System Global Area 1202556928 bytes
Fixed Size                  2212816 bytes
Variable Size             654314544 bytes
Database Buffers          536870912 bytes
Redo Buffers                9158656 bytes
Database mounted.

Database opened.

# 4. 启动监听服务
[root@node1 ~]# docker exec -it myoracle /bin/bash  
[root@server01 oracle]# su - oracle
Last login: Wed Feb  3 01:58:34 UTC 2021 on pts/0
-bash: warning: setlocale: LC_ALL: cannot change locale (en_US): No such file or directory
-bash: warning: setlocale: LC_ALL: cannot change locale (en_US): No such file or directory
[oracle@server01 ~]$ cd $ORACLE_HOME/bin
[oracle@server01 bin]$ ls
..............................
[oracle@server01 bin]$ lsnrctl start 

LSNRCTL for Linux: Version 11.2.0.1.0 - Production on 03-FEB-2021 02:02:24

Copyright (c) 1991, 2009, Oracle.  All rights reserved.

Starting /u01/app/oracle/product/11.2.0//bin/tnslsnr: please wait...

TNSLSNR for Linux: Version 11.2.0.1.0 - Production
System parameter file is /u01/app/oracle/product/11.2.0/network/admin/listener.ora
Log messages written to /u01/app/oracle/diag/tnslsnr/server01/listener/alert/log.xml
Listening on: (DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=EXTPROC1521)))
Listening on: (DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=server01)(PORT=1521)))

Connecting to (DESCRIPTION=(ADDRESS=(PROTOCOL=IPC)(KEY=EXTPROC1521)))
STATUS of the LISTENER
------------------------
Alias                     LISTENER
Version                   TNSLSNR for Linux: Version 11.2.0.1.0 - Production
Start Date                03-FEB-2021 02:02:26
Uptime                    0 days 0 hr. 0 min. 0 sec
Trace Level               off
Security                  ON: Local OS Authentication
SNMP                      OFF
Listener Parameter File   /u01/app/oracle/product/11.2.0/network/admin/listener.ora
Listener Log File         /u01/app/oracle/diag/tnslsnr/server01/listener/alert/log.xml
Listening Endpoints Summary...
  (DESCRIPTION=(ADDRESS=(PROTOCOL=ipc)(KEY=EXTPROC1521)))
  (DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=server01)(PORT=1521)))
The listener supports no services
The command completed successfully


# 5. 验证启动是否成功
SQL> select instance_name AS "SID",host_name,version from v$instance;

SID
--------------------------------
HOST_NAME
--------------------------------------------------------------------------------
VERSION
----------------------------------
orcl
server01
11.2.0.1.0
```



> 使用数据库Client工具：DBeave连接Oracle数据库，创建连接时信息如下所示：

![1612317905816](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612317905816.png)

> 连接到Oracle数据库以后，查看到物流系统数据

![1612318124950](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612318124950.png)

## 05-[掌握]-业务服务器之MySQL 数据库

> ​		在业务服务器中，将CRM系统使用MySQL数据库部署在Docker容器中：mysql 容器，与前面类似先启动容器，在启动数据库，最后使用DBeave连接。

```ini
# 1. 启动容器，默认就启动MySQL数据库，不需要人为启动
[root@node1 ~]# docker ps -a
CONTAINER ID        IMAGE                             COMMAND                  CREATED             STATUS                      PORTS                                                     NAMES
28888fad98c9        canal/canal-server:v1.1.2         "/alidata/bin/main.s…"   8 months ago        Exited (255) 5 months ago   2222/tcp, 8000/tcp, 11112/tcp, 0.0.0.0:11111->11111/tcp   canal-server
8b5cd2152ed9        mysql:5.7                         "docker-entrypoint.s…"   8 months ago        Exited (0) 34 hours ago                                                               mysql
cb7a41433712        kungkk/oracle11g_centos7:latest   "/bin/bash"              8 months ago        Up 15 minutes               0.0.0.0:1521->1521/tcp                                    myoracle
[root@node1 ~]# 
[root@node1 ~]# docker start mysql
mysql
[root@node1 ~]# 
[root@node1 ~]# docker ps
CONTAINER ID        IMAGE                             COMMAND                  CREATED             STATUS              PORTS                               NAMES
8b5cd2152ed9        mysql:5.7                         "docker-entrypoint.s…"   8 months ago        Up 8 seconds        0.0.0.0:3306->3306/tcp, 33060/tcp   mysql
cb7a41433712        kungkk/oracle11g_centos7:latest   "/bin/bash"              8 months ago        Up 16 minutes       0.0.0.0:1521->1521/tcp              myoracle

# 2. 进入容器
[root@node1 ~]# docker exec -it mysql /bin/bash
root@8b5cd2152ed9:/#

# 3. 登录客户端
root@8b5cd2152ed9:/# mysql -uroot -p
Enter password: 
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 2
Server version: 5.7.30-log MySQL Community Server (GPL)

Copyright (c) 2000, 2020, Oracle and/or its affiliates. All rights reserved.

Oracle is a registered trademark of Oracle Corporation and/or its
affiliates. Other names may be trademarks of their respective
owners.

Type 'help;' or '\h' for help. Type '\c' to clear the current input statement.

mysql> show databases ;
+--------------------+
| Database           |
+--------------------+
| information_schema |
| canal_tsdb         |
| crm                |
| mysql              |
| performance_schema |
| sys                |
| test               |
+--------------------+
7 rows in set (0.53 sec)

mysql> show tables in crm ;
+--------------------------+
| Tables_in_crm            |
+--------------------------+
| crm_address              |
| crm_consumer_address_map |
| crm_customer             |
+--------------------------+
3 rows in set (0.00 sec)

```

> 为了后续方便对CRM系统数据库进行操作，同样使用DBeaver进行连接：

![1612318536994](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612318536994.png)



## 06-[掌握]-OGG 数据同步之功能概述

> ​		`OGG（Oracle GoldenGate）` 是一种基于**日志**的**结构化数据复制**软件，通过==解析源数据库在线日志或归档日志获得数据的增删改==变化，将数据投递到目标端（Oracle数据库表中、Kafka消息队列Topic中等等）。

![1612320791630](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612320791630.png)

> ​		OGG 能够实现大量交易数据的实时捕捉，变换和投递，实现源数据库与目标数据库的数据同步，保持最少10ms的数据延迟。

![1612320849409](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612320849409.png)



## 07-[掌握]-OGG 数据同步之基本原理及架构

> ​		Oracle GoldenGate 实现原理是**通过抽取源端的redo log 或者 archive log** ，然后**通过TCP/IP投递到目标端**，最后**解析还原应用到目标端**，使目标端实现 同源端数据同步。

![1612321610205](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612321610205.png)

> - 1）、利用抽取进程(Extract Process)在源端数据库中读取Online Redo Log或者Archive Log，然
>   后进行解析，只提取其中数据的变化信息，比如DML操作——增、删、改操作；
> - 2）、将抽取的信息转换为GoldenGate自定义的中间格式存放在队列文件(trail file)中
> - 3）、再利用传输进程将队列文件(trail file)通过TCP/IP传送到目标系统
> - 4）、目标端有一个进程叫Server Collector，这个进程接受了从源端传输过来的数据变化信息
> - 5）、把信息缓存到GoldenGate 队列文件(trail file)当中，等待目标端的复制进程读取数据
> - 6）、GoldenGate 复制进程Replicat process，从队列文件(trail file)中读取数据变化信息，并创建对应的SQL语句，通过数据库的本地接口执行，提交到目标端数据库，提交成功后更新自己的检查点，记录已经完成复制的位置，数据的复制过程最终完成。
>
> [OGG可以实现将Oracle数据库中表的数据实时同步到消息队列或数据库中，实时增量（Incremental）同步]()

## 08-[理解]-OGG 数据同步之拓扑结构及支持环境

> OGG属于数据同步工具，可以使用在不同业务需求中，主要功能如下图所示：

![1612322129973](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612322129973.png)

> ​		GoldenGate TDM的复制模式非常灵活，用户可以根据自己的需求选择特定的复制方式，并根据系统扩展对复制进行扩展。
>
> ​	针对最新版本OGG来说，==源和目标的操作系统和数据库可以进行任意的组合==

![1612322445774](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612322445774.png)

> 实际项目中，通过==将Oracle数据库数据同步到Oracle数据库或消息对象Kafka==。



## 09-[掌握]-OGG 数据同步之测试环境准备

> ​			测试OGG实时同步数据到Kafka消息队列中，[无需配置OGG，node1虚拟机中myoracle容器已经配置好，只需要启动OGG服务即可。]()
>
> - step1、启动Oracle数据库：数据库服务Server和监听服务Listener
>
>   - [参考<04-[掌握]-业务服务器之Oracle 数据库>章节内容]()
>
> - step2、启动Kafka消息队列：首先启动Zookeeper，再启动Kafka，最后创建Topic
>
>   - 解压给大家提供虚拟机【node2.itcast.cn】，导入至VMWare中，启动虚拟机
>   - 默认情况下，启动CM和所有的数据服务，[建议在CM界面，将所有服务停止]()
>   - 在CM（http://node2.itcast.cn:7180）界面，先启动Zookeeper，再启动Kafka
>
>   [如果CM界面一直打不开，可能CM端口号被占用]()
>
>   ```ini
>   [root@node2 ~]# netstat -tnlp   
>   
>   # CM使用9000端口号，默认情况下与ClickHouse数据库端口号重复，可以停止CKServer
>   [root@node2 ~]# systemctl stop clickhouse-server 
>   
>   ```
>
>   [使用KafkaTools工具连接Kafka服务，创建Topic和查看Topic中数据]()
>
>   ![1612324507435](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612324507435.png)
>
> - step3、启动OGG：具体来说分为2个部分
>
>   [配置OGG 11g版本，没有配置Collect进程，所有配置进程如下所示：]()
>
>   ![1612324623208](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612324623208.png)
>
>   [切换到 oracle 账号且启动了 Oracle；注意：要严格按照启动顺序执行：]()
>
>   ```
>   第一步：启动源端mgr进程
>   第二步：启动目标端mgr进程
>   第三步：启动源端extract进程
>   第四步：启动源端pump进程
>   第五步：启动目标端replicate进程
>   ```
>
>   - 源端管理进程和其他进程启动
>   - 目标端管理进程和其他进程启动

```ini
4) 启动OGG服务
	切换到 oracle 账号，
	第一步：启动源端mgr进程
	[root@node1 ~]# docker exec -it myoracle /bin/bash
	[root@server01 oracle]# su - oracle
	[oracle@server01 ~]$ cd $OGG_SRC_HOME
	[oracle@server01 src]$ ./ggsci	
	GGSCI (server01) 1> info all
	GGSCI (server01) 2> start mgr
	
	第二步：启动目标端mgr进程
	[root@node1 ~]# docker exec -it myoracle /bin/bash
	[root@server01 oracle]# su - oracle
	[oracle@server01 ~]$ cd $OGG_TGR_HOME
	[oracle@server01 tgr]$ ./ggsci
	GGSCI (server01) 1> start mgr
	GGSCI (server01) 2> info all
	
	第三步：启动源端extract进程
	GGSCI (server01) 4> start extkafka
	
	第四步：启动源端pump进程
	GGSCI (server01) 5> start pukafka
	
	第五步：启动目标端replicate进程
	GGSCI (server01) 7> start rekafka

```



## 10-[掌握]-OGG 数据同步之物流数据同步Kafka

> 在DBeave连接Oracle数据库命令行界面或UI界面，对物流系统数据库【ITCAST】表的数据进行CUD操作。

```SQL
-- 插入数据INSERT
INSERT INTO ITCAST."tbl_company"("id", "company_name", "city_id", "company_number", "company_addr", "company_addr_gis", "company_tel", "is_sub_company", "state", "cdt", "udt", "remark")VALUES(11, '广州传智速递邮箱公司', 440100, NULL, '广州校区', '117.28177895734918_31.842711680531399', NULL, 1, 1, TO_DATE('2020-06-13 15:24:51','yyyy-mm-dd hh24:mi:ss'), TO_DATE('2020-06-13 15:24:51','yyyy-mm-dd hh24:mi:ss'), NULL);

```

同步至Kafka Topic中JSON数据

```JSON
{
  "table": "ITCAST.tbl_company",
  "op_type": "I",
  "op_ts": "2021-02-03 04:05:40.000511",
  "current_ts": "2021-02-03T04:05:54.920000",
  "pos": "00000000150000001245",
  "after": {
    "id": 11,
    "company_name": "广州传智速递邮箱公司",
    "city_id": 440100,
    "company_number": null,
    "company_addr": "广州校区",
    "company_addr_gis": "117.28177895734918_31.842711680531399",
    "company_tel": null,
    "is_sub_company": 1,
    "state": 1,
    "cdt": "2020-06-13 15:24:51",
    "udt": "2020-06-13 15:24:51",
    "remark": null
  }
}
```



```SQL
-- 更新数据UPDATE
UPDATE ITCAST."tbl_company" SET "company_name"='广州传智速递有限公司-1' WHERE "id"=11;

```

同步至Kafka Topic中JSON数据

```JSON
{
  "table": "ITCAST.tbl_company",
  "op_type": "U",
  "op_ts": "2021-02-03 04:08:20.000766",
  "current_ts": "2021-02-03T04:08:30.894000",
  "pos": "00000000150000001981",
  "before": {
    "id": 11,
    "company_name": "广州传智速递邮箱公司",
    "city_id": 440100,
    "company_number": null,
    "company_addr": "广州校区",
    "company_addr_gis": "117.28177895734918_31.842711680531399",
    "company_tel": null,
    "is_sub_company": 1,
    "state": 1,
    "cdt": "2020-06-13 15:24:51",
    "udt": "2020-06-13 15:24:51",
    "remark": null
  },
  "after": {
    "id": 11,
    "company_name": "深圳传智速递邮箱公司",
    "city_id": 440100,
    "company_number": null,
    "company_addr": "广州校区",
    "company_addr_gis": "117.28177895734918_31.842711680531399",
    "company_tel": null,
    "is_sub_company": 1,
    "state": 1,
    "cdt": "2020-06-13 15:24:51",
    "udt": "2020-06-13 15:24:51",
    "remark": null
  }
}
```



```SQL
-- 删除数据DELETE
DELETE ITCAST."tbl_company" WHERE "id"=11;
```

同步至Kafka Topic中JSON数据

```JSON
{
  "table": "ITCAST.tbl_company",
  "op_type": "D",
  "op_ts": "2021-02-03 04:10:02.001275",
  "current_ts": "2021-02-03T04:10:13.617000",
  "pos": "00000000150000003036",
  "before": {
    "id": 11,
    "company_name": "深圳传智速递邮箱公司",
    "city_id": 440100,
    "company_number": null,
    "company_addr": "广州校区",
    "company_addr_gis": "117.28177895734918_31.842711680531399",
    "company_tel": null,
    "is_sub_company": 1,
    "state": 1,
    "cdt": "2020-06-13 15:24:51",
    "udt": "2020-06-13 15:24:51",
    "remark": null
  }
}
```



## 11-[理解]-Canal 数据同步之MySQL binlog日志

> 前面讲解Oracle数据库表的数据使用OGG进行实时增量同步之Kafka 消息对象，MySQL数据库表的数据使用Canal（属于阿里巴巴开源框架，在国内被众多大中型公司使用，尤其Canal-1.1.x版本功能更加强大）。

![1612326012096](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612326012096.png)

> 所以，当使用Canal同步MySQL数据库表的数据时，需要首先开启MySQL binlog日志功能。

![1612326070489](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612326070489.png)

> 如何开启MySQL binlog日志功能：

![1612326242576](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612326242576.png)

![1612326262266](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612326262266.png)

## 12-[理解]-Canal 数据同步之功能及工作原理

> Canal [kə'næl]，用途是`基于 MySQL 数据库增量日志解析`，提供增量数据订阅和消费。
>
> 官网：https://github.com/alibaba/canal

![1612334014005](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612334014005.png)

![1612334177209](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612334177209.png)

> MySQL 主备复制原理：

![1612334485517](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612334485517.png)

> (1) MySQL master 将数据变更写入二进制日志( binary log, 其中记录叫做二进制日志事件binary
> log events，可以通过 show binlog events 进行查看)
> (2) MySQL slave 将 master 的 binary log events 拷贝到它的中继日志(relay log)
> (3) MySQL slave 重放 relay log 中事件，将数据变更反映它自己的数据

![1612334597112](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612334597112.png)

> ​			基于上述原理，==Canal 框架，将自己伪装成MySQL数据库Slave（小弟），发送dump协议，获取binlog日志，进行处理，将数据同步到目标端==。

![1612334674650](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612334674650.png)

> 1．canal 模拟 MySQL slave 的交互协议，伪装自己为 MySQL slave ，向 MySQL master 发
> 送dump 协议
> 2．MySQL master 收到 dump 请求，开始推送 binary log 给 slave (即 canal )
> 3．canal 解析 binary log 对象(原始为 byte 流)

![1612334913065](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612334913065.png)

> 重要版本更新说明：

![1612335037449](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612335037449.png)



## 13-[理解]-Canal 数据同步之Canal 架构

> - Canal Server 代表一个 Canal 运行实例，对应于一个 JVM；
> - Instance 对应于一个数据队列 （1个 Canal Server 对应 1..n 个 Instance)

![1612335387028](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612335387028.png)

> ​		EventParser在向MySQL发送dump命令之前会先从Log Position中获取上次解析成功的位置(如果是第一次启动，则获取初始指定位置或者当前数据段binlog位点)。

![1612335671638](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612335671638.png)

> ​		MySQL接受到dump命令后，由EventParser从MySQL上pull binlog数据进行解析并传递给EventSink(传递给EventSink模块进行数据存储，是一个阻塞操作，直到存储成功 )，传送成功之后更新Log Position。

![1612335817705](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612335817705.png)

## 14-[掌握]-Canal 数据同步之Docker 安装部署

> ​		Canal的好处在于对业务代码没有侵入，因为是基于监听binlog日志去进行同步数据的。实时性也能做到准实时，其实是很多企业一种比较常见的数据同步的方案。

![1612336004227](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612336004227.png)

> Canal 1.1.0开始支持Docker 容器部署：https://github.com/alibaba/canal/wiki/Docker-QuickStart

![1612336127908](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612336127908.png)

```ini
# 1. 拉取镜像
docker pull canal/canal-server:v1.1.2

# 2.创建容器
docker run -d --name canal-server \
-e canal.instance.master.address=192.168.88.10:3306 \
-e canal.instance.dbUsername=root \
-e canal.instance.dbPassword=123456 \
-p 11111:11111 \
-d canal/canal-server:v1.1.2

# 3. 进入容器
docker exec -it canal-server bash

# 4. 启动CanalServer
/home/admin/canal-server/bin/startup.sh
/home/admin/canal-server/bin/restart.sh
```

> [如果需要配置Canal Server，配置如下两个地方]()
>
> - 1）、CanalServer服务配置文件：`canal.properties`

![1612336982882](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612336982882.png)

> - 2）、具体Canal Instance实例配置：`instance.properties` 

![1612337033877](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612337033877.png)

> 当配置完成以后，重启CanalServer即可。



## 15-[理解]-Canal 数据同步之CRM数据同步Kafka

> 测试Canal实时同步数据到Kafka 消息队列中，如下操作：
>
> - 1）、启动MySQL数据库
>
>   ```ini
>   
>   使用VMWare 启动node1.itcast.cn虚拟机，使用root用户（密码123456）登录
>   1) 启动MySQL数据库
>   	# 查看容器
>   	[root@node1 ~]# docker ps -a
>   	8b5cd2152ed9        mysql:5.7     0.0.0.0:3306->3306/tcp   mysql	
>   	
>   	# 启动容器
>   	[root@node1 ~]# docker start mysql
>   	myoracle
>   	
>   	# 容器状态
>   	[root@node1 ~]# docker ps
>   	8b5cd2152ed9        mysql:5.7   Up 6 minutes        0.0.0.0:3306->3306/tcp   mysql	
>   ```
>
> - 2）、启动Kafka和Zookeeper服务组件
>
>   - node2.itcast.cn启动CM服务，在界面UI上启动Zk服务和Kafka服务
>  
>       ![1612337231893](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612337231893.png)
>
>     [启动完成Kafka服务以后，创建Topic：crm，可以使用KafkaTool工具]()
>
> - 3）、启动CanalServer服务
>
>   ```ini
>   2) 启动CanalServer服务
>   	# 查看容器
>   	[root@node1 ~]# docker ps -a
>   	28888fad98c9        canal/canal-server:v1.1.2        0.0.0.0:11111->11111/tcp   canal-server
>   	
>   	# 启动容器
>   	[root@node1 ~]# docker start canal-server
>   	myoracle
>   	
>   	# 容器状态
>   	[root@node1 ~]# docker ps
>   	28888fad98c9        canal/canal-server:v1.1.2       Up 2 minutes  0.0.0.0:11111->11111/tcp   canal-server	
>   	
>   	# 进入容器
>   	[root@node1 ~]# docker exec -it canal-server /bin/bash
>   	[root@28888fad98c9 admin]# 
>   	
>   	# 进入CanalServer启动脚本目录
>   	[root@28888fad98c9 admin]# cd canal-server/bin/
>   	
>   	# 重启CanalServer服务
>   	[root@28888fad98c9 bin]# ./restart.sh 
>   	
>   	# 退出容器
>   	[root@28888fad98c9 bin]# exit
>   
>   ```
>
> - 4）、使用DBeave 工具远程连接MySQL数据库，针对CRM系统对应数据库中某张表数据进行CUD操作



```SQL
-- 插入数据INSERT
INSERT INTO `crm_address` VALUES ('10001', '葛秋红', null, '17*******47', '恒大影城南侧小金庄', '130903', null, '2020-02-02 18:51:39', '2020-02-02 18:51:39', null);


```

插入数据到MySQL数据库表，Kafka中数据：

```JSON
{
  "data": [
    {
      "id": "10001",
      "name": "葛秋红",
      "tel": null,
      "mobile": "17*******47",
      "detail_addr": "恒大影城南侧小金庄",
      "area_id": "130903",
      "gis_addr": null,
      "cdt": "2020-02-02 18:51:39",
      "udt": "2020-02-02 18:51:39",
      "remark": null
    }
  ],
  "database": "crm",
  "es": 1612337462000,
  "id": 2,
  "isDdl": false,
  "mysqlType": {
    "id": "bigint(20)",
    "name": "varchar(50)",
    "tel": "varchar(20)",
    "mobile": "varchar(20)",
    "detail_addr": "varchar(100)",
    "area_id": "bigint(20)",
    "gis_addr": "varchar(20)",
    "cdt": "datetime",
    "udt": "datetime",
    "remark": "varchar(100)"
  },
  "old": null,
  "sql": "",
  "sqlType": {
    "id": -5,
    "name": 12,
    "tel": 12,
    "mobile": 12,
    "detail_addr": 12,
    "area_id": -5,
    "gis_addr": 12,
    "cdt": 93,
    "udt": 93,
    "remark": 12
  },
  "table": "crm_address",
  "ts": 1612337462678,
  "type": "INSERT"
}
```

更新MySQL数据库表的数据，查看Kafka中数据格式：

```JSON
{
  "data": [
    {
      "id": "1",
      "name": "杨玲玉",
      "tel": null,
      "mobile": "13*******47",
      "detail_addr": "虹口区曲阳路500号202室",
      "area_id": "310109",
      "gis_addr": null,
      "cdt": "2020-02-02 18:51:00",
      "udt": "2020-02-02 18:51:00",
      "remark": null
    }
  ],
  "database": "crm",
  "es": 1612337560000,
  "id": 3,
  "isDdl": false,
  "mysqlType": {
    "id": "bigint(20)",
    "name": "varchar(50)",
    "tel": "varchar(20)",
    "mobile": "varchar(20)",
    "detail_addr": "varchar(100)",
    "area_id": "bigint(20)",
    "gis_addr": "varchar(20)",
    "cdt": "datetime",
    "udt": "datetime",
    "remark": "varchar(100)"
  },
  "old": [
    {
      "name": "杨玉玲"
    }
  ],
  "sql": "",
  "sqlType": {
    "id": -5,
    "name": 12,
    "tel": 12,
    "mobile": 12,
    "detail_addr": 12,
    "area_id": -5,
    "gis_addr": 12,
    "cdt": 93,
    "udt": 93,
    "remark": 12
  },
  "table": "crm_address",
  "ts": 1612337560347,
  "type": "UPDATE"
}
```

删除MySQL数据库表的数据，查看Kafka中数据：

```JSON
{
  "data": [
    {
      "id": "10001",
      "name": "葛秋红",
      "tel": null,
      "mobile": "17*******47",
      "detail_addr": "恒大影城南侧小金庄",
      "area_id": "130903",
      "gis_addr": null,
      "cdt": "2020-02-02 18:51:39",
      "udt": "2020-02-02 18:51:39",
      "remark": null
    }
  ],
  "database": " ",
  "es": 1612337624000,
  "id": 4,
  "isDdl": false,
  "mysqlType": {
    "id": "bigint(20)",
    "name": "varchar(50)",
    "tel": "varchar(20)",
    "mobile": "varchar(20)",
    "detail_addr": "varchar(100)",
    "area_id": "bigint(20)",
    "gis_addr": "varchar(20)",
    "cdt": "datetime",
    "udt": "datetime",
    "remark": "varchar(100)"
  },
  "old": null,
  "sql": "",
  "sqlType": {
    "id": -5,
    "name": 12,
    "tel": 12,
    "mobile": 12,
    "detail_addr": 12,
    "area_id": -5,
    "gis_addr": 12,
    "cdt": 93,
    "udt": 93,
    "remark": 12
  },
  "table": "crm_address",
  "ts": 1612337625476,
  "type": "DELETE"
}
```



## 16-[了解]-Canal 数据同步之集群高可用HA

> 使用Canal集群（高可用HA）实时增量同步MySQL表数据至Kafka消息队列，架构示意图如下：

![1612338707148](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612338707148.png)

> ​		构建Canal集群（高可用HA）时，至少需要启动两个Canal Server，其中一个Runing 状态，另外一个Standby状态，依赖Zookeeper集群（存储基本信息和监控与选举Running）。

![1612338755220](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612338755220.png)

> Canal 集群可以配置从多个MySQL数据库获取数据（获取binlog日志），将数据发送到Kafka消息队列。

![1612338926977](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612338926977.png)

> 从Canal 1.1.4版本开始提供集群高可用HA功能。



## 17-[掌握]-大数据服务器之CM安装架构及目录

> ​			在本物流项目中，大数据环境基本使用使用`CM安装CDH组件`，版本为：6.2.1，部署在单台服务器`node2.itcast.cn`。
>
> - 1）、首先安装CM 6.2.1（Cloudera Manager）
> - 2）、然后使用CM 界面安装CDH 6.2.1组件
>   - 登录CM访问页面：http://node2.itcast.cn:7180/cmf/login，用户名和密码：`admin/admin`

![1612339418383](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612339418383.png)

> ​		安装CM（配置信息）与CDH组件（Hive、Oozie和Hue等）时，需要依赖数据库，此处使用`MySQL数据库`，连接数据库用户名和密码：`root/Abcd1234.`

![1612339634276](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612339634276.png)

> 使用CM安装大数据组件CDH服务时，架构原理，理解掌握：
>
> - 1）、Cloudera将所有大数据组件放在一个目录，打包为文件：parcel
>
> - 2）、ClouderaManager属于主从架构，Server服务（主机点），所有Agent为（从节点）
>
> - 3）、使用CM安装CDH时，其实就是将parcel包解压到服务器目录：`/opt/cloudera/parceles/`
>
> - 4）、CM安装某个框架时，配置分为2个部分：
>
>   - 第一部分、Server 服务启动时读取配置信息
>     - 存储在MySQL数据库中
>   - 第二部分、Client客户端连接服务时，所需要配置信息
>     - `/etc/xx/conf`目录
>
>   ![1612340938539](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612340938539.png)

![1612340738839](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612340738839.png)



## 18-[掌握]-大数据服务器之CDH框架安装细节

> 当使用CM安装配置完成CDH 大数据组件框架以后，有一些注意事项，方便后续使用。

![1612341161516](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612341161516.png)

> - 1）、框架用户	
>
>   - [当使用ClouderaManager安装CDH各个框架以后，针对各个框架将以框架名称创建用户和用户组，启动各个框架服务组件时，使用对应用户进行启动]()
>
>   ![1612341322517](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612341322517.png)
>
>   - 举例说明：使用CM安装HDFS组件时，自动创建`hdfs`用户，安装NameNode和DataNode相关组件，并且启动NameNode和DataNode时，使用`hdfs`用户启动。
>
>   ![1612341380635](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612341380635.png)
>
>   ![1612341431603](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612341431603.png)



> - 2）、如果需要使用框架用户，比如hdfs用户，对HDFS文件进行操作，比如创建目录：
>
>   - [使用【sudo -u userName】方式切换用户，直接加上执行即可]()
>   - 针对HDFS文件系统来说，默认情况下，使用hdfs用户创建目录，权限时最大的
>
>   ![1612341587875](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612341587875.png)
>
>   - 查看HDFS文件系统上文件
>
>     ```ini
>     [root@node2 ~]# sudo -u hdfs hdfs dfs -ls /
>     ```
>
>     ![1612341725167](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612341725167.png)
>
>     ```ini
>     [root@node2 ~]# sudo -u hdfs hdfs dfs -mkdir -p /data01
>     [root@node2 ~]# 
>     [root@node2 ~]# sudo -u hdfs hdfs dfs -ls /
>     ```
>
>     ![1612341770289](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612341770289.png)



> - 3）、使用CM安装数据，Client端配置文件目录
>
>   - 客户端配置文件目录：`/etc/xx/conf`
>
>   ![1612341837672](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612341837672.png)



> - 4）、启动服务组件日志目录：`/var/log/xx`
>
> ![1612341917382](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612341917382.png)
>
> 比如进入kafka安装日志目录：
>
> ![1612341946029](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612341946029.png)



> 面试题：[大数据集群使用CM部署安装，如果Cloudera Manager宕机了，整个集群运行是否正常？？？能自己启动组件服务？？？]()
>
> ```ini
> [root@node2 hadoop]# sudo -u hdfs hdfs --daemon stop namenode 
> [root@node2 hadoop]# jps
> 12017 DataNode
> 2515 -- process information unavailable
> 2197 Kafka
> 2517 -- process information unavailable
> 2203 QuorumPeerMain
> 15548 Jps
> 1229 Main
> ```
>
> 



```
1）、SparkSQL-离线分析
	大数据教程电信信号强度诊断项目实战
	https://www.bilibili.com/video/BV1da4y1Y7Zg
	
2）、Flink&Spark-实时分析
	黑马程序员大数据企业级实战项目 千亿级数仓实战
	https://www.bilibili.com/video/BV1bv411x7vr
```





