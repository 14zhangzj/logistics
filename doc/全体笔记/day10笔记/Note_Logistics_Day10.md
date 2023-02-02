---
stypora-copy-images-to: img
typora-root-url: ./
---



# Logistics_Day10：ClickHouse 快速入门



![1614160921553](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614160921553.png)



## 01-[复习]-上次课程内容回顾 

> 上次课程主要讲解：离线报表分析（2个主题）和实时ETL存储Es（快递物流信息查询）。
>
> - 1）、离线报表分析：车辆主题和客户主题
>
>   - 第一个主题：车辆主题报表开发，分为网点车辆报表分析和仓库车辆报表分析
>
>     - 无论是网点车辆还是仓库车辆报表分析，都是按照数仓分层架构管理数据和开发应用程序。
>     - 三层架构：`ODS层（结构化流实时存储） -> DWD层（数据明细层，拉宽操作） -> DWS层（数据服务层，指标计算）`
>     - 每个主题报表开发需要开发2个Spark应用程序：DWD拉宽和DWS指标计算。
>
>   - 第二个主题：客户主题
>
>     - 主要统计每个客户下单情况统计，针对客户来说，可以划分客户类型（客户活跃度）
>     - 指标，有时候也称为标签，用户统计分析
>
>     ![1614300944286](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614300944286.png)
>
>     - 此主题报表开发，首先事实表拉宽操作，然后时指标计算。
>
>   - 开发Shell 脚本，使用Azkaban进行调度执行，传递参数，运行不同主题报表分析
>
> - 2）、即席查询Impala，集成Kudu，分析数据，基于用户相关业务数据，给用户打上标签
>
>   - 基于Hue集成Impala，提供可视化SQL界面，分析数据
>
>   ![1614301093902](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614301093902.png)
>
> - 3）、实时ETL存储Elasticsearch索引
>
>   - [需要：针对物流快递行业来说，给用户提供查询快递物流信息功能，将**`快递单数据和运单数据`**存储Es索引中，语句快递单号或运单号查询物流信息，]()
>
>   ![1614301207901](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614301207901.png)
>
>   当业务数据存储Es索引以后，最好提供服务接口，如下图所示：
>
>   ![1614301274529](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614301274529.png)
>
>   编写结构化流应用程序，实时将数据ETL存储到Es索引中。
>
>   ![1614301345379](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614301345379.png)
>
>   [其中可以优化上次代码，先过滤获取2个业务表数据，在进行ETL转换，代码如下所示：]()
>
>   ![1614301708220](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614301708220.png)



## 02-[了解]-第10天：课程内容提纲 

> ​		首先回顾一下，整个物流项目数据流转和分析业务，其中需要进行OLAP分析，实时查询数据，此处使用数据库：`ClickHouse`，简称`CK`。

![1614302125656](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614302125656.png)

> 第8章课程学习目标，讲解3.5天，内容比较关键。

![1614302312199](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614302312199.png)

> 今日主要带大家了解ClickHouse基础知识，可以ClickHouse入门：
>
> - 1）、OALP分析技术框架选项和对比
> - 2）、ClickHouse 概述与案例场景
> - 3）、ClickHouse快速入门：安装部署和体验查询分析（重点）
> - 4）、ClickHouse SQL语法和函数
> - 5）、ClickHouse支持数据类型（重点）
> - 6）、ClickHouse 存储引擎（重点）



## 03-[了解]-实时OLAP分析之技术选型

> ​	首先了解物流项目为什么需要OLAP分析？？？[业务背景知识]()

![1614303708882](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614303708882.png)

> 在物流项目中，要实现大屏展示，秒级别数据查询分析，使用OLAP数据库：ClickHouse
>

![1614303973787](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614303973787.png)

> 在大数据领域中，OLAP分析型数据库有很多，不同数据库有不同特点，为什么选择CK呢？？比对其他框架

![1614304834921](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614304834921.png)



## 04-[理解]-实时OLAP分析之设计方案

> ​		客快物流项目来说，实时增量将业务数据存储至ClickHouse数据库表中，以便OLAP分析查询，比如：为实时大屏提供数据和提供数据接口服务。
>
> [实时将RDBMS表业务数据同步到ClickHouse数据库表中，CK对外提供OLAP分析查询。]()

![1614305384716](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614305384716.png)

> ​			当将数据存储到ClickHouse表中以后，对外部提供查询时，既可以直接通过JDBC方式查询数据，也可以提供服务接口查询数据。
>

![1614305650291](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614305650291.png)

> ​		开发服务接口（比如使用SpringCloud），按照业务需要，通过JDBC Client API查询CK表数据，以JSON格式返回数据。

![1614305803987](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614305803987.png)

> ​		针对到来说，每隔一定时间刷新页面，调度服务接口，到数据库进行查询，返回JSON结果数据，前端使用图表形式展示。





## 05-[了解]-ClickHouse 入门之概述及案例

> ClickHouse是一个`面向列`的`数据库`管理系统（DBMS），用于`在线分析`处理查询（OLAP）。
>
> - 官网：https://clickhouse.tech/
> - Logo 图标：
>
> ![ClickHouse logo](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/logo.svg)
>
> - 文档：https://clickhouse.tech/docs/en/，支持中文：https://clickhouse.tech/docs/zh/

![1614306056944](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614306056944.png)

> ClickHouse，近几年发展比较好OLAP分析框架：https://db-engines.com/en/ranking

![1614306208031](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614306208031.png)

> 简单的说ClickHouse作为分析型数据库，三大特点：一是跑分快， 二是功能多 ，三是文艺范。

![1614306621646](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614306621646.png)

> ClickHouse 优势，目前支持数据压缩算：`Lz4`和ZSTD算法，其中lz4算法比SNAPPY压缩算性能好很多。

![1614306652563](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614306652563.png)

> ClickHouse存储数据是面向列进行存储，类似ORC和Parquet及Kudu数据库存储数据方式。
>
> [ClickHouse 基准测试]()

![1614306921909](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614306921909.png)

> ClickHouse是近年来备受关注的开源列式数据库，主要用于数据分析（OLAP）领域。

![1614306992278](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614306992278.png)



## 06-[掌握]-ClickHouse 入门之安装部署

> ​	ClickHouse数据库安装部署启动，比较简单的，首先安装：

![1614308014068](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614308014068.png)

> 可以在线yum源方式安装（表较慢，需要下载依赖RPM包），建议离线安装（先下载PRM包，在手动安装）：
>
> - 1）、ClickHouse Server服务端，类似MySQL服务
> - 2）、ClickHouse Client 客户端，提供clickhouse-client 命令行，类似mysql命令行，操作数据库
>
> [如果采用离线安装RPM方式，命令：`rpm -ivh clickhouse-*.rpm`]()

> 如何进行在线安装ClickHouse数据库：
>
> - 1）、安装yum-utils工具包
>
>   [`yum install yum-utils -y`]()
>
> - 2）、添加ClickHouse的yum源
>
>   [`yum-config-manager --add-repo https://repo.yandex.ru/clickhouse/rpm/stable/x86_64`]()
>
> - 3）、安装ClickHouse的服务端和客户端
>
>   [`yum install -y clickhouse-server clickhouse-client --nogpgcheck`]()
>
> 采用YUM安装方式，ClickHouser安装完成以后目录存说明：
>
> - 默认的配置文件路径是：`/etc/clickhouse-server/`
> - 默认的日志文件路径是：`/var/log/clickhouse-server/`

![1614308460355](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614308460355.png)

> - 服务配置：`config.xml`，默认情况下ClickHouse提供TCP访问端口号为`9000`，与CM端口重复，所以需要修改端口，可以改为`9999`

![1614308540698](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614308540698.png)

> 修改ClickHouse端口号为：9999，截图如下

![1614308680550](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614308680550.png)

> - 用户配置：`users.xml`，默认情况下用户名为default，没有设置密码，但是实际项目中需要配置用户和密码，此物料项目来说，用户名为`root`，密码 为`123456`

![1614308757844](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614308757844.png)

> 启动ClickHouse服务Server：`systemctl start clickhouse-server`

![1614308830809](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614308830809.png)

> CK服务启动完成以后，使用客户端连接：`clickhouse-client -m --host node2.itcast.cn --port 9999 --user root --password 123456`

```ini
[root@node2 ~]# clickhouse-client -m --host node2.itcast.cn --port 9999 --user root --password 123456
ClickHouse client version 20.4.2.9 (official build).
Connecting to node2.itcast.cn:9999 as user root.
Connected to ClickHouse server version 20.4.2 revision 54434.

node2.itcast.cn :) show databases ;

SHOW DATABASES

┌─name───────────────────────────┐
│ _temporary_and_external_tables │
│ db_test                        │
│ default                        │
│ logistics                      │
│ system                         │
└────────────────────────────────┘

5 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) select count(1) as cnt from db_test.ontime ;

SELECT count(1) AS cnt
FROM db_test.ontime

┌──────cnt─┐
│ 12202581 │
└──────────┘

1 rows in set. Elapsed: 0.137 sec. 
```



> 在实际项目中，ClickHouse更多的是进行集群安装部署（Cluster Deploy），依赖于Zookeeper协作服务框架。

![img](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/734555-20180723114647876-24433769.png)

> 文档：https://www.cnblogs.com/freeweb/p/9352947.html



## 07-[掌握]-ClickHouse 入门之命令行使用

> ​		ClickHouse安装包中提供了clickhouse-client工具，这个客户端在运行shell环境中，使用TCP方式连接clickhouse-server服务。
>
> [在使用clickhouse-client连接CK服务时，有一些参数：]()

![1614309157040](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614309157040.png)

![1614309176656](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614309176656.png)

> 上述四个参数，需要进行传递，才能成功连接到ClickHouseServer服务。

> 连接ClickHouse数据库服务时，提供两种方式：
>
> - 1）、方式一、批量执行方式，使用--query或-q指定查询语句
>
>   ```SQL
>    clickhouse-client --host node2.itcast.cn --port 9999 --user root --password 123456 -q "select count(1) as cnt from db_test.ontime"
>   ```
>
> - 2）、方式二：交互式查询，通过加上-m参数，表示编写SQL语句支持多行模式
>
>   ```SQL
>   clickhouse-client -m --host node2.itcast.cn --port 9999 --user root --password 123456 
>   ```
>
>   

![1614309451517](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614309451517.png)



## 08-[了解]-ClickHouse 入门之可视化界面

> ​		ClickHouse数据库也支持可视化界面工具连接，比如IDEA和DBeave都可以。
>
> - [DBeaver](https://dbeaver.io/) 具有ClickHouse支持的通用桌面数据库客户端。
>
>   - 底层采用JDBC Client驱动方式连接，需要驱动包
>
>   ![1614309971531](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614309971531.png)

![1614310114843](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614310114843.png)



> - [DataGrip](https://www.jetbrains.com/datagrip/) 是JetBrains的数据库IDE，专门支持ClickHouse。 它还嵌入到其他基于IntelliJ的工具中：PyCharm，IntelliJ `IDEA`，GoLand，PhpStorm等。

![1614310208028](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614310208028.png)



## 09-[掌握]-ClickHouse 入门之快速体验【导入数据】

> ​	快速体验，ClickHouse官方提供很多数据集案例，使用ontime数据集：航空飞行数据。
>
> - 1）、编写下载航班数据脚本
>
>   ```shell
>   vim clickhouse-example-data-download.sh
>   
>   for s in `seq 2017 2020`
>   do
>   for m in `seq 1 12`
>   do
>   wget https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${s}_${m}.zip
>   done
>   done
>   
>   chmod +x clickhouse-example-data-download.sh
>   
>   ./clickhouse-example-data-download.sh
>   ```
>
> - 2）、创建表
>
>   ```ini
>   clickhouse-client -m --host node2.itcast.cn --port 9999 --user root --password 123456
>   ```
>
>   创建表DDL语句：
>
>   ```SQL
>   CREATE TABLE `ontime` (
>     `Year` UInt16,
>     `Quarter` UInt8,
>     `Month` UInt8,
>     `DayofMonth` UInt8,
>     `DayOfWeek` UInt8,
>     `FlightDate` Date,
>     `UniqueCarrier` FixedString(7),
>     `AirlineID` Int32,
>     `Carrier` FixedString(2),
>     `TailNum` String,
>     `FlightNum` String,
>     `OriginAirportID` Int32,
>     `OriginAirportSeqID` Int32,
>     `OriginCityMarketID` Int32,
>     `Origin` FixedString(5),
>     `OriginCityName` String,
>     `OriginState` FixedString(2),
>     `OriginStateFips` String,
>     `OriginStateName` String,
>     `OriginWac` Int32,
>     `DestAirportID` Int32,
>     `DestAirportSeqID` Int32,
>     `DestCityMarketID` Int32,
>     `Dest` FixedString(5),
>     `DestCityName` String,
>     `DestState` FixedString(2),
>     `DestStateFips` String,
>     `DestStateName` String,
>     `DestWac` Int32,
>     `CRSDepTime` Int32,
>     `DepTime` Int32,
>     `DepDelay` Int32,
>     `DepDelayMinutes` Int32,
>     `DepDel15` Int32,
>     `DepartureDelayGroups` String,
>     `DepTimeBlk` String,
>     `TaxiOut` Int32,
>     `WheelsOff` Int32,
>     `WheelsOn` Int32,
>     `TaxiIn` Int32,
>     `CRSArrTime` Int32,
>     `ArrTime` Int32,
>     `ArrDelay` Int32,
>     `ArrDelayMinutes` Int32,
>     `ArrDel15` Int32,
>     `ArrivalDelayGroups` Int32,
>     `ArrTimeBlk` String,
>     `Cancelled` UInt8,
>     `CancellationCode` FixedString(1),
>     `Diverted` UInt8,
>     `CRSElapsedTime` Int32,
>     `ActualElapsedTime` Int32,
>     `AirTime` Int32,
>     `Flights` Int32,
>     `Distance` Int32,
>     `DistanceGroup` UInt8,
>     `CarrierDelay` Int32,
>     `WeatherDelay` Int32,
>     `NASDelay` Int32,
>     `SecurityDelay` Int32,
>     `LateAircraftDelay` Int32,
>     `FirstDepTime` String,
>     `TotalAddGTime` String,
>     `LongestAddGTime` String,
>     `DivAirportLandings` String,
>     `DivReachedDest` String,
>     `DivActualElapsedTime` String,
>     `DivArrDelay` String,
>     `DivDistance` String,
>     `Div1Airport` String,
>     `Div1AirportID` Int32,
>     `Div1AirportSeqID` Int32,
>     `Div1WheelsOn` String,
>     `Div1TotalGTime` String,
>     `Div1LongestGTime` String,
>     `Div1WheelsOff` String,
>     `Div1TailNum` String,
>     `Div2Airport` String,
>     `Div2AirportID` Int32,
>     `Div2AirportSeqID` Int32,
>     `Div2WheelsOn` String,
>     `Div2TotalGTime` String,
>     `Div2LongestGTime` String,
>     `Div2WheelsOff` String,
>     `Div2TailNum` String,
>     `Div3Airport` String,
>     `Div3AirportID` Int32,
>     `Div3AirportSeqID` Int32,
>     `Div3WheelsOn` String,
>     `Div3TotalGTime` String,
>     `Div3LongestGTime` String,
>     `Div3WheelsOff` String,
>     `Div3TailNum` String,
>     `Div4Airport` String,
>     `Div4AirportID` Int32,
>     `Div4AirportSeqID` Int32,
>     `Div4WheelsOn` String,
>     `Div4TotalGTime` String,
>     `Div4LongestGTime` String,
>     `Div4WheelsOff` String,
>     `Div4TailNum` String,
>     `Div5Airport` String,
>     `Div5AirportID` Int32,
>     `Div5AirportSeqID` Int32,
>     `Div5WheelsOn` String,
>     `Div5TotalGTime` String,
>     `Div5LongestGTime` String,
>     `Div5WheelsOff` String,
>     `Div5TailNum` String
>   ) ENGINE = MergeTree
>   PARTITION BY Year
>   ORDER BY (Carrier, FlightDate)
>   SETTINGS index_granularity = 8192;
>   ```
>
> - 3）、导入数据
>
>   - 编写脚本文件，使用clickhouse-client客户端导入数据
>
>     ```shell
>     vim import.sh
>     
>     for i in *.zip; do echo $i; unzip -cq $i '*.csv' | sed 's/\.00//g' | clickhouse-client --host node2.itcast.cn --port 9999 --user root --password 123456 --query="INSERT INTO default.ontime FORMAT CSVWithNames"; done
>     
>     chmod +x import.sh
>     
>     ./import.sh
>     ```
>
>     要求导入数据脚本与数据文件zip同一个目录下。

![1614311655709](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614311655709.png)



## 10-[掌握]-ClickHouse 入门之快速体验【简单查询】

> ​		前面已经将航空飞行数据导入至ClickHouse表：ontime，编写SQL语句进行查询
>
> 文档：https://clickhouse.tech/docs/zh/getting-started/example-datasets/ontime/#cha-xun

```ini
SELECT 
    DayOfWeek, 
    count(1) AS c
FROM ontime
WHERE (Year >= 2017) AND (Year <= 2020)
GROUP BY DayOfWeek
ORDER BY c DESC ；
```

![1614311861952](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614311861952.png)

> 为什么这么快？？？
>
> - 1）、列式存储数据，上面查询，仅仅获取两列数据：Year和DayOfWeek
> - 2）、基于内存
> - 3）、`存储引擎：MergeTreee`



```SQL
-- 添加过滤条件，再次进行分组、聚合和排序
SELECT 
    DayOfWeek, 
    count(1) AS c
FROM ontime
WHERE (DepDelay > 10) AND (Year >= 2017) AND (Year <= 2020)
GROUP BY DayOfWeek
ORDER BY c DESC
```

![1614312028267](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614312028267.png)



```SQL
SELECT
   min(Year), max(Year), Carrier, count(*) AS cnt,
   sum(ArrDelayMinutes>30) AS flights_delayed,
   round(sum(ArrDelayMinutes>30)/count(*),2) AS rate
FROM ontime
WHERE
   DayOfWeek NOT IN (6,7) AND OriginState NOT IN ('AK', 'HI', 'PR', 'VI')
   AND DestState NOT IN ('AK', 'HI', 'PR', 'VI')
   AND FlightDate < '2020-01-01'
GROUP by Carrier
HAVING cnt>100000 and max(Year)>1990
ORDER by rate DESC
LIMIT 1000;
```

![1614312485943](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614312485943.png)

## 11-[掌握]-CK SQL 语法之常用 SQL 命令

> ​		ClickHouse数据库，其中支持DDL和DML语句，基本上与MySQL数据库类型，常见SQL命令。

![1614312660304](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614312660304.png)

> 创建表的基本语法：
>
> - 1）、表的类型分为两种：临时表temporary和持久化表
>
> - 2）、创建表示，建议写上数据库名称
>
> - 3）、如果使用CK集群，创建表时，在表的名称后面加上：`ON CLUSTER clusterId`
>
> - 4）、每个数据表创建时，需要指定存储引擎，非常关键属性，决定业务查询性能
>
>   `engine = EngineName(parameters);`

```SQL
create [temporary] table [if not exists] dbName.tableName [ON CLUSTER cluster] (
fieldName dataType
) engine = EngineName(parameters);
```

> 视图创建：对表数据投影（Project），选择部分字段或者聚合操作得到数据，视图View中数据仅仅只能读取

```SCALA
create view view_name as select ...
```



## 12-[掌握]-CK SQL 语法之SELECT、INSERT和ALTER

> ​	ClickHouse数据库支持SQL语法只有三种：查询SELECT、插入INSERT和修改ALTER（实现UPDATE和DELETE）
>
> - 1）、==SELECT 语法==：查询检索数据，类似MySQL数据库中语法

```SQL
SELECT [DISTINCT] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] ANY|ALL INNER|LEFT JOIN (subquery)|table USING columns_list
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH TOTALS]
[HAVING expr]
[ORDER BY expr_list]
[LIMIT [n, ]m]
[UNION ALL ...]
[INTO OUTFILE filename]
[FORMAT format]
[LIMIT n BY columns]
```

> ARRAY JOIN子句，范例演示如下：

```ini
node2.itcast.cn :) create table tbl_test_array_join(str String, arr Array(Int8)) engine=Memory;

CREATE TABLE tbl_test_array_join
(
    `str` String, 
    `arr` Array(Int8)
)
ENGINE = Memory

Ok.

0 rows in set. Elapsed: 0.002 sec. 

node2.itcast.cn :) show tables ;

SHOW TABLES

┌─name────────────────┐
│ tbl_test_array_join │
└─────────────────────┘

1 rows in set. Elapsed: 0.010 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) insert into tbl_test_array_join(str,arr) values('a',[1,3,5]),('b',[2,4,6]);

INSERT INTO tbl_test_array_join (str, arr) VALUES

Ok.

2 rows in set. Elapsed: 0.006 sec. 

node2.itcast.cn :) select str,arr,arrItem from tbl_test_array_join ARRAY JOIN arr as arrItem;

SELECT 
    str, 
    arr, 
    arrItem
FROM tbl_test_array_join
ARRAY JOIN arr AS arrItem

┌─str─┬─arr─────┬─arrItem─┐
│ a   │ [1,3,5] │       1 │
│ a   │ [1,3,5] │       3 │
│ a   │ [1,3,5] │       5 │
│ b   │ [2,4,6] │       2 │
│ b   │ [2,4,6] │       4 │
│ b   │ [2,4,6] │       6 │
└─────┴─────────┴─────────┘

6 rows in set. Elapsed: 0.005 sec. 

```



> ClickHouse中完整insert的主要用于向系统中添加数据，有如下几种方式：
>
> - 1）、语法一：通用插入数据
>
>   `INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23)...`
>
> - 2）、语法二：
>
>   `INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set`
>
> - 3）、语法三：子查询插入数据
>
>   `INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...`



> ALTER 语法：用于对ClickHouse表进行添加列、删除列和修改列以及数据的更新和删除
>
> [ClickHouse中的ALTER只支持MergeTree系列，Merge和Distributed引擎的表]()

![1614322835301](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614322835301.png)

```SQL
-- ALTER 语法
CREATE TABLE mt_table
(
    `date` Date, 
    `id` UInt8, 
    `name` String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(date)
ORDER BY id
SETTINGS index_granularity = 8192 ;

insert into mt_table values ('2020-09-15', 1, 'zhangsan');
insert into mt_table values ('2020-09-15', 2, 'lisi');
insert into mt_table values ('2020-09-15', 3, 'wangwu');

-- 添加列
alter table mt_table add column age UInt8 ;
-- 查看表结构
desc mt_table ;
-- 查看数据
select * from mt_table ;


-- 修改列
alter table mt_table modify column age UInt16 ;

-- 删除列
alter table mt_table drop column age ; 
```



## 13-[掌握]-CK SQL 语法之UPDATE和DELETE

> ​		接下来使用ALTER语法，完成CK数据库表中数据的更新和删除，变向实现。
>
> [从使用场景来说，Clickhouse是个分析型数据库。这种场景下，数据一般是不变的，因此Clickhouse对update、delete的支持是比较弱的，实际上并不支持标准的update、delete操作。]()

![1614323264033](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614323264033.png)

> 无论是删除还是更新，使用ALTER语法时，需要指定WHERE条件，只有满足条件，才能进进行更新与删除。

```SQL
-- upate和delete，基于alter语法实现

CREATE TABLE tbl_test_users
(
    `id` UInt64, 
    `email` String, 
    `username` String, 
    `gender` UInt8, 
    `birthday` Date, 
    `mobile` FixedString(13), 
    `pwd` String, 
    `regDT` DateTime, 
    `lastLoginDT` DateTime, 
    `lastLoginIP` String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(regDT)
ORDER BY id
SETTINGS index_granularity = 8192 ;



insert into tbl_test_users(id, email, username, gender, birthday, mobile, pwd, regDT, lastLoginDT,lastLoginIP) values (1,'wcfr817e@yeah.net','督咏',2,'1992-05-31','13306834911','7f930f90eb6604e837db06908cc95149','2008-08-06 11:48:12','2015-05-08 10:51:41','106.83.54.165'),(2,'xuwcbev9y@ask.com','上磊',1,'1983-10-11','15302753472','7f930f90eb6604e837db06908cc95149','2008-08-10 05:37:32','2014-07-28 23:43:04','121.77.119.233'),(3,'mgaqfew@126.com','涂康',1,'1970-11-22','15200570030','96802a851b4a7295fb09122b9aa79c18','2008-08-10 11:37:55','2014-07-22 23:45:47','171.12.206.122'),(4,'b7zthcdg@163.net','金俊振',1,'2002-02-10','15207308903','96802a851b4a7295fb09122b9aa79c18','2008-08-10 14:47:09','2013-12-26 15:55:02','61.235.143.92'),(5,'ezrvy0p@163.net','阴福',1,'1987-09-01','13005861359','96802a851b4a7295fb09122b9aa79c18','2008-08-12 21:58:11','2013-12-26 15:52:33','182.81.200.32');


ALTER TABLE tbl_test_users UPDATE username='张三' WHERE id=1;


ALTER TABLE tbl_test_users DELETE WHERE id=1;
```



## 14-[掌握]-ClickHouse 入门之SQL 函数

> ​		在ClickHouse数据库中支持一些函数：
>
> 文档：https://clickhouse.tech/docs/zh/sql-reference/functions/

- 1）、类型检测函数：`toTypeName`

```SQL
SELECT 
    toTypeName(toDate('2019-12-12 ')) AS dateType, 
    toTypeName(toDateTime('2019-1 2-12 12:12:12')) AS dateTimeType
    
SELECT toTypeName([1, 3, 5])
```



- 2）、时间函数

```ini
node2.itcast.cn :) select now() as curDT,toYYYYMM(curDT),toYYYYMMDD(curDT),toYYYYMMDDhhmmss(curDT);

SELECT 
    now() AS curDT, 
    toYYYYMM(curDT), 
    toYYYYMMDD(curDT), 
    toYYYYMMDDhhmmss(curDT)

┌───────────────curDT─┬─toYYYYMM(now())─┬─toYYYYMMDD(now())─┬─toYYYYMMDDhhmmss(now())─┐
│ 2021-02-26 15:25:21 │          202102 │          20210226 │          20210226152521 │
└─────────────────────┴─────────────────┴───────────────────┴─────────────────────────┘

1 rows in set. Elapsed: 0.006 sec. 

node2.itcast.cn :) select toDateTime('2019-12-16 14:27:30') as curDT;

SELECT toDateTime('2019-12-16 14:27:30') AS curDT

┌───────────────curDT─┐
│ 2019-12-16 14:27:30 │
└─────────────────────┘

1 rows in set. Elapsed: 0.002 sec. 

node2.itcast.cn :) select toDate('2019-12-12') as curDT;

SELECT toDate('2019-12-12') AS curDT

┌──────curDT─┐
│ 2019-12-12 │
└────────────┘

1 rows in set. Elapsed: 0.003 sec. 
```



## 15-[掌握]-CK 数据类型之数值类型

> 任何一门编程语言或者数据库，都要提及数据类型，绝大多数数据类型基本类似。
>
> [ClickHouse与常用的关系型数据库MySQL或Oracle的数据类型类似，提供了丰富的数据类型支持。]()

![1614325898744](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614325898744.png)

> ClickHouse支持Int和Uint两种固定长度的整型，Int类型是符号整型，Uint类型是无符号整型。

![1614325973118](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614325973118.png)



> ClickHouse支持Float32和Float64两种浮点类型

```
node2.itcast.cn :) select 1-0.9
:-] ;

SELECT 1 - 0.9

┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘

1 rows in set. Elapsed: 0.016 sec. 
```



> ​		Decimal：大数类型，就是Java语言中BigDecimal类型，ClickHouse支持Decimal类型的有符号定点数，可在加、减和乘法运算过程中保持精度。
>
> [Decimal(P,S)，P参数指的是精度，有效范围：[1:38]，决定可以有多少个十进制数字（包括分数）；S参数指的是小数长度，有效范围：[0：P]，决定数字的小数部分中包含的小数位数。]()

![img](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/20171019171205378.png)

> 在Java中对Double类型数字进行相加时，需要转换为BigDecimal类型，再进行操作。

```scala
 /**
     * 加法运算
     * @param m1
     * @param m2
     * @return
     */
    public static double addDouble(double m1, double m2) {
        BigDecimal p1 = new BigDecimal(Double.toString(m1));
        BigDecimal p2 = new BigDecimal(Double.toString(m2));
        return p1.add(p2).doubleValue();
    }
```



## 16-[掌握]-CK 数据类型之字符及NULL类型

> ​			ClickHouse中的String类型没有编码的概念。字符串可以是任意的字节集，按它们原本的方式进行存储和输出。[若需存储文本，建议使用UTF-8编码。]()
>
> - 1）、String：可以任意长度的
> - 2）、FixedString(N)：固定长度 N 的字符串，N必须是严格的正自然数。



> 此外，在ClickHouse中支持UUID数据类型，[ClickHouse支持UUID类型（通用唯一标识符），该类型是一个16字节的数字，用于标识记录。]()
>



> cClickHouse数据类型中，没有布尔类型，[ClickHouse中没有定义布尔类型，可以使用UInt8类型，取值限制为0或1。]()



> ClickHouse支持`Enum8`和`Enum16`两种枚举类型，Enum保存的是'string'=integer的对应关系。

![1614326940185](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614326940185.png)

```SQL
CREATE TABLE tbl_test_enum
(
    `e1` Enum8('male' = 1, 'female' = 2), 
    `e2` Enum16('hello' = 1, 'word' = 2), 
    `e3` Nullable(Enum8('A' = 1, 'B' = 2)), 
    `e4` Nullable(Enum16('a' = 1, 'b' = 2))
)
ENGINE = TinyLog ;


insert into tbl_test_enum values('male', 'hello', 'A', null),('male', 'word', null, 'a');

insert into tbl_test_enum values(2, 1, 'A', null);
```



> ClickHouse支持`Nullable`类型，该类型允许用`NULL`来表示缺失值。

```SQL
-- Nullable 类型
CREATE TABLE tbl_test_nullable
(
    `f1` String, 
    `f2` Int8, 
    `f3` Nullable(Int8)
)
ENGINE = TinyLog ;

insert into tbl_test_nullable(f1,f2,f3) values('NoNull',1,1);

insert into tbl_test_nullable(f1,f2,f3) values(null,2,2);
insert into tbl_test_nullable(f1,f2,f3) values('NoNull2',null,2);
```



## 17-[掌握]-CK 数据类型之日期时间类型

> ​		ClickHouse数据库中支持日期和时间数据类型，必须掌握，往往定义表时，直接将字段类型设置为字符串String，需要时调用日期时间函数转换为对应格式。
>
> - 1）、日期类型
>
>   [ClickHouse支持Date类型，这个日期类型用两个字节存储，表示从 1970-01-01 (无符号) 到当
>   前的日期值]()
>
>   `select toDate('2019-12-12') as curDT;`
>
> - 2）、时间类型
>
>   [ClickHouse支持DataTime类型，这个时间戳类型用四个字节（无符号的）存储Unix时间戳]()
>
>   `select toDateTime('2019-12-16 14:27:30') as curDT;`



> ​	`interval`是ClickHouse提供的一种特殊的数据类型，此数据类型用来对Date和Datetime进行运算，不能使用Interval类型声明表中的字段。
>
> [Interval支持的时间类型有SECOND、MINUTE、HOUR、DAY、WEEK、MONTH、QUARTER和YEAR。]()

![1614327653792](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614327653792.png)

```ini
node2.itcast.cn :) select now() as cur_dt, cur_dt + interval 4 DAY plus_dt;

SELECT 
    now() AS cur_dt, 
    cur_dt + toIntervalDay(4) AS plus_dt

┌──────────────cur_dt─┬─────────────plus_dt─┐
│ 2021-02-26 16:21:17 │ 2021-03-02 16:21:17 │
└─────────────────────┴─────────────────────┘

1 rows in set. Elapsed: 0.005 sec. 


node2.itcast.cn :) select now() as cur_dt, cur_dt + interval 4 DAY + interval 3 HOUR as plus_dt;

SELECT 
    now() AS cur_dt, 
    (cur_dt + toIntervalDay(4)) + toIntervalHour(3) AS plus_dt

┌──────────────cur_dt─┬─────────────plus_dt─┐
│ 2021-02-26 16:22:15 │ 2021-03-02 19:22:15 │
└─────────────────────┴─────────────────────┘

1 rows in set. Elapsed: 0.003 sec. 
```



> 在创建表时，定义字段数据类型以后，插入数据如果不指定值，给以默认值

![1614327849172](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614327849172.png)



## 18-[理解]-CK 数据类型之数组和元组类型

> 在Scala语言中，集合框架中：数组和元组，[两者区别是什么？？？？？]()
>
> - 1）、数组中数据类型相同的，元组可以不同
> - 2）、数组中值可以修改，元组定义以后值不能修改

> ClickHouse支持Array(T)类型，T可以是任意类型，如果字段类型为数组类型，值表现形式：`[v1, v2, v3, …]`
>

![1614328054777](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614328054777.png)



> [ClickHouse提供Tuple类型支持，Tuple(T1,T2...)中每个元素都可以是单独的类型。]()

```ini
node2.itcast.cn :) select tuple(1, 'a') as x, toTypeName(x);

SELECT 
    (1, 'a') AS x, 
    toTypeName(x)

┌─x───────┬─toTypeName((1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String) │
└─────────┴──────────────────────┘

1 rows in set. Elapsed: 0.009 sec. 



node2.itcast.cn :) select tuple(1, null) as x, toTypeName(x);

SELECT 
    (1, NULL) AS x, 
    toTypeName(x)

┌─x────────┬─toTypeName((1, NULL))───────────┐
│ (1,NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└──────────┴─────────────────────────────────┘

1 rows in set. Elapsed: 0.003 sec. 
```



## 19-[了解]-CK 数据类型之其他类型

> ​	ClickHouse支持IPv4和Ipv6两种Domain类型，Ipv4类型是与UInt32类型保持二进制兼容的Domain类型，其用于存储IPv4地址的值；IPv6是与FixedString(16)类型保持二进制兼容的Domain类型，其用于存储IPv6地址的值。

```ini
node2.itcast.cn :) create table tbl_test_domain(url String, ip4 IPv4, ip6 IPv6) ENGINE = MergeTree() ORDER BY url;

CREATE TABLE tbl_test_domain
(
    `url` String, 
    `ip4` IPv4, 
    `ip6` IPv6
)
ENGINE = MergeTree()
ORDER BY url

Ok.

0 rows in set. Elapsed: 0.004 sec. 

node2.itcast.cn :) insert into tbl_test_domain(url,ip4,ip6) values('http://www.itcast.cn','127.0.0.1','2a02:aa08:e000:3100::2');

INSERT INTO tbl_test_domain (url, ip4, ip6) VALUES

Ok.

1 rows in set. Elapsed: 0.010 sec. 
```



> 嵌套数据结构：可以简单地把嵌套数据结构当做是所有列都是相同长度的多列数组。

```ini
node2.itcast.cn :) create table tbl_test_nested(uid Int64, ctime date, user Nested(name String, age Int8, phone Int64),
:-] Sign Int8) engine=CollapsingMergeTree(ctime,intHash32(uid),(ctime,intHash32(uid)),8192,Sign);

CREATE TABLE tbl_test_nested
(
    `uid` Int64, 
    `ctime` date, 
    `user` Nested(
    name String, 
    age Int8, 
    phone Int64), 
    `Sign` Int8
)
ENGINE = CollapsingMergeTree(ctime, intHash32(uid), (ctime, intHash32(uid)), 8192, Sign)

Ok.

0 rows in set. Elapsed: 0.005 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) insert into tbl_test_nested values(1,'2019-12-25',['zhangsan'],[23],[13800138000],1);

INSERT INTO tbl_test_nested VALUES

Ok.

1 rows in set. Elapsed: 0.011 sec. 

node2.itcast.cn :) select * from tbl_test_nested where uid=1 and arrayFilter(u -> u >= 20, user.age) != [];

SELECT *
FROM tbl_test_nested
WHERE (uid = 1) AND (arrayFilter(u -> (u >= 20), user.age) != [])

┌─uid─┬──────ctime─┬─user.name────┬─user.age─┬─user.phone────┬─Sign─┐
│   1 │ 2019-12-25 │ ['zhangsan'] │ [23]     │ [13800138000] │    1 │
└─────┴────────────┴──────────────┴──────────┴───────────────┴──────┘

1 rows in set. Elapsed: 0.014 sec. 
```



> ClickHouse中支持聚合函数类型：AggregateFunction 类型

![1614329560681](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614329560681.png)

```ini

CREATE TABLE aggMT
(
    `whatever` Date DEFAULT '2019-12-18', 
    `key` String, 
    `value` String, 
    `first` AggregateFunction(min, DateTime), 
    `last` AggregateFunction(max, DateTime), 
    `total` AggregateFunction(count, UInt64)
)
ENGINE = AggregatingMergeTree(whatever, (key, value), 8192)；

INSERT INTO aggMT (key, value, first, last, total) SELECT 
    'test', 
    '1.2.3.4', 
    minState(toDateTime(1576654217)), 
    maxState(toDateTime(1576654217)), 
    countState(CAST(1, 'UInt64'))；
    
INSERT INTO aggMT (key, value, first, last, total) SELECT 
    'test', 
    '1.2.3.5', 
    minState(toDateTime(1576654261)), 
    maxState(toDateTime(1576654261)), 
    countState(CAST(1, 'UInt64'))；
    
 INSERT INTO aggMT (key, value, first, last, total) SELECT 
    'test', 
    '1.2.3.6', 
    minState(toDateTime(1576654273)), 
    maxState(toDateTime(1576654273)), 
    countState(CAST(1, 'UInt64'))；
    
node2.itcast.cn :) select key, value,minMerge(first),maxMerge(last),countMerge(total) from aggMT group by key, value;

SELECT 
    key, 
    value, 
    minMerge(first), 
    maxMerge(last), 
    countMerge(total)
FROM aggMT
GROUP BY 
    key, 
    value

┌─key──┬─value───┬─────minMerge(first)─┬──────maxMerge(last)─┬─countMerge(total)─┐
│ test │ 1.2.3.5 │ 2019-12-18 15:31:01 │ 2019-12-18 15:31:01 │                 1 │
│ test │ 1.2.3.4 │ 2019-12-18 15:30:17 │ 2019-12-18 15:30:17 │                 1 │
│ test │ 1.2.3.6 │ 2019-12-18 15:31:13 │ 2019-12-18 15:31:13 │                 1 │
└──────┴─────────┴─────────────────────┴─────────────────────┴───────────────────┘    


```



## 20-[了解]-ClickHouse 引擎之TinyLog 引擎

> ​		ClickHouse之所以可以OLAP分析，查询检索数据很快，最重要的原因在于：存储引擎。

![1614329982014](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614329982014.png)

> 官方文档：https://clickhouse.tech/docs/zh/engines/table-engines/

![1614330015460](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614330015460.png)

![1614330101472](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614330101472.png)

> 最简单存储引擎：TinyLog日志引擎

![1614330257474](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614330257474.png)

```ini
node2.itcast.cn :) create table user (id UInt16, name String) ENGINE=TinyLog;

CREATE TABLE user
(
    `id` UInt16, 
    `name` String
)
ENGINE = TinyLog

Ok.

0 rows in set. Elapsed: 0.005 sec. 

node2.itcast.cn :) insert into user (id, name) values (1, 'zhangsan');

INSERT INTO user (id, name) VALUES

Ok.

1 rows in set. Elapsed: 0.002 sec. 

```

接下来，查看底层数据存储目录结构：

![1614330503530](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614330503530.png)

> 进入数据库：`db_ck` 

![1614330582897](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614330582897.png)

> 查看：sizes.json文件，

![1614330642707](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614330642707.png)



## 21-[了解]-ClickHouse 引擎之MySQL 引擎

> ​		在ClickHouse数据库中创建表时，可以将数据存储到指定MySQL数据库表中，进行关联存储，称为:MySQL 引擎，此种方式使用不多，但是可以通过此种方式集成CK与MySQL。
>
> [MySQL引擎用于将远程的MySQL服务器中的表映射到ClickHouse中，并允许您对表进行INSERT和SELECT查询，以方便您在ClickHouse与MySQL之间进行数据交换。]()

![1614330767215](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614330767215.png)

![1614330802927](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614330802927.png)

> 范例演示：

```ini
[root@node2 ~]# mysql -uroot -pAbcd1234.
mysql> create database test ;
Query OK, 1 row affected (0.00 sec)

mysql> use test ;
Database changed
mysql> 
mysql> CREATE TABLE `mysql_table` (
    -> `int_id` INT NOT NULL AUTO_INCREMENT,
    -> `float` FLOAT NOT NULL,
    -> PRIMARY KEY (`int_id`)
    -> );
Query OK, 0 rows affected (0.01 sec)

mysql> insert into mysql_table (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0.00 sec)

mysql> 
mysql> select * from mysql_table;
+--------+-------+
| int_id | float |
+--------+-------+
|      1 |     2 |
+--------+-------+
1 row in set (0.00 sec)



```



## 22-[掌握]-CK 引擎之MergeTree 引擎

> ​		MergeTree（合并树）系列引擎是ClickHouse中最强大的表引擎，是官方主推的存储引擎，几乎支持ClickHouse所有的核心功能。
>
> [该系列引擎主要用于海量数据分析的场景，支持对表数据进行分区、复制、采样、存储有序、主键索引、稀疏索引和数据TTL等特性。]()

![1614331941497](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614331941497.png)

> 简而言之就是具**有批量数据快速插入和后台并发处理的优势**。

![1614331982043](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614331982043.png)

> [MergeTree引擎的表的`允许插入主键重复的数据`，主键主要作用是`生成主键索引来提升查询效率`，而不是用来保持记录主键唯一。]()

![1614332084322](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614332084322.png)

```

```


