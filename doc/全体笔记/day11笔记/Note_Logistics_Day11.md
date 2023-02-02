---
stypora-copy-images-to: img
typora-root-url: ./
---



# Logistics_Day11：ClickHouse API 使用

![1614160990289](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614160990289.png)



## 01-[复习]-上次课程内容回顾 

> 主要讲解`ClickHouse`数据库（属于OLAP分析数据库），基本入门使用。
>
> - 1）、OLAP分析需求以及技术选项对比
>   - Hive（MapReduce、`Tez、Spark`）、`Presto、Impala`、SparkSQL
>   - Kylin（Hive，预计算，存储HBase）、Druid、`ClickHouse`
> - 2）、物流项目使用ClickHouse流程
>   - 数据实时ETL存储CK，编写结构化流程序
>   - 当业务数据实时同步到CK表中，如何被使用

![1614386666928](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614386666928.png)

> - 3）、ClickHouse 基础入门
>
>   - ClickHouse框架概述：官网定义、官方文档
>   - ClickHouse应用场景、使用案例和优缺点
>   - ClickHouse 单机安装、配置和服务启动：采用yum在线安装，或者rpm离线安装
>     - 配置文件：`/etc/clickhouse-server`-> `config.xml`  和 `users.xml`
>     - 端口号：HTTP服务【8123】，TCP服务【9000，修改为9999】
>     - 启动服务和命令行`clickhouse-client`
>
>   ```ini
>   [root@node2 ~]# systemctl start clickhouse-server
>   [root@node2 ~]# 
>   [root@node2 ~]# ps -ef|grep clickhouse-server
>   clickho+  2891     1 16 08:47 ?        00:00:01 /usr/bin/clickhouse-server --config=/etc/clickhouse-server/config.xml --pid-file=/run/clickhouse-server/clickhouse-server.pid
>   
>   [root@node2 ~]# clickhouse-client -m --host node2.itcast.cn --port 9999 --user root --password 123456
>   ClickHouse client version 20.4.2.9 (official build).
>   Connecting to node2.itcast.cn:9999 as user root.
>   Connected to ClickHouse server version 20.4.2 revision 54434.
>   
>   node2.itcast.cn :) 
>   ```
>
>   - 官方案例数据分析：航班飞行数据
>
> - 4）、ClickHouse SQL 语法
>
>   - 常用SQL语句，创建表、删除表、创建视图等语句，类似MySQL数据库
>
>   - 插入数据INSERT
>
>   - 查询数据SELECT
>
>   - ALTER 语法，针对表的列进行添加、删除和修改，此外使用UPDATE和DELETE
>
>   - 更新和删除
>
>     ```SQL
>     ALTER TABLE db.table UPDATE SET col1=value1, col2=value2, ... WHERE id = ?
>     
>     ALTER TABLE db.table DELETE WHERE id = ?
>     ```
>
>   - 基本函数
>
> - 5）、ClickHouse 数据类型
>
>   - 数值类型、字符串、日期时间、数组和元组及其他数据类型
>
> - 6）、ClickHouse 数据存储引擎
>
>   - 日志引擎，TinyLog日志存储
>   - 数据库引擎，将ClickHouse表映射到MySQL数据库，数据真正存储和查询在MySQL数据库进行的
>   - 合并树（MergeTree）系列引擎
>     - `MergeTree`引擎



## 02-[了解]-第11天：课程内容提纲 

> 课程内容2个方面内容：ClickHouse 合并树系列引擎和ClickHouse JDBC API使用。
>
> - 1）、ClickHouse 存储引擎：MergeTree 系列引擎
>   - 文档：https://clickhouse.tech/docs/zh/engines/table-engines/mergetree-family/
>   - ReplaceingMergeTree：替换表中数据
>   - SummingMergeTree：使用sum函数聚合和AggregatingMergeTree（指定聚合函数）引擎
>   - 折叠树引擎和`多版本折叠树引擎`
> - 2）、ClickHouse JDBC Client API使用：必须掌握
>   - Java 操作CK，调用JDBC Client API进行数据查询，类似MySQL JDBC使用
>   - Spark 操作CK，调用JDBC Client API进行DDL操作和DML操作
>     - DDL操作：创建表和删除表
>     - DML操作：插入数据INSERT、更新数据UPDATE和删除数据DELETE
>   - 文档：https://clickhouse.tech/docs/zh/interfaces/jdbc/



## 03-[掌握]-CK 引擎之ReplacingMergeTree 引擎

> ​		为了解决MergeTree相同主键无法去重的问题，ClickHouse提供了ReplacingMergeTree引擎，用来对主键重复的数据进行去重。
>
> [删除重复数据可以使用optimize命令手动执行，这个合并操作是在后台运行的，且无法预测具体的执行时间。]()
>
> ReplacingMergeTree适合在后台清除重复数据以节省空间，但不能保证不存在重复数据。

![1614388309916](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614388309916.png)

> 创建表，演示案例：

```scala

node2.itcast.cn :) CREATE TABLE tbl_test_replacingmergetree_users
:-] (
:-]     `id` UInt64, 
:-]     `email` String, 
:-]     `username` String, 
:-]     `gender` UInt8, 
:-]     `birthday` Date, 
:-]     `mobile` FixedString(13), 
:-]     `pwd` String, 
:-]     `regDT` DateTime, 
:-]     `lastLoginDT` DateTime, 
:-]     `lastLoginIP` String
:-] )
:-] ENGINE = ReplacingMergeTree(id)
:-] PARTITION BY toYYYYMMDD(regDT)
:-] ORDER BY id
:-] SETTINGS index_granularity = 8192 ;

CREATE TABLE tbl_test_replacingmergetree_users
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
ENGINE = ReplacingMergeTree(id)
PARTITION BY toYYYYMMDD(regDT)
ORDER BY id
SETTINGS index_granularity = 8192

Ok.

0 rows in set. Elapsed: 0.008 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) insert into tbl_test_replacingmergetree_users select * from tbl_test_mergetree_users where id<=5;

INSERT INTO tbl_test_replacingmergetree_users SELECT *
FROM tbl_test_mergetree_users
WHERE id <= 5

Ok.

0 rows in set. Elapsed: 0.013 sec. 

node2.itcast.cn :) show tables ;

SHOW TABLES

┌─name──────────────────────────────┐
│ ontime                            │
│ tbl_order                         │
│ tbl_test_mergetree_users          │
│ tbl_test_replacingmergetree_users │
└───────────────────────────────────┘

4 rows in set. Elapsed: 0.014 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) 
node2.itcast.cn :) select * from tbl_test_replacingmergetree_users ;

SELECT *
FROM tbl_test_replacingmergetree_users

┌─id─┬─email─────────────┬─username─┬─gender─┬───birthday─┬─mobile──────┬─pwd──────────────────────────────┬───────────────regDT─┬─────────lastLoginDT─┬─lastLoginIP────┐
│  2 │ xuwcbev9y@ask.com │ 上磊     │      1 │ 1983-10-11 │ 15302753472 │ 7f930f90eb6604e837db06908cc95149 │ 2008-08-10 05:37:32 │ 2014-07-28 23:43:04 │ 121.77.119.233 │
│  3 │ mgaqfew@126.com   │ 涂康     │      1 │ 1970-11-22 │ 15200570030 │ 96802a851b4a7295fb09122b9aa79c18 │ 2008-08-10 11:37:55 │ 2014-07-22 23:45:47 │ 171.12.206.122 │
│  4 │ b7zthcdg@163.net  │ 金俊振   │      1 │ 2002-02-10 │ 15207308903 │ 96802a851b4a7295fb09122b9aa79c18 │ 2008-08-10 14:47:09 │ 2013-12-26 15:55:02 │ 61.235.143.92  │
└────┴───────────────────┴──────────┴────────┴────────────┴─────────────┴──────────────────────────────────┴─────────────────────┴─────────────────────┴────────────────┘
┌─id─┬─email───────────┬─username─┬─gender─┬───birthday─┬─mobile──────┬─pwd──────────────────────────────┬───────────────regDT─┬─────────lastLoginDT─┬─lastLoginIP───┐
│  5 │ ezrvy0p@163.net │ 阴福     │      1 │ 1987-09-01 │ 13005861359 │ 96802a851b4a7295fb09122b9aa79c18 │ 2008-08-12 21:58:11 │ 2013-12-26 15:52:33 │ 182.81.200.32 │
└────┴─────────────────┴──────────┴────────┴────────────┴─────────────┴──────────────────────────────────┴─────────────────────┴─────────────────────┴───────────────┘
┌─id─┬─email─────────────┬─username─┬─gender─┬───birthday─┬─mobile──────┬─pwd──────────────────────────────┬───────────────regDT─┬─────────lastLoginDT─┬─lastLoginIP───┐
│  1 │ wcfr817e@yeah.net │ 督咏     │      2 │ 1992-05-31 │ 13306834911 │ 7f930f90eb6604e837db06908cc95149 │ 2008-08-06 11:48:12 │ 2015-05-08 10:51:41 │ 106.83.54.165 │
└────┴───────────────────┴──────────┴────────┴────────────┴─────────────┴──────────────────────────────────┴─────────────────────┴─────────────────────┴───────────────┘

5 rows in set. Elapsed: 0.006 sec. 

node2.itcast.cn :) insert into tbl_test_replacingmergetree_users(id,email,username,gender,birthday,mobile,pwd,regDT,lastLoginIP,lastLoginDT) select id,email,username,gender,birthday,mobile,pwd,regDT,lastLoginIP,now() as lastLoginDT from tbl_test_mergetree_users where id<=3;

INSERT INTO tbl_test_replacingmergetree_users (id, email, username, gender, birthday, mobile, pwd, regDT, lastLoginIP, lastLoginDT) SELECT 
    id, 
    email, 
    username, 
    gender, 
    birthday, 
    mobile, 
    pwd, 
    regDT, 
    lastLoginIP, 
    now() AS lastLoginDT
FROM tbl_test_mergetree_users
WHERE id <= 3

Ok.

0 rows in set. Elapsed: 0.022 sec. 

node2.itcast.cn :) select * from tbl_test_replacingmergetree_users ;

SELECT *
FROM tbl_test_replacingmergetree_users

┌─id─┬─email─────────────┬─username─┬─gender─┬───birthday─┬─mobile──────┬─pwd──────────────────────────────┬───────────────regDT─┬─────────lastLoginDT─┬─lastLoginIP───┐
│  1 │ wcfr817e@yeah.net │ 督咏     │      2 │ 1992-05-31 │ 13306834911 │ 7f930f90eb6604e837db06908cc95149 │ 2008-08-06 11:48:12 │ 2015-05-08 10:51:41 │ 106.83.54.165 │
└────┴───────────────────┴──────────┴────────┴────────────┴─────────────┴──────────────────────────────────┴─────────────────────┴─────────────────────┴───────────────┘
┌─id─┬─email─────────────┬─username─┬─gender─┬───birthday─┬─mobile──────┬─pwd──────────────────────────────┬───────────────regDT─┬─────────lastLoginDT─┬─lastLoginIP───┐
│  1 │ wcfr817e@yeah.net │ 督咏     │      2 │ 1992-05-31 │ 13306834911 │ 7f930f90eb6604e837db06908cc95149 │ 2008-08-06 11:48:12 │ 2021-02-27 09:17:17 │ 106.83.54.165 │
└────┴───────────────────┴──────────┴────────┴────────────┴─────────────┴──────────────────────────────────┴─────────────────────┴─────────────────────┴───────────────┘
┌─id─┬─email─────────────┬─username─┬─gender─┬───birthday─┬─mobile──────┬─pwd──────────────────────────────┬───────────────regDT─┬─────────lastLoginDT─┬─lastLoginIP────┐
│  2 │ xuwcbev9y@ask.com │ 上磊     │      1 │ 1983-10-11 │ 15302753472 │ 7f930f90eb6604e837db06908cc95149 │ 2008-08-10 05:37:32 │ 2014-07-28 23:43:04 │ 121.77.119.233 │
│  3 │ mgaqfew@126.com   │ 涂康     │      1 │ 1970-11-22 │ 15200570030 │ 96802a851b4a7295fb09122b9aa79c18 │ 2008-08-10 11:37:55 │ 2014-07-22 23:45:47 │ 171.12.206.122 │
│  4 │ b7zthcdg@163.net  │ 金俊振   │      1 │ 2002-02-10 │ 15207308903 │ 96802a851b4a7295fb09122b9aa79c18 │ 2008-08-10 14:47:09 │ 2013-12-26 15:55:02 │ 61.235.143.92  │
└────┴───────────────────┴──────────┴────────┴────────────┴─────────────┴──────────────────────────────────┴─────────────────────┴─────────────────────┴────────────────┘
┌─id─┬─email─────────────┬─username─┬─gender─┬───birthday─┬─mobile──────┬─pwd──────────────────────────────┬───────────────regDT─┬─────────lastLoginDT─┬─lastLoginIP────┐
│  2 │ xuwcbev9y@ask.com │ 上磊     │      1 │ 1983-10-11 │ 15302753472 │ 7f930f90eb6604e837db06908cc95149 │ 2008-08-10 05:37:32 │ 2021-02-27 09:17:17 │ 121.77.119.233 │
│  3 │ mgaqfew@126.com   │ 涂康     │      1 │ 1970-11-22 │ 15200570030 │ 96802a851b4a7295fb09122b9aa79c18 │ 2008-08-10 11:37:55 │ 2021-02-27 09:17:17 │ 171.12.206.122 │
└────┴───────────────────┴──────────┴────────┴────────────┴─────────────┴──────────────────────────────────┴─────────────────────┴─────────────────────┴────────────────┘
┌─id─┬─email───────────┬─username─┬─gender─┬───birthday─┬─mobile──────┬─pwd──────────────────────────────┬───────────────regDT─┬─────────lastLoginDT─┬─lastLoginIP───┐
│  5 │ ezrvy0p@163.net │ 阴福     │      1 │ 1987-09-01 │ 13005861359 │ 96802a851b4a7295fb09122b9aa79c18 │ 2008-08-12 21:58:11 │ 2013-12-26 15:52:33 │ 182.81.200.32 │
└────┴─────────────────┴──────────┴────────┴────────────┴─────────────┴──────────────────────────────────┴─────────────────────┴─────────────────────┴───────────────┘

8 rows in set. Elapsed: 0.022 sec. 

node2.itcast.cn :) optimize table tbl_test_replacingmergetree_users final;

OPTIMIZE TABLE tbl_test_replacingmergetree_users FINAL

Ok.

0 rows in set. Elapsed: 0.009 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) select * from tbl_test_replacingmergetree_users ;

SELECT *
FROM tbl_test_replacingmergetree_users

┌─id─┬─email─────────────┬─username─┬─gender─┬───birthday─┬─mobile──────┬─pwd──────────────────────────────┬───────────────regDT─┬─────────lastLoginDT─┬─lastLoginIP────┐
│  2 │ xuwcbev9y@ask.com │ 上磊     │      1 │ 1983-10-11 │ 15302753472 │ 7f930f90eb6604e837db06908cc95149 │ 2008-08-10 05:37:32 │ 2021-02-27 09:17:17 │ 121.77.119.233 │
│  3 │ mgaqfew@126.com   │ 涂康     │      1 │ 1970-11-22 │ 15200570030 │ 96802a851b4a7295fb09122b9aa79c18 │ 2008-08-10 11:37:55 │ 2021-02-27 09:17:17 │ 171.12.206.122 │
│  4 │ b7zthcdg@163.net  │ 金俊振   │      1 │ 2002-02-10 │ 15207308903 │ 96802a851b4a7295fb09122b9aa79c18 │ 2008-08-10 14:47:09 │ 2013-12-26 15:55:02 │ 61.235.143.92  │
└────┴───────────────────┴──────────┴────────┴────────────┴─────────────┴──────────────────────────────────┴─────────────────────┴─────────────────────┴────────────────┘
┌─id─┬─email─────────────┬─username─┬─gender─┬───birthday─┬─mobile──────┬─pwd──────────────────────────────┬───────────────regDT─┬─────────lastLoginDT─┬─lastLoginIP───┐
│  1 │ wcfr817e@yeah.net │ 督咏     │      2 │ 1992-05-31 │ 13306834911 │ 7f930f90eb6604e837db06908cc95149 │ 2008-08-06 11:48:12 │ 2021-02-27 09:17:17 │ 106.83.54.165 │
└────┴───────────────────┴──────────┴────────┴────────────┴─────────────┴──────────────────────────────────┴─────────────────────┴─────────────────────┴───────────────┘
┌─id─┬─email───────────┬─username─┬─gender─┬───birthday─┬─mobile──────┬─pwd──────────────────────────────┬───────────────regDT─┬─────────lastLoginDT─┬─lastLoginIP───┐
│  5 │ ezrvy0p@163.net │ 阴福     │      1 │ 1987-09-01 │ 13005861359 │ 96802a851b4a7295fb09122b9aa79c18 │ 2008-08-12 21:58:11 │ 2013-12-26 15:52:33 │ 182.81.200.32 │
└────┴─────────────────┴──────────┴────────┴────────────┴─────────────┴──────────────────────────────────┴─────────────────────┴─────────────────────┴───────────────┘

5 rows in set. Elapsed: 0.007 sec. 
```



## 04-[掌握]-CK 引擎之SummingMergeTree 引擎

> [ClickHouse通过SummingMergeTree来支持对主键列进行预聚合。]()
>
> ​		在后台合并时，会将主键相同的多行进行sum求和，然后使用一行数据取而代之，从而大幅度降低存储空间占用，提升聚合计算性能。
>
> [在预聚合时，ClickHouse会对主键列以外的其他所有列进行预聚合。]()
>
> - 但这些列必须是数值类型才会计算sum（当sum结果为0时会删除此行数据）；
> - 如果是String等不可聚合的类型，则随机选择一个值。

![1614389158549](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614389158549.png)

> ​		通常建议将SummingMergeTree与MergeTree配合使用，使用MergeTree来存储明细数据，使用SummingMergeTree存储预聚合的数据来支撑加速查询。

![1614389217787](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614389217787.png)

> 演示案例如下所示

```ini
node2.itcast.cn :) create table tbl_test_summingmergetree(
:-] key UInt64,
:-] value UInt64
:-] ) engine=SummingMergeTree() order by key;

CREATE TABLE tbl_test_summingmergetree
(
    `key` UInt64, 
    `value` UInt64
)
ENGINE = SummingMergeTree()
ORDER BY key

Ok.

0 rows in set. Elapsed: 0.005 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) insert into tbl_test_summingmergetree(key,value) values(1,13);

INSERT INTO tbl_test_summingmergetree (key, value) VALUES

Ok.

1 rows in set. Elapsed: 0.004 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) 
node2.itcast.cn :) select * from tbl_test_summingmergetree;

SELECT *
FROM tbl_test_summingmergetree

┌─key─┬─value─┐
│   1 │    13 │
└─────┴───────┘

1 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) insert into tbl_test_summingmergetree(key,value) values(1,13);

INSERT INTO tbl_test_summingmergetree (key, value) VALUES

Ok.

1 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) SELECT * FROM tbl_address WHERE 1=0;

SELECT *
FROM tbl_address
WHERE 1 = 0

Received exception from server (version 20.4.2):
Code: 60. DB::Exception: Received from node2.itcast.cn:9999. DB::Exception: Table default.tbl_address doesn't exist.. 

0 rows in set. Elapsed: 0.004 sec. 

node2.itcast.cn :) select * from tbl_test_summingmergetree;

SELECT *
FROM tbl_test_summingmergetree

┌─key─┬─value─┐
│   1 │    13 │
└─────┴───────┘
┌─key─┬─value─┐
│   1 │    13 │
└─────┴───────┘

2 rows in set. Elapsed: 0.005 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) 
node2.itcast.cn :) insert into tbl_test_summingmergetree(key,value) values(1,16);

INSERT INTO tbl_test_summingmergetree (key, value) VALUES

Ok.

1 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) select * from tbl_test_summingmergetree;

SELECT *
FROM tbl_test_summingmergetree

┌─key─┬─value─┐
│   1 │    13 │
└─────┴───────┘
┌─key─┬─value─┐
│   1 │    13 │
└─────┴───────┘
┌─key─┬─value─┐
│   1 │    16 │
└─────┴───────┘

3 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) 
node2.itcast.cn :) select key,sum(value),count(value) from tbl_test_summingmergetree group by key;

SELECT 
    key, 
    sum(value), 
    count(value)
FROM tbl_test_summingmergetree
GROUP BY key

┌─key─┬─sum(value)─┬─count(value)─┐
│   1 │         42 │            3 │
└─────┴────────────┴──────────────┘

1 rows in set. Elapsed: 0.009 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) optimize table tbl_test_summingmergetree final;

OPTIMIZE TABLE tbl_test_summingmergetree FINAL

Ok.

0 rows in set. Elapsed: 0.005 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) select * from tbl_test_summingmergetree;

SELECT *
FROM tbl_test_summingmergetree

┌─key─┬─value─┐
│   1 │    42 │
└─────┴───────┘

1 rows in set. Elapsed: 0.002 sec. 
```



## 05-[掌握]-CK 引擎之AggregatingMergeTree 引擎

> AggregatingMergeTree也是预聚合引擎的一种，是在MergeTree的基础上针对聚合函数计算结果进行增量计算用于提升聚合计算的性能。
>
> [AggregatingMergeTree与SummingMergeTree区别在于：可以指定不同聚合函数，针对不同列]()
>
> ​		与SummingMergeTree的区别在于：SummingMergeTree对非主键列进行sum聚合，而AggregatingMergeTree则可以指定各种聚合函数。
>
> [在insert和select时，也有独特的写法和要求：写入时需要使用-State语法，查询时使用-Merge语法。]()

![1614390567329](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614390567329.png)

> 实例案例，如下所示：

```ini
node2.itcast.cn :) create table tbl_test_mergetree_logs(
:-] guid String,
:-] url String,
:-] refUrl String,
:-] cnt UInt16,
:-] cdt DateTime
:-] ) engine = MergeTree() partition by toYYYYMMDD(cdt) order by toYYYYMMDD(cdt);

CREATE TABLE tbl_test_mergetree_logs
(
    `guid` String, 
    `url` String, 
    `refUrl` String, 
    `cnt` UInt16, 
    `cdt` DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(cdt)
ORDER BY toYYYYMMDD(cdt)

Ok.

0 rows in set. Elapsed: 0.004 sec. 

node2.itcast.cn :) insert into tbl_test_mergetree_logs(guid,url,refUrl,cnt,cdt) values('a','www.itheima.com','www.itcast.cn',1,'2019-12-17 12:12:12'),('a','www.itheima.com','www.itcast.cn',1,'2019-12-17 12:14:45'),('b','www.itheima.com','www.itcast.cn',1,'2019-12-17 13:13:13');

INSERT INTO tbl_test_mergetree_logs (guid, url, refUrl, cnt, cdt) VALUES

Ok.

3 rows in set. Elapsed: 0.019 sec. 

node2.itcast.cn :) select * from tbl_test_mergetree_logs;

SELECT *
FROM tbl_test_mergetree_logs

┌─guid─┬─url─────────────┬─refUrl────────┬─cnt─┬─────────────────cdt─┐
│ a    │ www.itheima.com │ www.itcast.cn │   1 │ 2019-12-17 12:12:12 │
│ a    │ www.itheima.com │ www.itcast.cn │   1 │ 2019-12-17 12:14:45 │
│ b    │ www.itheima.com │ www.itcast.cn │   1 │ 2019-12-17 13:13:13 │
└──────┴─────────────────┴───────────────┴─────┴─────────────────────┘

3 rows in set. Elapsed: 0.004 sec. 

node2.itcast.cn :) create table tbl_test_aggregationmergetree_visitor(
:-] guid String,
:-] cnt AggregateFunction(count, UInt16),
:-] cdt Date
:-] ) engine = AggregatingMergeTree() partition by cdt order by cnt;

CREATE TABLE tbl_test_aggregationmergetree_visitor
(
    `guid` String, 
    `cnt` AggregateFunction(count, UInt16), 
    `cdt` Date
)
ENGINE = AggregatingMergeTree()
PARTITION BY cdt
ORDER BY cnt

Ok.

0 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) insert into tbl_test_aggregationmergetree_visitor select guid,countState(cnt),toDate(cdt) from tbl_test_mergetree_logs group by guid,cnt,cdt;

INSERT INTO tbl_test_aggregationmergetree_visitor SELECT 
    guid, 
    countState(cnt), 
    toDate(cdt)
FROM tbl_test_mergetree_logs
GROUP BY 
    guid, 
    cnt, 
    cdt

Ok.

0 rows in set. Elapsed: 0.007 sec. 

node2.itcast.cn :) select guid,count(cnt) from tbl_test_aggregationmergetree_visitor group by guid,cnt;

SELECT 
    guid, 
    count(cnt)
FROM tbl_test_aggregationmergetree_visitor
GROUP BY 
    guid, 
    cnt

┌─guid─┬─count(cnt)─┐
│ a    │          2 │
│ b    │          1 │
└──────┴────────────┘

2 rows in set. Elapsed: 0.004 sec. 
```



## 06-[掌握]-CK 引擎之CollapsingMergeTree 引擎

> ​		ClickHouse提供了一个CollapsingMergeTree表引擎，它继承于MergeTree引擎，是通过一种变通的方式来实现状态的更新。
>
> [指定Sign列（必须是Int8类型）：]()
>
> - 1表示为状态行，当需要新增一个状态时，需要将insert语句中的Sign列值设为1；
> - -1表示为取消行，当需要删除一个状态时，需要将insert语句中的Sign列值设为-1。

![1614391139950](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614391139950.png)

> 演示案例如下：

```ini
node2.itcast.cn :) create table tbl_test_collapsingmergetree_day_mall_sale (
:-] mallId UInt64,
:-] mallName String,
:-] totalAmount Decimal(32,2),
:-] cdt Date,
:-] sign Int8
:-] ) engine=CollapsingMergeTree(sign) partition by toYYYYMMDD(cdt) order by mallId;

CREATE TABLE tbl_test_collapsingmergetree_day_mall_sale
(
    `mallId` UInt64, 
    `mallName` String, 
    `totalAmount` Decimal(32, 2), 
    `cdt` Date, 
    `sign` Int8
)
ENGINE = CollapsingMergeTree(sign)
PARTITION BY toYYYYMMDD(cdt)
ORDER BY mallId

Ok.

0 rows in set. Elapsed: 0.020 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) insert into tbl_test_collapsingmergetree_day_mall_sale(mallId,mallName,totalAmount,cdt,sign) values(1,'西单大悦城',17649135.64,'2019-12-24',1);

INSERT INTO tbl_test_collapsingmergetree_day_mall_sale (mallId, mallName, totalAmount, cdt, sign) VALUES

Ok.

1 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) insert into tbl_test_collapsingmergetree_day_mall_sale(mallId,mallName,totalAmount,cdt,sign) values(2,'朝阳大悦城',16341742.99,'2019-12-24',1);

INSERT INTO tbl_test_collapsingmergetree_day_mall_sale (mallId, mallName, totalAmount, cdt, sign) VALUES

Ok.

1 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) select * from tbl_test_collapsingmergetree_day_mall_sale;

SELECT *
FROM tbl_test_collapsingmergetree_day_mall_sale

┌─mallId─┬─mallName───┬─totalAmount─┬────────cdt─┬─sign─┐
│      2 │ 朝阳大悦城 │ 16341742.99 │ 2019-12-24 │    1 │
└────────┴────────────┴─────────────┴────────────┴──────┘
┌─mallId─┬─mallName───┬─totalAmount─┬────────cdt─┬─sign─┐
│      1 │ 西单大悦城 │ 17649135.64 │ 2019-12-24 │    1 │
└────────┴────────────┴─────────────┴────────────┴──────┘

2 rows in set. Elapsed: 0.004 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) insert into tbl_test_collapsingmergetree_day_mall_sale(mallId,mallName,totalAmount,cdt,sign) values(1,'西单大悦城',17649135.64,'2019-12-24',-1);

INSERT INTO tbl_test_collapsingmergetree_day_mall_sale (mallId, mallName, totalAmount, cdt, sign) VALUES

Ok.

1 rows in set. Elapsed: 0.005 sec. 

node2.itcast.cn :) insert into tbl_test_collapsingmergetree_day_mall_sale(mallId,mallName,totalAmount,cdt,sign) values(2,'朝阳大悦城',16341742.99,'2019-12-24',-1);

INSERT INTO tbl_test_collapsingmergetree_day_mall_sale (mallId, mallName, totalAmount, cdt, sign) VALUES

Ok.

1 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) optimize table tbl_test_collapsingmergetree_day_mall_sale final;

OPTIMIZE TABLE tbl_test_collapsingmergetree_day_mall_sale FINAL

Ok.

0 rows in set. Elapsed: 0.007 sec. 

node2.itcast.cn :) select * from tbl_test_collapsingmergetree_day_mall_sale;

SELECT *
FROM tbl_test_collapsingmergetree_day_mall_sale

Ok.

0 rows in set. Elapsed: 0.002 sec. 

node2.itcast.cn :) insert into tbl_test_collapsingmergetree_day_mall_sale(mallId,mallName,totalAmount,cdt,sign) values(1,'西单大悦城',17649135.64,'2019-12-24',1);

INSERT INTO tbl_test_collapsingmergetree_day_mall_sale (mallId, mallName, totalAmount, cdt, sign) VALUES

Ok.

1 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) insert into tbl_test_collapsingmergetree_day_mall_sale(mallId,mallName,totalAmount,cdt,sign) values(2,'朝阳大悦城',16341742.99,'2019-12-24',1);

INSERT INTO tbl_test_collapsingmergetree_day_mall_sale (mallId, mallName, totalAmount, cdt, sign) VALUES

Ok.

1 rows in set. Elapsed: 0.007 sec. 

node2.itcast.cn :) insert into tbl_test_collapsingmergetree_day_mall_sale(mallId,mallName,totalAmount,cdt,sign) values(1,'西单大悦城',17649135.64,'2019-12-24',-1);

INSERT INTO tbl_test_collapsingmergetree_day_mall_sale (mallId, mallName, totalAmount, cdt, sign) VALUES

Ok.

1 rows in set. Elapsed: 0.004 sec. 

node2.itcast.cn :) select * from tbl_test_collapsingmergetree_day_mall_sale;

SELECT *
FROM tbl_test_collapsingmergetree_day_mall_sale

┌─mallId─┬─mallName───┬─totalAmount─┬────────cdt─┬─sign─┐
│      2 │ 朝阳大悦城 │ 16341742.99 │ 2019-12-24 │    1 │
└────────┴────────────┴─────────────┴────────────┴──────┘
┌─mallId─┬─mallName───┬─totalAmount─┬────────cdt─┬─sign─┐
│      1 │ 西单大悦城 │ 17649135.64 │ 2019-12-24 │    1 │
└────────┴────────────┴─────────────┴────────────┴──────┘
┌─mallId─┬─mallName───┬─totalAmount─┬────────cdt─┬─sign─┐
│      1 │ 西单大悦城 │ 17649135.64 │ 2019-12-24 │   -1 │
└────────┴────────────┴─────────────┴────────────┴──────┘

3 rows in set. Elapsed: 0.008 sec. 

node2.itcast.cn :) optimize table tbl_test_collapsingmergetree_day_mall_sale final;

OPTIMIZE TABLE tbl_test_collapsingmergetree_day_mall_sale FINAL

Ok.

0 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) select * from tbl_test_collapsingmergetree_day_mall_sale;

SELECT *
FROM tbl_test_collapsingmergetree_day_mall_sale

┌─mallId─┬─mallName───┬─totalAmount─┬────────cdt─┬─sign─┐
│      2 │ 朝阳大悦城 │ 16341742.99 │ 2019-12-24 │    1 │
└────────┴────────────┴─────────────┴────────────┴──────┘

1 rows in set. Elapsed: 0.023 sec. 
```





## 07-[掌握]-CK 引擎之多版本折叠树引擎

![1614391559717](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614391559717.png)

> 允许以多个线程的任何顺序插入数据，特别是 Version 列有助于正确折叠行

![1614391619065](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614391619065.png)

> 其中表中存在两个字段：
>
> - 1）、Sing：标识字段，类型：Int8，值：1和-1
> - 2）、Version：版本字段，类型UInt8，值为正整数

```ini

node2.itcast.cn :) CREATE TABLE UAct
:-] (
:-] UserID UInt64,
:-] PageViews UInt8,
:-] Duration UInt8,
:-] Sign Int8,
:-] Version UInt8
:-] )
:-] ENGINE = VersionedCollapsingMergeTree(Sign, Version) 
:-] ORDER BY UserID ;

CREATE TABLE UAct
(
    `UserID` UInt64, 
    `PageViews` UInt8, 
    `Duration` UInt8, 
    `Sign` Int8, 
    `Version` UInt8
)
ENGINE = VersionedCollapsingMergeTree(Sign, Version)
ORDER BY UserID

Ok.

0 rows in set. Elapsed: 0.021 sec. 

node2.itcast.cn :) INSERT INTO UAct VALUES (4324182021466249494, 5, 146, 1, 1) ;

INSERT INTO UAct VALUES

Ok.

1 rows in set. Elapsed: 0.004 sec. 

node2.itcast.cn :) show tables ;

SHOW TABLES

┌─name───────────────────────────────────────┐
│ UAct                                       │
│ ontime                                     │
│ tbl_order                                  │
│ tbl_test_aggregationmergetree_visitor      │
│ tbl_test_collapsingmergetree_day_mall_sale │
│ tbl_test_mergetree_logs                    │
│ tbl_test_mergetree_users                   │
│ tbl_test_replacingmergetree_users          │
│ tbl_test_summingmergetree                  │
└────────────────────────────────────────────┘

9 rows in set. Elapsed: 0.004 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) 
node2.itcast.cn :) select * from UAct
:-] ;

SELECT *
FROM UAct

┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘

1 rows in set. Elapsed: 0.002 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) 
node2.itcast.cn :) INSERT INTO UAct VALUES (4324182021466249494, 5, 146, -1, 1),(4324182021466249494, 6, 185, 1, 2) ;

INSERT INTO UAct VALUES

Ok.

2 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) 
node2.itcast.cn :) select * from UAct ;

SELECT *
FROM UAct

┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘

3 rows in set. Elapsed: 0.014 sec. 

node2.itcast.cn :) select * from UAct ;

SELECT *
FROM UAct

┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘

3 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) select * from UAct ;

SELECT *
FROM UAct

┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘

3 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) select * from UAct ;

SELECT *
FROM UAct

┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘

3 rows in set. Elapsed: 0.004 sec. 

node2.itcast.cn :) select * from UAct ;

SELECT *
FROM UAct

┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │   -1 │       1 │
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         5 │      146 │    1 │       1 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘

3 rows in set. Elapsed: 0.004 sec. 

node2.itcast.cn :) optimize table UAct final ;

OPTIMIZE TABLE UAct FINAL

Ok.

0 rows in set. Elapsed: 0.005 sec. 

node2.itcast.cn :) 
node2.itcast.cn :) select * from UAct ;

SELECT *
FROM UAct

┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┬─Version─┐
│ 4324182021466249494 │         6 │      185 │    1 │       2 │
└─────────────────────┴───────────┴──────────┴──────┴─────────┘

1 rows in set. Elapsed: 0.004 sec. 
```



## 08-[掌握]-Java 操作 CK之JDBC Client 概述

> ​		ClickHouse数据库官方提供Client客户端，其中提供类似MySQL数据库方式JDBC Client客户端
>
> 文档：https://clickhouse.tech/docs/zh/interfaces/#interfaces，提供2个网络接口：
>
> - 1）、HTTP：端口号【8123】
>   - JDBC Client 使用8123端口，连接ClickHouse Server，进行操作
> - 2）、Native TCP：端口号【9000】，物流项目中修改为【9999】
>   - clickhouse-client交互式命令行客户端

![1614392388793](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614392388793.png)

> JDBC驱动：https://clickhouse.tech/docs/zh/interfaces/jdbc/
>
> - 1）、官方驱动：建议使用
>
>   - https://github.com/ClickHouse/clickhouse-jdbc
>
>   - 添加依赖：
>
>     ```xml
>     <dependency>
>         <groupId>ru.yandex.clickhouse</groupId>
>         <artifactId>clickhouse-jdbc</artifactId>
>         <version>0.2.6</version>
>     </dependency>
>     ```
>
>   - JDBC 驱动连接地址和驱动类
>
>     - JDBC Driver Class: `ru.yandex.clickhouse.ClickHouseDriver`
>     - URL syntax: `jdbc:clickhouse://<host>:<port>[/<database>]`
>
>   - 获取连接Connection，获取Statement对象，进行查询和执行更新操作
>
>   - 
>
> - 2）、第三方，都是基于官方提供驱动进行优化修改的
>
>   - [ClickHouse-Native-JDBC](https://github.com/housepower/ClickHouse-Native-JDBC)
>   - [clickhouse4j](https://github.com/blynkkk/clickhouse4j)

​		使用JDBC Client驱动，对ClickHouse数据库进行DDL操作（创建表和删除表）及DML操作（CRUD）。



## 09-[理解]-Java 操作 CK之工程环境准备

> ​		在前面创建好的Maven Projet工程中，创建Maven Module模块，添加依赖：

![1614393933187](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614393933187.png)

```xml
    <!-- 指定仓库位置，依次为aliyun、cloudera和jboss仓库 -->
    <repositories>
        <repository>
            <id>aliyun</id>
            <url>http://maven.aliyun.com/nexus/content/groups/public/</url>
        </repository>
        <repository>
            <id>cloudera</id>
            <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
        </repository>
        <repository>
            <id>jboss</id>
            <url>http://repository.jboss.com/nexus/content/groups/public</url>
        </repository>
    </repositories>

    <!-- 版本属性 -->
    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <clickhouse>0.2.4</clickhouse>
    </properties>

    <dependencies>
        <!-- Clickhouse -->
        <dependency>
            <groupId>ru.yandex.clickhouse</groupId>
            <artifactId>clickhouse-jdbc</artifactId>
            <version>${clickhouse}</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.1</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                </configuration>
            </plugin>
        </plugins>
    </build>
```

> 导入依赖以后，查看clickhouse-jdbc相关jar包结构如下：

![1614394146590](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614394146590.png)



> 面试题：
>
> - 1）、手写MySQL JDBC 插入数据，整合Spark进行操作
>   - 将DataFrame数据保存到MySQL表中，考虑数据可以被更新
>   - MySQL表中，如果主键存在更新数据，如果不存在就是插入数据
>   - 使用Scala语言
> - 2）、手写MySQL JDBC方式查询数据
>   - 使用Java语言



## 10-[掌握]-Java 操作 CK之查询代码案例

> ​		编写Java程序：JDBC 代码，从ClickHouse表中读取数据，执行SQL语句：
>
> `select count(1) from default.ontime ;`

![1614394707656](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614394707656.png)

```scala
package cn.itcast.clickhouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 编写JDBC代码，从ClickHouse表查询分析数据
	 * step1. 加载驱动类
	 * step2. 获取连接Connection
	 * step3. 创建PreparedStatement对象
	 * step4. 查询数据
	 * step5. 获取数据
	 * step6. 关闭连接
 */
public class ClickHouseJDBCDemo {

	public static void main(String[] args) throws Exception{
		// 定义变量
		Connection conn = null ;
		PreparedStatement pstmt = null ;
		ResultSet result = null ;

		try {
			//step1. 加载驱动类
			Class.forName("ru.yandex.clickhouse.ClickHouseDriver");

			//step2. 获取连接Connection
			conn = DriverManager.getConnection(
				"jdbc:clickhouse://node2.itcast.cn:8123", "root", "123456"
			) ;

			//step3. 创建PreparedStatement对象
			pstmt = conn.prepareStatement("select count(1) from default.ontime");

			//step4. 查询数据
			result = pstmt.executeQuery();

			//step5. 获取数据
			while (result.next()){
				System.out.println(result.getLong(1));
			}
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			//step6. 关闭连接
			if(null != result) result.close();
			if(null != pstmt) pstmt.close();
			if(null != conn) conn.close();
		}
	}

}

```



## 11-[理解]-Spark 操作 CK之工程环境准备

> ​		继续调用JDBC Client API操作ClickHouse，将其与Spark程序进行集成。
>
> [任务说明：编写Spark程序，构建数据集DataFrame，调用ClickHouse JDBC API，进行DML操作和DDL操作]()

![1614395325532](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614395325532.png)

> 创建Maven Module模块，添加依赖

![1614395684737](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614395684737.png)

```xml
    <repositories>
        <repository>
            <id>mvnrepository</id>
            <url>https://mvnrepository.com/</url>
            <layout>default</layout>
        </repository>
        <repository>
            <id>cloudera</id>
            <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
        </repository>
        <repository>
            <id>elastic.co</id>
            <url>https://artifacts.elastic.co/maven</url>
        </repository>
    </repositories>

    <properties>
        <!--- Scala -->
        <scala.version>2.11.12</scala.version>
        <scala.binary.version>2.11</scala.binary.version>
        <!-- Spark -->
        <spark.version>2.4.0-cdh6.2.1</spark.version>
        <!-- Hadoop -->
        <hadoop.version>3.0.0-cdh6.2.1</hadoop.version>
        <!-- ClickHouse -->
        <clickhouse.version>0.2.4</clickhouse.version>
    </properties>

    <dependencies>
        <!-- 依赖Scala语言 -->
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>
        <!-- Spark Core 依赖 -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_${scala.binary.version}</artifactId>
            <version>${spark.version}</version>
        </dependency>
        <!-- Spark -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${scala.binary.version}</artifactId>
            <version>${spark.version}</version>
        </dependency>
        <!-- Hadoop Client 依赖 -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop.version}</version>
        </dependency>
        <!-- Clickhouse -->
        <dependency>
            <groupId>ru.yandex.clickhouse</groupId>
            <artifactId>clickhouse-jdbc</artifactId>
            <version>${clickhouse.version}</version>
            <exclusions>
                <exclusion>
                    <groupId>com.fasterxml.jackson.core</groupId>
                    <artifactId>jackson-databind</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>com.fasterxml.jackson.core</groupId>
                    <artifactId>jackson-core</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
    </dependencies>

    <build>
        <outputDirectory>target/classes</outputDirectory>
        <testOutputDirectory>target/test-classes</testOutputDirectory>
        <resources>
            <resource>
                <directory>${project.basedir}/src/main/resources</directory>
            </resource>
        </resources>
        <!-- Maven 编译的插件 -->
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.0</version>
                <configuration>
                    <source>1.8</source>
                    <target>1.8</target>
                    <encoding>UTF-8</encoding>
                </configuration>
            </plugin>
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.0</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```





> 日志属性文件：`log4j.properties`

```properties
log4j.rootLogger=${root.logger}
root.logger=WARN,console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n
shell.log.level=WARN
log4j.logger.org.spark-project.jetty=WARN
log4j.logger.org.spark-project.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO
log4j.logger.org.apache.parquet=ERROR
log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR
log4j.logger.org.apache.spark.repl.Main=${shell.log.level}
log4j.logger.org.apache.spark.api.python.PythonGatewayServer=${shell.log.level}

```



## 12-[掌握]-Spark 操作 CK之编写代码框架

> ​		创建Spark程序，模拟产生业务数据（比如交易订单数据），依据DataFrame数据操作ClickHouse数据库：DDL和DML操作。

![1614396002257](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614396002257.png)

```scala
package cn.itcast.clickhouse

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * SparkSQL加载JSON格式文件数据，依据Schema信息在ClickHouse中创建表，并进行数据CUD操作
 */
object ClickHouseSparkDemo {
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[2]")
    		.config("spark.sql.shuffle.partitions", "2")
    		.getOrCreate()
		import spark.implicits._
		import org.apache.spark.sql.functions._
		
		// 2. 加载JSON数据：交易订单数据
		val ordersDF: DataFrame = spark.read.json("datas/order.json")
		/*
			root
			 |-- areaName: string (nullable = true)
			 |-- category: string (nullable = true)
			 |-- id: long (nullable = true)
			 |-- money: string (nullable = true)
			 |-- timestamp: string (nullable = true)
		 */
		//ordersDF.printSchema()
		/*
			+--------+--------+---+-----+--------------------+
			|areaName|category|id |money|timestamp           |
			+--------+--------+---+-----+--------------------+
			|北京    |平板电脑|1  |1450 |2019-05-08T01:03.00Z|
			|北京    |手机    |2  |1450 |2019-05-08T01:01.00Z|
			|北京    |手机    |3  |8412 |2019-05-08T01:03.00Z|
			|上海    |电脑    |4  |1513 |2019-05-08T05:01.00Z|
			+--------+--------+---+-----+--------------------+
		 */
		// ordersDF.show(10, truncate = false)
		
		// 3. 依据DataFrame数据集，在ClickHouse数据库中创建表和删除表
		
		
		// 4. 保存DataFrame数据集到ClickHouse表中
		
		
		
		// 5. 更新数据到ClickHouse表中
		val updateDF: DataFrame = Seq(
			(3, 9999, "2020-12-08T01:03.00Z"),
			(4, 9999, "2020-12-08T01:03.00Z")
		).toDF("id", "money", "timestamp")
		
		
		// 6. 删除ClickHouse表中数据
		val deleteDF: DataFrame = Seq( Tuple1(1), Tuple1(2), Tuple1(3)).toDF("id")
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
	
}

```



## 13-[掌握]-Spark 操作 CK之工具类【基本方法】

> ​	创建对象：ClickHouserUtils，后续在其中实现创建表、删除表、插入数据、更新数据集删除数据代码。

```scala
package cn.itcast.clickhouse

import org.apache.spark.sql.DataFrame

/**
 * ClickHouse 工具类，创建表、删除表及对表数据进行CUD操作
 */
object ClickHouseUtils extends Serializable {
	
	
	
	/**
	 * 插入数据：DataFrame到ClickHouse表
	 */
	def insertData(dataframe: DataFrame,
	               dbName: String, tableName: String): Unit = {
		
	}
	
	/**
	 * 更新数据：依据主键，更新DataFrame数据到ClickHouse表
	 */
	def updateData(dataframe: DataFrame, dbName: String,
	               tableName: String, primaryField: String = "id"): Unit = {
		
	}
	
	/**
	 * 删除数据：依据主键，将ClickHouse表中数据删除
	 */
	def deleteData(dataframe: DataFrame, dbName: String,
	               tableName: String, primaryField: String = "id"): Unit = {
		
	}
	
}

```

> ​		需要依据DataFrame中Schema信息，要在ClickHouse中创建表和删除表操作，进行DDL操作时，就是执行DDL 语句即可，此时可以编写方法，专门执行DDL 语句即可。

```scala
	/**
	 * 传递Connection对象和SQL语句对ClickHouse发送请求，执行更新操作
	 *
	 * @param sql 执行SQL语句或者DDL语句
	 */
	def executeUpdate(sql: String): Unit = {
		// 声明变量
		var conn: ClickHouseConnection = null
		var pstmt: ClickHouseStatement = null
		try{
			// a. 获取ClickHouse连接对象
			conn = ClickHouseUtils.createConnection("node2.itcast.cn", "8123", "root", "123456")
			// b. 获取PreparedStatement实例对象
			pstmt = conn.createStatement()
			// c. 执行更新操作
			pstmt.executeUpdate(sql)
		}catch {
			case e: Exception => e.printStackTrace()
		}finally {
			// 关闭连接
			if (null != pstmt) pstmt.close()
			if (null != conn) conn.close()
		}
	}
```

![1614398390869](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614398390869.png)

> ​		无论进行DDL操作还是进行DML操作，都需要进行获取ClickHouse数据库连接，所以定义一个方法，专门获取连接Connection对象。

```scala
	/**
	 * 创建ClickHouse的连接实例，返回连接对象，通过连接池获取连接对象
	 *
	 * @param host ClickHouse 服务主机地址
	 * @param port ClickHouse 服务端口号
	 * @param username 用户名称，默认用户：default
	 * @param password 密码，默认为空
	 * @return Connection 对象
	 */
	def createConnection(host: String, port: String,
	                     username: String, password: String): ClickHouseConnection = {
		// TODO: 使用ClickHouse中提供ClickHouseDataSource获取连接对象
		val datasource: ClickHouseDataSource = new ClickHouseDataSource(s"jdbc:clickhouse://${host}:${port}")
		// 获取连接对象
		val connection: ClickHouseConnection = datasource.getConnection(username, password)
		// 返回对象
		connection
	}
```



## 14-[掌握]-Spark 操作 CK之工具类【创建表】

> ​		需要构建创建表DDL语句，依据DataFrame中Schema信息，如下为创建表语句模板：

```ini
CREATE TABLE IF NOT EXISTS test.tbl_order (
areaName String,
category String,
id Int64,
money String,
timestamp String,
sign Int8,
version Int8
)
ENGINE=VersionedCollapsingMergeTree(sign, version)
ORDER BY id ;
```



> 分析思路：创建ClickHouse表的关键就是构建创建表的DDL语句
>
> [核心点：依据DataFrame中Schema信息（字段名称和字段类型）转换为ClickHouse中列名称和列类型，拼凑字符串即可。]()

![1614398740028](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614398740028.png)

> 编程实现语句构建，代码如下：

```scala
	/**
	 * 依据DataFrame数据集中约束Schema信息，构建ClickHouse表创建DDL语句
	 *
	 * @param dbName 数据库的名称
	 * @param tableName 表的名称
	 * @param schema DataFrame数据集约束Schema
	 * @param primaryKeyField ClickHouse表主键字段名称
	 * @return ClickHouse表创建DDL语句
	 */
	def createTableDdl(dbName: String, tableName: String,
	                   schema: StructType, primaryKeyField: String = "id"): String = {
		/*
		areaName String,
		category String,
		id Int64,
		money String,
		timestamp String,
		 */
		val fieldStr: String = schema.fields.map{field =>
			// 获取字段名称
			val fieldName: String = field.name
			// 获取字段类型
			val fieldType: String = field.dataType match {
				case StringType => "String"
				case IntegerType => "Int32"
				case FloatType => "Float32"
				case LongType => "Int64"
				case BooleanType => "UInt8"
				case DoubleType => "Float64"
				case DateType => "Date"
				case TimestampType => "DateTime"
				case x => throw new Exception(s"Unsupported type: ${x.toString}")
			}
			// 组合字符串
			s"${fieldName} ${fieldType}"
		}.mkString(",\n")
	
		// 构建创建表DDL语句
		val createDdl: String = s"""
		  |CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
		  |${fieldStr},
		  |sign Int8,
		  |version UInt64
		  |)
		  |ENGINE=VersionedCollapsingMergeTree(sign, version)
		  |ORDER BY ${primaryKeyField}
		  |""".stripMargin
		
		// 返回DDL语句
		createDdl
	}
```



## 15-[掌握]-Spark 操作 CK之工具类【删除表】

> ​		当删除ClickHouse中表时，只要执行如下DDL语句即可：

```SQL
DROP TABLE IF EXISTS default.tbl_orders ;
```

> 在工具类中，编写一个方法，构建删除表的DDL语句，代码如下：

```scala
	/**
	 * 依据数据库名称和表名称，构建ClickHouse中删除表DDL语句
	 *
	 * @param dbName 数据库名称
	 * @param tableName 表名称
	 * @return 删除表的DDL语句
	 */
	def dropTableDdl(dbName: String, tableName: String): String = {
		s"DROP TABLE IF EXISTS ${dbName}.${tableName}"
	}
```



## 16-[掌握]-Spark 操作 CK之工具类【插入SQL】

> ​		实现将DataFrame数据集中数据保存到ClickHouse表中，其中需要将DataFrame中每条数据Row，构建一条插入INSERT语句：

```SQL
INSERT INTO db_ck.tbl_orders (areaName, category, id, money, timestamp, sign, version) VALUES ('北京', '平板电脑', 1, '1450', '2019-05-08T01:03.00', 1, 1);
```

![1614408360682](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614408360682.png)

> 实现代码，据图如下所示：

```scala
	/**
	 * 构建数据插入ClickHouse中SQL语句
	 *
	 * @param dbName 数据库名称
	 * @param tableName 表的名称
	 * @param columns 列名称
	 * @return INSERT 插入语句
	 */
	def createInsertSQL(dbName: String, tableName: String, columns: Array[String] ): String = {
		// 所有列名称字符串 -> areaName, category, id, money, timestamp
		val fieldsStr: String = columns.mkString(", ")
		// 所有列对应值的占位符 -> ?, ?, ?, ?, ?
		val valuesStr: String = columns.map(_ => "?").mkString(", ")
		// 插入INSERT SQL语句
		s"""
		   |INSERT INTO ${dbName}.${tableName} (${fieldsStr}, sign, version) VALUES (${valuesStr}, 1, 1);
		   |""".stripMargin
	}
```



## 17-[掌握]-Spark 操作 CK之工具类【插入数据】

> ​		接下来实现，将DataFrame数据转换为INSERT语句，并且对INSERT语句中占位符进行赋值操作，[尤其注意，针对DataFrame每个分区数据进行操作，最后批量插入到表中。]()

```scala
	/**
	 * 插入数据：DataFrame到ClickHouse表
	 */
	def insertData(dataframe: DataFrame, dbName: String, tableName: String): Unit = {
		// 获取DataFrame中Schema信息
		val schema: StructType = dataframe.schema
		// 获取DataFrame中所有列名称
		val columns: Array[String] = dataframe.columns
		
		// 构建INSERT语句
		val insertSql: String = createInsertSQL(dbName, tableName, columns)
		// TODO：针对每个分区数据进行操作，每个分区的数据进行批量插入
		dataframe.foreachPartition{iter =>
			// 声明变量
			var conn: ClickHouseConnection = null
			var pstmt: PreparedStatement = null
			try{
				// a. 获取ClickHouse连接对象
				conn = ClickHouseUtils.createConnection("node2.itcast.cn", "8123", "root", "123456")
				// b. 构建PreparedStatement对象
				pstmt = conn.prepareStatement(insertSql)
				// TODO: c. 遍历每个分区中数据，将每条数据Row值进行设置
				var counter: Int = 0
				iter.foreach{row =>
					// 从row中获取每列的值，和索引下标 -> 通过列名称 获取 Row中下标索引 , 再获取具体值
					columns.foreach{column =>
						// 通过列名称 获取 Row中下标索引
						val index: Int = schema.fieldIndex(column)
						// 依据索引下标，从Row中获取值
						val value: Any = row.get(index)
						// 进行PreparedStatement设置值
						pstmt.setObject(index + 1, value)
					}
					pstmt.setObject(columns.length + 1, 1)
					pstmt.setObject(columns.length + 2, 1)
					
					// 加入批次
					pstmt.addBatch()
					counter += 1
					// 判断counter大小，如果大于1000 ，进行一次批量插入
					if(counter >= 1000) {
						pstmt.executeBatch()
						counter = 0
					}
				}
				// d. 批量插入
				pstmt.executeBatch()
			}catch {
				case e: Exception => e.printStackTrace()
			}finally {
				// e. 关闭连接
				if(null != pstmt) pstmt.close()
				if(null != conn) conn.close()
			}
		}
	}
```

> 运行Spark程序，查看ClickHouse表中数据，如下图所示：

![1614410257772](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614410257772.png)



## 18-[掌握]-Spark 操作 CK之工具类【更新数据】

> ​		对ClickHouse表的数据进行更新操作，在ClickHouse数据库中，更新数据，使用ALTER语法实现。

```SQL
ALTER TABLE db_ck.tbl_orders UPDATE money = '9999', timestamp = '2020-12-08T01:03.00Z' WHERE id = 3 ;
```

> 更新订单数据，如下所示：

```scala
val updateDF: DataFrame = Seq(
			(3, 9999, "2020-12-08T01:03.00Z"),
			(4, 9999, "2020-12-08T01:03.00Z")
		).toDF("id", "money", "timestamp")
```

> 将要更新的数据Row，转换为ALTER UPDATE 更新语句。

![1614410564796](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614410564796.png)

> 首先完成更新数据方法整体结构代码，如下所示：

```scala
	/**
	 * 将DataFrame数据集更新至ClickHouse表中，依据主键进行更新
	 *
	 * @param dbName 数据库名称
	 * @param tableName 表名称
	 * @param dataframe 数据集DataFrame，更新的数据，其中要包含主键
	 */
	def updateData(dataframe: DataFrame, dbName: String,
	               tableName: String, primaryField: String = "id"): Unit = {
		// 对DataFrame每个分区数据进行操作
		dataframe.foreachPartition{iter =>
			// 声明变量
			var conn: ClickHouseConnection = null
			var pstmt: ClickHouseStatement = null
			try{
				// a. 获取ClickHouse连接对象
				conn = ClickHouseUtils.createConnection("node2.itcast.cn", "8123", "root", "123456")
				// b. 构建PreparedStatement对象
				pstmt = conn.createStatement()
				
				// TODO: 遍历分区中每条数据Row，构建更新Update语句，进行更新操作
				iter.foreach{row =>
					// b. 依据Row对象，创建更新SQL语句
					// ALTER TABLE db_ck.tbl_orders UPDATE money = '9999', timestamp = '2020-12-08T01:03.00Z' WHERE id = 3 ;
					val updateSql: String = createUpdateSQL(dbName, tableName, row)
					// d. 执行更新操作
					pstmt.executeUpdate(updateSql)
				}
			}catch {
				case e: Exception => e.printStackTrace()
			}finally {
				// e. 关闭连接
				if(null != pstmt) pstmt.close()
				if(null != conn) conn.close()
			}
		}
	}
```



## 19-[掌握]-Spark 操作 CK之工具类【更新语句】

> ​		依据Row对象（每条数据）构建UPDATE更新语句，思路如下所示：

![1614411895240](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614411895240.png)

> 具体代码实现，如下所示：

```scala
	/**
	 * 构建数据更新ClickHouse表中SQL语句
	 *
	 * @param dbName 数据库名称
	 * @param tableName 表名称
	 * @param row DataFrame中每行数据
	 * @param primaryKeyField 主键列
	 * @return update更新SQL语句
	 */
	def createUpdateSQL(dbName: String, tableName: String,
	                    row: Row, primaryKeyField: String = "id"): String = {
		/*
				 id     money    timestamp
			Row: 3,    9999, "2020-12-08T01:03.00Z"
						     |
			     money = '9999', timestamp = '2020-12-08T01:03.00Z'
		 */
		val updatesStr: String = row.schema.fields
    		.map(field => field.name) // 获取所有列名称
			// 过滤主键列名称
    		.filter(columnName => ! primaryKeyField.equals(columnName))
			// 依据列名称获取对应的值
    		.map{columnName =>
			    val columnValue: Any = getFieldValue(row, columnName)
			    s"${columnName} = '${columnValue}'"
		    }.mkString(", ")
		// 获取主键的值
		val primaryKeyValue: Any = getFieldValue(row, primaryKeyField)
		// 构建UPDATE更新SQL语句
		s"""
		   |ALTER TABLE ${dbName}.${tableName}
		   |    UPDATE ${updatesStr} WHERE ${primaryKeyField} = ${primaryKeyValue} ;
		   |""".stripMargin
	}
```





## 20-[掌握]-Spark 操作 CK之工具类【删除数据】

> ​		在ClickHouse数据库中，如果对表中数据进行删除时，主要依据主键进行删除，依然与更新数据类似使用ALTER语法实现删除操作

```SQL
ALTER TABLE db_ck.tbl_orders DELETE WHERE id = "3" ;
```

> 构建出要删除DataFrame，只包含一个字段信息，就是主键id，代码如下所示：

```scala
val deleteDF: DataFrame = Seq( Tuple1(1), Tuple1(2), Tuple1(3)).toDF("id")
```

> 实现删除数据代码，方法：deleteData，如下所示：

```scala
	/**
	 * 删除数据：依据主键，将ClickHouse表中数据删除
	 */
	def deleteData(dataframe: DataFrame, dbName: String,
	               tableName: String, primaryField: String = "id"): Unit = {
		// 对DataFrame每个分区数据进行操作
		dataframe.foreachPartition{iter =>
			// 声明变量
			var conn: ClickHouseConnection = null
			var pstmt: ClickHouseStatement = null
			try{
				// a. 获取ClickHouse连接对象
				conn = ClickHouseUtils.createConnection("node2.itcast.cn", "8123", "root", "123456")
				// b. 构建PreparedStatement对象
				pstmt = conn.createStatement()
				
				// TODO: 遍历分区中每条数据Row，构建更新Update语句，进行更新操作
				iter.foreach{row =>
					// b. 依据Row对象，创建删除SQL语句
					// ALTER TABLE db_ck.tbl_orders DELETE WHERE id = "3" ;
					val deleteSql: String = s"ALTER TABLE ${dbName}.${tableName} DELETE WHERE ${primaryField} = ${getFieldValue(row, primaryField)} ;"
					// d. 执行更新操作
					pstmt.executeUpdate(deleteSql)
				}
			}catch {
				case e: Exception => e.printStackTrace()
			}finally {
				// e. 关闭连接
				if(null != pstmt) pstmt.close()
				if(null != conn) conn.close()
			}
		}
	}
```





