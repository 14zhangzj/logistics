---
1stypora-copy-images-to: img
typora-root-url: ./
---



# Logistics_Day04：Kudu 入门使用



## 01-[复习]-上次课程内容回顾

> 物流项目数据采集：实时增量采集数据，业务数据存储在数据库中：
>
> - 1）、物流Logistics系统业务数据：Oracle数据库，[采用OGG采集，以JSON字符串发送到Kafka Topic]()
> - 2）、CRM系统业务数据：MySQL数据库，[采用Canal采集，以JSON字符串发送到Kafka Topic]()
>
> 此外，物流项目大数据服务器，基于CM安装CDH大数据组件，着重CM安装CDH架构及本质原理。

![1612399459152](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612399459152.png)

> 思维导图

![1612401151615](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612401151615.png)

## 02-[了解]-第4天：课程内容提纲

> ​			主要讲解新的大数据存储引擎：Kudu 存储引擎，从2015年就诞生，由Cloudera开源存储引擎，取代HDFS文件系统和NOSQL数据库HBase。
>
> - 1）、ETL 实现方案
>
>   - 前期已经将业务数据采集到Kafka消息队列中，此时需要将业务数据ETL存储
>   - ETL数据处理流程
>
> - 2）、Kudu 介绍
>
>   - Kudu是什么？为什么会出现？诞生背景？
>   - Kudu 相关技术发展史
>   - Kudu 架构设计（类比HBase数据库即可），自身实现数据一致性和容错性
>   - CM安装Kudu及Kudu基本使用（KuduPlus）
>
> - 3）、Java 操作 Kudu
>
>   - 主要使用Java Client API对Kudu进行DDL和DML操作
>   - DDL 操作：创建表Create、删除表Delete和修改表Alter
>   - DML操作：CRUD增删改查
>
> - 4）、Spark 集成Kudu
>
>   - 基于RDD集成，KuduContext
>
>   - DataFrame和SparkSession
>
>     ```scala
>     spark.read.format("kudu").option().load
>     
>     dataframe.write.format("kudu").option().load
>     ```
>
>     





## 03-[掌握]-数据实时ETL 处理流程图

> ​			物流大数据项目中，数据属于实时采集业务系统数据（分别使用OGG和Canal），存储到Kafka 分布式消息队列中。编写流式计算程序（此处使用StructuredStreaming结构化流），实时消费Kafka数据，将数据进行ETL转换操作，将最终数据存储不同存储引擎（Kudu 数据库、ClickHouse数据库和Elasticsearch索引）。
>
> - 第一点、StructuredStreaming与Elasticsearch和Kudu集成库，直接使用即可；但是与ClickHouse集成需要自己开发代码，实现集成。
> - 第二点、针对流式应用来说，核心关注三个方面：
>   - step1、数据源Source，统一基本上都是Kafka，消费数据偏移量管理，都是JSON数据或者文本数据
>   - step2、数据ETL（转换），需要依据业务对数据进行处理
>     - 针对StructuredStreaming来说，建议编写DSL编程，需要自定义UDF函数
>   - step3、数据终端Sink，将数据存储，基本上都提供库直接使用即可。

![1612402963455](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612402963455.png)

> 面试题：==为什么使用StructuredStreaming实时ETL，而不是SparkStreaming或者Flink呢？？？？？==
>
> - 第一、StructuredStreaming从Spark2.0版本提供，针对结构化流式数据进行实时处理Spark模块，将数据封装到数据结构DataFrame，底层重用SparkSQL分析引擎Catalyst，使用程序可以进行自动优化，提升性能。并且Spark2.2版本成为Release版本，可以使用于生成环境，此外Spark2.3中提供持续流处理Continues Processing（来一条数据处理一条数据，能够实现真正流式计算），目前Spark 3.0中StructuredStreaming性能又进一步提升，所以选择StructuredStreaming。
> - 第二、SparkStreaming属于老的Spark中针对流式数据处理模块，底层依然是RDD，编程来说需要更加底层，此外有些功能无法实现的，比如窗口统计时，不能基于事件时间窗口分析，只能基于处理数据分析；SparkStreaming分析统计属于无状态统计，需要自己管理状态。Spark 2.0中提出StructuredStreaming就是为了取代SparkStreaming模块，从2.0开始，SparkStreaming一直属于维护状态，没有添加任何新的特性。
> - 第三、本项目实时ETL对实时性要求不是很高，延迟性在毫秒或秒都可以接收，没有必要使用Flink实时高流式计算框架。Spark框架出现很久了，很多框架都提供与Spark集成库，比如Redis、Elasticsearch和Kudu都提供，并且SparkSQL提供一套完整外部数据源接口API，只要用户实现以后，就可以方便加载被保存外部数据源的数据。



## 04-[理解]-为什么使用Kudu及技术发展

> ​		在大数据技术发展中，针对SQL on Hadoop技术发展来说，一开始都是使用Hive分析数据（数据存储在HDFS或HBase），后来受到Google论文Dreml启发，开发Prestor和Impala基于内存并行计算分析引擎。又由于实际项目中既需要对数据进行批量加载分析，又需要进行随机读写查询，出现Kudu 存储引擎。

![1612404768871](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612404768871.png)

> ​		用户行为【日志的快速分析】：在Kudu框架没有出现之前，如果既要做离线分析，又要做实时分析，往往需要将数据存储在两个地方（HDFS文件系统：Parquet、HBase数据库：随机读写），比较好的满足需求。

![1612404944015](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612404944015.png)

> 当Kudu出现以后，直接将业务数据存储到Kudu中即可，满足批量加载分析，又能进行随机读写：

![1612405066905](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612405066905.png)



## 05-[掌握]-Kudu 是什么及应用场景 

> Apache Kudu是由Cloudera开源的存储引擎，可以同时提供低延迟的随机读写和高效的数据分析能力。
>
> [Kudu支持水平扩展，使用Raft协议进行一致性保证，并且与Cloudera Impala和Apache Spark等当前流行的大数据查询和分析工具结合紧密。]()

![1612405286765](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612405286765.png)

> 在Kudu没有出现之前，大数据存储引擎分为如下两种：

![1612405533284](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612405533284.png)

> ​		从上面分析可知，两种数据在存储方式上完全不同，进而导致使用场景完全不同，但在真实的场景中，边界可能没有那么清晰，面对==既需要随机读写，又需要批量分析的大数据场景==，该如何选择呢？

![1612405583101](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612405583101.png)



> ​		数据实时写入HBase，实时的数据更新也在HBase完成，为了应对OLAP需求，定时（通常是T+1或者T+H）将HBase数据写成静态的文件（如：Parquet）导入到OLAP引擎（如：HDFS）。
>
> [Kudu的定位是Fast Analytics on Fast Data，是一个既支持随机读写、又支持 OLAP 分析的大数据存储引擎。]()

![1612405726185](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612405726185.png)

> KUDU 是一个==折中的产品==，在 HDFS 和 HBase 这两个偏科生中平衡了随机读写和批量分析的性能。
>
> Kudu 应用场景：

![1612406021978](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612406021978.png)





## 06-[掌握]-Kudu 数据模型及分区策略 

> Kudu 数据存储引擎，属于一个折中产品，介于HDFS和HBase框架，既能够批量数据加载分析，又能随机读写
>
> - 1）、Kudu Master：主节点，一个节点，管理集群的元数据
>   - [考虑高可用HA，可以有多个主节点，一个节点为Active，其他节点为Standby]()
> - 2）、Kudu TabletServer：从节点，多个节点，存储实际数据
>
> [理解Kudu存储引擎时，对比HBase数据库相关概念即可。]()

- 第一点、数据模型

> ​			在Kudu中数据放在表Table中进行存储的，每个表中有字段，每个字段有名称和类型及其他信息，每个表中必须指定主键，主键可以是一个字段，也可以是多个字段组成。
>
> [一个 KUDU 集群由多个表组成，每个表由多个字段组成，一个表必须指定一个由若干个（>=1）字段组成的主键]()

![1612407445258](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612407445258.png)

> ​		KUDU 表中的每个字段是强类型的，而不是 HBase 那样所有字段都认为是 bytes。好处是可以对不同类型数据进行不同的编码，节省空间。

![1612407869134](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612407869134.png)

> 在Kudu中，如何将Table划分为多个分区Tablet的呢？？使用什么策略划分？？
>
> - 1）、范围分区，类似HBase Range分区方式

![1612408040313](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612408040313.png)

> - 2）、哈希分区，对某些字段获取哈希值，属于同一个值就是一个分区Tablet

![1612408084060](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612408084060.png)

> - 3）、多级分区，可以组合范围分区与哈希分区或者多个哈希分区组合等等

![1612408116669](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612408116669.png)

> [在Kudu中对每个Table进行分区时，策略相对比较灵活。]()
>
> KUDU 是一个列式存储的存储引擎，其数据存储方式如下：

![1612408149555](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612408149555.png)





## 07-[掌握]-Kudu 框架整体架构设计 

> KUDU 中存在两个角色：
>
> - ==Mater Server==：负责集群管理、元数据管理等功能
> - ==Tablet Server==：负责数据存储，并提供数据读写服务

![1612408861444](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612408861444.png)

> 由于Tablet任务比较繁重，所以在使用Kudu存储数据，需要对Tablet进行限制，主要如下图所示：

![1612409154766](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612409154766.png)

> 在Kudu中，每个Tablet数目，在创建表的时候就需要确定，不会自己增长的。



## 08-[掌握]-Kudu 服务启动及相关配置

> ​		本项目中（物流项目演示），使用CM 安装Kudu（Kudu是Cloudera公司开发存储引擎），在单机（node2.itcast.cn）进行安装部署，所以只有一个Master和一个TabletServer。
>
> - 1）、第一步、启动node2.itcast.cn机器，登录CM：http://node2.itcast.cn:7180/cmf/login

![1612409364969](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612409364969.png)

> - 2）、第二步、基于CM UI界面启动服务
>
>   [Kudu 存储引擎框架，当进行分部署安装部署时，对集群时间同步要求极高，必须所有机器时间要是同步]()
>
>   启动完成以后，KuduMaster提供WEB UI界面：http://node2.itcast.cn:8051/

![1612409597432](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612409597432.png)



> - 1）、Kudu 安装以后Client 配置文件：`/etc/kudu/conf`

![1612409745475](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612409745475.png)

> - 2）、Kudu 数据存储目录：`/var/lib/kudu/`
> - 3）、Kudu 服务日志目录：`/var/log/kudu`

![1612409873347](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612409873347.png)

> - 4）、Kudu 框架提供`kudu` 命令行

```ini
[root@node2 ~]# kudu
Usage: /opt/cloudera/parcels/CDH-6.2.1-1.cdh6.2.1.p0.1425774/bin/../lib/kudu/bin/kudu <command> [<args>]

<command> can be one of the following:
         cluster   Operate on a Kudu cluster
        diagnose   Diagnostic tools for Kudu servers and clusters
              fs   Operate on a local Kudu filesystem
             hms   Operate on remote Hive Metastores
   local_replica   Operate on local tablet replicas via the local filesystem
          master   Operate on a Kudu Master
             pbc   Operate on PBC (protobuf container) files
            perf   Measure the performance of a Kudu cluster
  remote_replica   Operate on remote tablet replicas on a Kudu Tablet Server
           table   Operate on Kudu tables
          tablet   Operate on remote Kudu tablets
            test   Various test actions
         tserver   Operate on a Kudu Tablet Server
             wal   Operate on WAL (write-ahead log) files
```

命令使用：

![1612410026456](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612410026456.png)

> ​		KUDU Client 在与服务端交互时，先从 Master Server 获取元数据信息，然后去 Tablet Server读写数据

![1612410080266](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612410080266.png)



## 09-[掌握]-Kudu 使用方式及KuduPlus 工具

> Kudu存储引擎来说，使用Kudu官方提供三种方式：
>
> - 1）、Java /C++/Python Client API，最基本方式
>
>   - 针对Java Client API来说，`KuduClient，指定KuduMaster地址：node2.itcast.cn:7051`
>
> - 2）、Kudu 与Impala 集成，天然集成，Impala分析存储在Kudu表的数据，一对CP
>
>   - 在Impala中创建表与Kudu表进行关联即可
>
>   ```SQL
>   CREATE EXTERNAL TABLE `itcast_users` STORED AS KUDU
>   TBLPROPERTIES(
>       'kudu.table_name' = 'itcast_users',
>       'kudu.master_addresses' = 'node2.itcast.cn:7051')
>   ```
>
>   
>
> - 3）、Kudu 与Spark 集成，提供类库，直接调用API，加载和保存Kudu 表数据
>
>   - 第一方式，基于RDD集成，KuduContext上下文对象，数据封装在RDD
>   - 第二方式，基于DataFrame集成，SparkSession加载数据

> ==Kudu-Plus==一款针对Kudu可视化工具，GitHub地址：https://github.com/Xchunguang/kudu-plus

![1612411358552](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612411358552.png)

![1612411545946](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612411545946.png)



## 10-[掌握]-Java 操作 Kudu之创建Maven Project 

> Kudu提供Java Client API，以便用户可以进行DDL操作（创建表和删除表）及DML操作。

创建Maven Project设置GAV如下图所示：

![1612411674589](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612411674589.png)

创建Maven Module模块，用于编写Java API 操作Kudu，模块AGV设置如下所示：

![1612411802819](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612411802819.png)

​		构建Maven Project工程或Maven Module模块，POM文件添加依赖如下：

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
        <kudu.version>1.9.0-cdh6.2.1</kudu.version>
        <junit.version>4.12</junit.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.kudu</groupId>
            <artifactId>kudu-client</artifactId>
            <version>${kudu.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.kudu</groupId>
            <artifactId>kudu-client-tools</artifactId>
            <version>${kudu.version}</version>
        </dependency>

        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.12</version>
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

> 为了操作方便，配置IDEA远程连接虚拟机node2.itcast.cn，上传下载方便及简易命令：

![1612412085837](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612412085837.png)



## 11-[掌握]-Java 操作 Kudu之创建KuduClient实例

> 首先在Maven Module模块中创建相关包名，结构如下所示：

![1612412290745](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612412290745.png)

> 在进行DDL和DML操作之前，需要创建KuduClient对象，连接Kudu Master主节点。
>
> [注意，无论是DDL还是DML操作，使用JUNIT测试完成，先创建KuduClient对象，最后关闭连接。]()

```java
package cn.itcast.kudu.table;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.junit.Before;
import org.junit.Test;

/**
 * 基于Java API对Kudu进行CRUD操作，包含创建表及删除表的操作
 */
public class KuduTableDemo {

	private KuduClient kuduClient = null ;

	/**
	 * 初始化KuduClient实例对象，传递Kudu Master地址信息
	 */
	@Before
	public void init(){
		// Kudu Master地址信息
		String masterAddresses = "node2.itcast.cn:7051" ;
		// 传递Kudu Master地址，构架KuduClient实例对象
		kuduClient = new KuduClient.KuduClientBuilder(masterAddresses)
			// 设置对Kudu操作超时时间
			.defaultOperationTimeoutMs(10000)
			// 建造者设计模式构建实例对象，方便设置参数
			.build();
	}

	@Test
	public void testKuduClient(){
		System.out.println(kuduClient);
	}

	/**
	 * 当对Kudu进行DDL操作后，需要关闭连接
	 */
	public void close() throws KuduException {
		if(null != kuduClient) kuduClient.close();
	}

}
```



## 12-[掌握]-Java 操作 Kudu之创建表(Hash分区) 

> 首先使用KuduClient实例对象，在Kudu中创建表，表的说明如下：

![1612420454141](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612420454141.png)

> 在Kudu 的Java Client API中，使用OOP思想，进行封装：
>
> - 1）、每列信息，封装在：ColumnSchema
> - 2）、所有列信息，封装在：Schema
> - 3）、分区和副本，封装 在：CreateTableOptions

![1612420659082](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612420659082.png)

> 使用API，创建表，具体代码如下：

```scala
	/**
	 * 依据列名称、列类型和是否为主键构建ColumnSchema对象
	 * @param name 列名称
	 * @param type 列类型
	 * @param key 是否为主键
	 */
	private ColumnSchema newColumnSchema(String name, Type type, boolean key){
		// 获取Builder对象
		ColumnSchema.ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder(name, type);
		// 设置是否为主键
		builder.key(key);
		// 返回对象
		return builder.build();
	}

	/**
	 * 创建Kudu中的表，表的结构如下所示：
	 create table itcast_user(
	 id int,
	 name string,
	 age byte,
	 primary key(id)
	 )
	 partition by hash(id) partitions 3
	 stored as kudu ;
	 */
	@Test
	public void createKuduTable() throws KuduException {
		// a. 定义表的Schema信息：字段名称和类型，是否为主键
		List<ColumnSchema> columns = new ArrayList<>();
		columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
		columns.add(newColumnSchema("name",Type.STRING, false));
		columns.add(newColumnSchema("age",Type.INT8, false));
		Schema schema = new Schema(columns) ;

		// b. 定义分区策略和副本数
		CreateTableOptions options = new CreateTableOptions() ;
		// 设置分区策略，此处使用哈希分区
		options.addHashPartitions(Arrays.asList("id"), 3);
		// 设置副本数，必须为奇数
		options.setNumReplicas(1) ;

		// 调用KuduClient中createTable方法创建表
		/*
			public KuduTable createTable(String name, Schema schema, CreateTableOptions builder)
		 */
		KuduTable kuduTable = kuduClient.createTable("itcaset_users", schema, options);
		System.out.println("Table ID: " + kuduTable.getTableId());
	}

```



## 13-[掌握]-Java 操作 Kudu之删除表 

> 如果要删除Kudu中表，首先判断表是否存在，如果不存在，则删除。具体演示代码如下：

```scala
	/**
	 * 删除Kudu中的表
	 */
	@Test
	public void dropKuduTable() throws KuduException {
		if(kuduClient.tableExists("itcaset_users")){
			DeleteTableResponse response = kuduClient.deleteTable("itcaset_users");
			System.out.println(response.getElapsedMillis());
		}
	}
```



## 14-[掌握]-Java 操作 Kudu之插入数据 

> 向Kudu表中插入数据，如果编写SQL：`INSERT INTO itcast_users(id, name, age)VALUES(1, “zhangsan”, 26) ;`

![1612421761460](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612421761460.png)

> 在Java API中，对插入、更新及删除和插入更新进行OOP封装，分别类：Insert、Update、Delete等，
>
> - 1）、获取表的句柄：KuduTable
> - 2）、进行插入、更新和删除时，需要获取会话实例对象KuduSession，可以认为PreparedStatement
> - 3）、可以将数据缓存，批量更新插入
> - 4）、关闭连接资源

```scala
	/**
	 * 将数据插入到Kudu Table中： INSERT INTO (id, name, age) VALUES (1001, "zhangsan", 26)
	 */
	@Test
	public void insertData() throws KuduException {
		// a. 获取表的句柄
		KuduTable kuduTable = kuduClient.openTable("itcast_users");

		// b. 会话实例对象
		KuduSession kuduSession = kuduClient.newSession();
		kuduSession.setMutationBufferSpace(1000);
		kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

		Random random = new Random();
		for(int i = 1 ; i <= 100; i ++){
			// TODO： c. 构建INSERT对象
			Insert insert = kuduTable.newInsert();
			PartialRow insertRow = insert.getRow();
			// 设置值
			insertRow.addInt("id", i);
			insertRow.addString("name", "zhangsan-" + i);
			insertRow.addByte("age", (byte)(random.nextInt(10) + 20));
			// d. 添加到缓冲中
			kuduSession.apply(insert) ;
		}

		// 手动刷新提交数据
		kuduSession.flush();

		// e. 关闭连接
		kuduSession.close();
	}
```



## 15-[掌握]-Java 操作 Kudu之全量查询数据

> 前面已经将数据插入到Kudu表，从Kudu表中查询数据，有两种方式：全量查询数据和过滤查询数据。
>
> [从Kudu表中查询数据时，类似从HBase表中查询数据，获取扫描器Scanner对象，进行迭代获取数据。]()
>
> 当从Kudu表查询数据，属于每个分区单独查询单独获取数据，示意图如下：

![1612422739021](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612422739021.png)

> 演示代码如下：

```java
	/**
	 * 从Kudu Table中查询数据
	 */
	@Test
	public void selectData() throws KuduException {
		// step1. 获取表句柄
		KuduTable kuduTable = kuduClient.openTable("itcast_users");

		// step2. 获取表的扫描器对象
		KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
		KuduScanner kuduScanner = scannerBuilder.build();

		// step3. 遍历迭代器获取数据
		int index = 1 ;
		while (kuduScanner.hasMoreRows()){
			System.out.println("Index = " + (index ++ ));
			RowResultIterator rowResults = kuduScanner.nextRows();
			// 遍历每个Tablet获取的数据
			while (rowResults.hasNext()){
				RowResult rowResult = rowResults.next();
				System.out.println(
					"id = " + rowResult.getInt("id")
					+ ", name = " + rowResult.getString("name")
					+ ", age = " + rowResult.getByte("age")
				);
			}
		}
	}
```



## 16-[掌握]-Java 操作 Kudu之过滤查询数据 

> 在实际从Kudu表查询数据时，依据条件过滤查询，在Kudu中支持两种方式过滤查询数据：
>
> - 1）、投影`project`，[选择字段Select]()，从Kudu表中选择查询哪些字段
>
> - 2）、谓词`predicate`，[设置Where条件]()，类似SQL语句中WHERE过滤条件
>
>   `SELECT * FROM xx WHERE x1 ADN x2 AND x3 ;`
>
> 演示案例：==只查询id和age两个字段的值，年龄age小于25，id大于50==，演示代码如下：

```scala
	/**
	 * 从Kudu Table中查询数据
	 */
	@Test
	public void queryData() throws KuduException {
		// step1. 获取表句柄
		KuduTable kuduTable = kuduClient.openTable("itcast_users");

		// step2. 获取表的扫描器对象
		KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
		// TODO： 只查询id和age两个字段的值 -> project ，年龄age小于25，id大于50 -> predicate
		// a. 设置选取字段
		List<String> columnNames = new ArrayList<>();
		columnNames.add("id");
		columnNames.add("age") ;

		// b. 设置过滤条件
		// id大于50
		KuduPredicate predicateId = KuduPredicate.newComparisonPredicate(
			newColumnSchema("id", Type.INT32, true), //
			KuduPredicate.ComparisonOp.GREATER, //
			50
		) ;

		// 年龄age小于25
		KuduPredicate predicateAge = KuduPredicate.newComparisonPredicate(
			newColumnSchema("age", Type.INT8, false), //
			KuduPredicate.ComparisonOp.LESS, //
			(byte) 25
		) ;

		KuduScanner kuduScanner = scannerBuilder
			.setProjectedColumnNames(columnNames)
			.addPredicate(predicateId)
			.addPredicate(predicateAge)
			.build();

		// step3. 遍历迭代器获取数据
		int index = 1 ;
		while (kuduScanner.hasMoreRows()){
			System.out.println("Index = " + (index ++ ));
			RowResultIterator rowResults = kuduScanner.nextRows();
			// 遍历每个Tablet获取的数据
			while (rowResults.hasNext()){
				RowResult rowResult = rowResults.next();
				System.out.println(
					"id = " + rowResult.getInt("id")
						+ ", age = " + rowResult.getByte("age")
				);
			}
		}
	}
```



## 17-[掌握]-Java 操作 Kudu之更新及删除数据 

> 除了可以对Kudu表的数据进行插入以外，还可以进行更新Update、插入更新Upsert和删Delete操作。
>
> - 1）、更新数据：`Update`

```java
	/**
	 * 在Kudu中更新数据时，只能依据主键进行更新
	 */
	@Test
	public void updateData() throws KuduException {
		// a. 获取表的句柄
		KuduTable kuduTable = kuduClient.openTable("itcast_users");

		// b. 会话实例对象
		KuduSession kuduSession = kuduClient.newSession();
		kuduSession.setMutationBufferSpace(1000);
		kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

		// TODO： c. 构建INSERT对象
		Update update = kuduTable.newUpdate();
		PartialRow updateRow = update.getRow();
		// 设置值
		updateRow.addInt("id", 1);
		updateRow.addString("name", "zhangsan疯");
		updateRow.addByte("age", (byte)120);
		// d. 添加到缓冲中
		kuduSession.apply(update) ;

		// 手动刷新提交数据
		kuduSession.flush();

		// e. 关闭连接
		kuduSession.close();
	}
```



> 在Kudu中提供方法：Upsert方法，表示当Kudu表中，主键存在时，更新数据；不存在时，插入数据。

```scala
	/**
	 * 在Kudu中更新插入数据时，只能依据主键进行操作
	 */
	@Test
	public void upsertData() throws KuduException {
		// a. 获取表的句柄
		KuduTable kuduTable = kuduClient.openTable("itcast_users");

		// b. 会话实例对象
		KuduSession kuduSession = kuduClient.newSession();
		kuduSession.setMutationBufferSpace(1000);
		kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

		// TODO： c. 构建INSERT对象
		Upsert upsert = kuduTable.newUpsert();
		PartialRow upsertRow = upsert.getRow();
		// 设置值
		upsertRow.addInt("id", 201);
		upsertRow.addString("name", "铁拐");
		upsertRow.addByte("age", (byte)100);
		// d. 添加到缓冲中
		kuduSession.apply(upsert) ;

		// 手动刷新提交数据
		kuduSession.flush();

		// e. 关闭连接
		kuduSession.close();
	}
```



> 在Kudu中，只能依据主键删除数据，演示案例代码如下：

```scala
	/**
	 * 在Kudu中更新插入数据时，只能依据主键进行操作
	 */
	@Test
	public void deleteData() throws KuduException {
		// a. 获取表的句柄
		KuduTable kuduTable = kuduClient.openTable("itcast_users");

		// b. 会话实例对象
		KuduSession kuduSession = kuduClient.newSession();
		kuduSession.setMutationBufferSpace(1000);
		kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

		// TODO： c. 构建INSERT对象
		Delete delete = kuduTable.newDelete();
		PartialRow deleteRow = delete.getRow();
		// 设置值
		deleteRow.addInt("id", 201);
		// d. 添加到缓冲中
		kuduSession.apply(delete) ;

		// 手动刷新提交数据
		kuduSession.flush();

		// e. 关闭连接
		kuduSession.close();
	}
```



## 18-[掌握]-Java 操作 Kudu之创建表(范围分区)

> - 1）、哈希分区：Hash Partitioning
>
>   - 哈希分区通过`哈希值`，将行分配到不同的 buckets ( 存储桶 )中；
>
>   ![1612425319205](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612425319205.png)
>
> - 2）、范围分区：Range Partitioning
>
>   - 范围分区可根据存入数据的数据量，均衡的存储到各个机器上，防止机器出现负载不均衡现象；
>   - [分区键必须是主键 或 主键的一部分；]()
>   - 案例演示：==【Range分区的方式：id】==
>
>   ```
>   id < 100   第1个分区
>   100 <= id < 500  第2个分区
>   id >= 500   第3分区
>   ```

```java
	/**
	 * 创建Kudu中的表，采用范围分区策略
	 */
	@Test
	public void createKuduTableRange() throws KuduException {
		// a. 定义表的Schema信息：字段名称和类型，是否为主键
		List<ColumnSchema> columns = new ArrayList<>();
		columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
		columns.add(newColumnSchema("name",Type.STRING, false));
		columns.add(newColumnSchema("age",Type.INT8, false));
		Schema schema = new Schema(columns) ;

		// b. 定义分区策略和副本数
		CreateTableOptions options = new CreateTableOptions() ;
		// TODO: 设置分区策略，此处使用Range范围分区
		// 设置范围分区字段，必须是主键或主键部分字段
		options.setRangePartitionColumns(Arrays.asList("id")) ;
		// 设置范围
		/**
		 * value < 100
		 * 100 <= value < 500
		 * 500 <= value
		 */
		// id < 100
		PartialRow upper100 = new PartialRow(schema);
		upper100.addInt("id", 100);
		options.addRangePartition(new PartialRow(schema), upper100) ;

		// 100 <= id < 500
		PartialRow lower100 = new PartialRow(schema);
		lower100.addInt("id", 100);
		PartialRow upper500 = new PartialRow(schema);
		upper500.addInt("id", 500);
		options.addRangePartition(lower100, upper500) ;

		//  500 <= id
		PartialRow lower500 = new PartialRow(schema);
		lower500.addInt("id", 500);
		options.addRangePartition(lower500, new PartialRow(schema)) ;

		// 设置副本数，必须为奇数
		options.setNumReplicas(1) ;

		// 调用KuduClient中createTable方法创建表
		/*
			public KuduTable createTable(String name, Schema schema, CreateTableOptions builder)
		 */
		KuduTable kuduTable = kuduClient.createTable("itcast_users_range", schema, options);
		System.out.println("Table ID: " + kuduTable.getTableId());
	}
```



## 20-[掌握]-Java 操作 Kudu之创建表(多级分区) 

> - 3）、多级分区：Multilevel Partitioning
>   - Kudu 允许一个表上组合使用Hash分区 及 Range分区；
>   - 分区键必须是主键 或 主键的一部分
>   - ==多级分区可以保留各个分区类型的优点，同时减少每个分区的缺点；==
>   - 案例如下：

![1612425988437](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612425988437.png)

```scala
	/**
	 * 创建Kudu中的表，先进行哈希分区，再进行范围分区
	 */
	@Test
	public void createKuduTableMulti() throws KuduException {
		// a. 定义表的Schema信息：字段名称和类型，是否为主键
		List<ColumnSchema> columns = new ArrayList<>();
		columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
		columns.add(newColumnSchema("age",Type.INT8, true));
		columns.add(newColumnSchema("name",Type.STRING, false));
		Schema schema = new Schema(columns) ;

		// b. 定义分区策略和副本数
		CreateTableOptions options = new CreateTableOptions() ;
		// TODO: 先进行设置哈希分区策略
		options.addHashPartitions(Arrays.asList("id"), 5) ;

		// TODO: 再设置Range范围分区
		// 设置范围分区字段，必须是主键或主键部分字段
		options.setRangePartitionColumns(Arrays.asList("age")) ;
		// 设置范围
		/**
		 * age < 20
		 * 20 <= value < 40
		 * 40 <= value
		 */
		// age < 20
		PartialRow upper20 = new PartialRow(schema);
		upper20.addByte("age", (byte)20);
		options.addRangePartition(new PartialRow(schema), upper20) ;

		// 100 <= id < 500
		PartialRow lower20 = new PartialRow(schema);
		lower20.addByte("age", (byte)20);
		PartialRow upper40 = new PartialRow(schema);
		upper40.addByte("age", (byte)40);
		options.addRangePartition(lower20, upper40) ;

		//  500 <= id
		PartialRow lower40 = new PartialRow(schema);
		lower40.addByte("age", (byte)40);
		options.addRangePartition(lower40, new PartialRow(schema)) ;

		// 设置副本数，必须为奇数
		options.setNumReplicas(1) ;

		// 调用KuduClient中createTable方法创建表
		/*
			public KuduTable createTable(String name, Schema schema, CreateTableOptions builder)
		 */
		KuduTable kuduTable = kuduClient.createTable("itcast_users_multi", schema, options);
		System.out.println("Table ID: " + kuduTable.getTableId());
	}

```

最后查看Table基本信息：

![1612426389796](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612426389796.png)



## 21-[掌握]-Java 操作 Kudu之 添加列和删除列

> 在Kudu中可以修改表，仅仅增加列或者删除列，演示代码如下：
>
> [给Kudu表增加列：address；再删除Kudu表列]()

```scala
	/**
	 * 给Kudu表增加列：address -> String
	 */
	@Test
	public void alterTableAddColumn() throws KuduException {
		// 添加列
		AlterTableOptions ato = new AlterTableOptions() ;
		ato.addColumn("address", Type.STRING, "深圳市") ;
		
		// 修改表
		AlterTableResponse response = kuduClient.alterTable("itcast_users", ato);
		System.out.println(response.getTableId());
	}
	
		/**
	 * 删除Kudu表中列：address
	 */
	@Test
	public void alterTableDropColumn() throws KuduException {
		// 添加列
		AlterTableOptions ato = new AlterTableOptions() ;
		ato.dropColumn("address") ;

		// 修改表
		AlterTableResponse response = kuduClient.alterTable("itcast_users", ato);
		System.out.println(response.getTableId());
	}
```



## 22-[掌握]-Kudu 集成 Spark之创建Maven Project 

> ​			Kudu框架自身实现与Spark 框架集成，可以从Kudu表读取数据，封装为RDD或者DataFrame；同时也可以将RDD或者DataFrame保存到Kudu表中。
>
> ```xml
>         <dependency>
>             <groupId>org.apache.kudu</groupId>
>             <artifactId>kudu-spark2_2.11</artifactId>
>             <version>${kudu.version}</version>
>         </dependency>
> ```
>
> ​			从Spark 2.0开始，推荐使用SparkSession加载和保存数据，但是在Spark1.x时，Kudu集成Spark提供上下文对象：KuduContext，主要用于创建表、删除表及DML操作。

![1612427178722](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612427178722.png)

构建Maven Project工程或Maven Module模块，POM文件添加依赖如下：

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
        <scala.version>2.11.12</scala.version>
        <scala.binary.version>2.11</scala.binary.version>
        <spark.version>2.4.0-cdh6.2.1</spark.version>
        <hadoop.version>3.0.0-cdh6.2.1</hadoop.version>
        <kudu.version>1.9.0-cdh6.2.1</kudu.version>
    </properties>

    <!-- 依赖JAR包 -->
    <dependencies>

        <dependency>
            <groupId>org.apache.kudu</groupId>
            <artifactId>kudu-client-tools</artifactId>
            <version>${kudu.version}</version>
        </dependency>

        <!-- Kudu Client 依赖包 -->
        <dependency>
            <groupId>org.apache.kudu</groupId>
            <artifactId>kudu-client</artifactId>
            <version>${kudu.version}</version>
        </dependency>

        <!-- Junit 依赖包 -->
        <dependency>
            <groupId>junit</groupId>
            <artifactId>junit</artifactId>
            <version>4.12</version>
        </dependency>

        <!-- https://mvnrepository.com/artifact/org.apache.kudu/kudu-spark2 -->
        <dependency>
            <groupId>org.apache.kudu</groupId>
            <artifactId>kudu-spark2_2.11</artifactId>
            <version>${kudu.version}</version>
        </dependency>

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

        <!-- Spark SQL 依赖 -->
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



编写Spark Application时，设置日志级别，通过`log4j.properties`设置，内容如下所示：

```properties
# Set everything to be logged to the console
log4j.rootCategory=WARN, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Set the default spark-shell log level to WARN. When running the spark-shell, the
# log level for this class is used to overwrite the root logger's log level, so that
# the user can have different defaults for the shell and regular Spark apps.
log4j.logger.org.apache.spark.repl.Main=WARN

# Settings to quiet third party logs that are too verbose
log4j.logger.org.spark_project.jetty=WARN
log4j.logger.org.spark_project.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR

# SPARK-9183: Settings to avoid annoying messages when looking up nonexistent UDFs in SparkSQL with Hive support
log4j.logger.org.apache.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR
```



## 23-[掌握]-Kudu 集成 Spark之创建表和删除表 

> 当Kudu与Spark集成时，首先在Kudu中创建表，必须创建KuduContext上下文实例对象。

```scala
package cn.itcast.kudu.table

import java.util

import org.apache.kudu.client.{CreateTableOptions, DeleteTableResponse, KuduTable}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ByteType, IntegerType, StringType, StructType}

/**
 * Kudu集成Spark，创建表和删除表：kudu_itcast_users
 */
object KuduSparkTableDemo {
	
	/**
	 * 使用KuduContext创建表
	 */
	def createKuduTable(context: KuduContext, tableName: String): Unit = {
		// a. 构建表的Schema信息
		val schema: StructType = new StructType()
    		.add("id", IntegerType, nullable = false)
    		.add("name", StringType, nullable = true)
    		.add("age", IntegerType, nullable = true)
		// b. 指定表的主键
		val keys: Seq[String] = Seq("id")
		// c. 设置表的分区和副本数
		val options: CreateTableOptions = new CreateTableOptions()
		options.addHashPartitions(util.Arrays.asList("id"), 3)
		options.setNumReplicas(1)
		/*
		  def createTable(
		      tableName: String,
		      schema: StructType,
		      keys: Seq[String],
		      options: CreateTableOptions
		  ): KuduTable
		 */
		val kuduTable: KuduTable = context.createTable(tableName, schema, keys, options)
		println("Table ID: " + kuduTable.getTableId)
	}
	
	/**
	 * 使用KuduContext删除表
	 */
	def dropKuduTable(context: KuduContext, tableName: String): Unit = {
		// 判断表是否存在，存在的话，进行删除
		if(context.tableExists(tableName)){
			val response: DeleteTableResponse = context.deleteTable(tableName)
			println(response.getElapsedMillis)
		}
	}
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象，设置相关属性
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[3]")
			// 设置Shuffle分区数目
    		.config("spark.sql.shuffle.partitions", "3")
    		.getOrCreate()
		import spark.implicits._
		
		// 2. 构建KuduContext实例对象
		val kuduContext: KuduContext = new KuduContext("node2.itcast.cn:7051", spark.sparkContext)
		
		// 3. 创建表或删除表
		val tableName: String = "kudu_itcast_users"
		createKuduTable(kuduContext, tableName)
		// dropKuduTable(kuduContext, tableName)
		
		// 程序结束，关闭资源
		spark.stop()
	}
	
}

```



## 24-[理解]-Kudu 集成 Spark之数据CRUD操作

> Spark 与Kudu集成时，可以对表中的数据进行CRUD操作，具体支持如下：

![1612429232674](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612429232674.png)

```scala
package cn.itcast.kudu.data

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Kudu集成Spark，使用KuduContext对Kudu表中的数据进行操作
 */
object KuduSparkDataDemo {
	
	// 插入数据
	def insertData(spark: SparkSession, kuduContext: KuduContext, tableName: String): Unit = {
		// 模拟数据
		val usersDF: DataFrame = spark.createDataFrame(
			Seq(
				(1001, "zhangsan", 23, "男"),
				(1002, "lisi", 22, "男"),
				(1003, "xiaohong", 24, "女"),
				(1004, "zhaoliu2", 33, "男")
			)
		).toDF("id", "name", "age", "gender")
		// 插入数据
		/*
		  def insertRows(
		      data: DataFrame,
		      tableName: String,
		      writeOptions: KuduWriteOptions = new KuduWriteOptions
		  ): Unit
		 */
		kuduContext.insertRows(usersDF, tableName)
	}
	
	// 查看Kudu表的数据
	def selectData(spark: SparkSession, kuduContext: KuduContext, tableName: String): Unit = {
		/*
		  def kuduRDD(
		      sc: SparkContext,
		      tableName: String,
		      columnProjection: Seq[String] = Nil,
		      options: KuduReadOptions = KuduReadOptions()
		  ): RDD[Row]
		 */
		val kuduRDD: RDD[Row] = kuduContext.kuduRDD(spark.sparkContext, tableName, Seq("id", "name", "age", "gender"))
		kuduRDD.foreach{row =>
			println(
				"id = " + row.getInt(0) + ", name = " +
					row.getString(1) + ", age = " + row.getInt(2) +  ", gender = " + row.getString(3)
			)
		}
	}
	
	// 更新数据
	def updateData(kuduContext: KuduContext, tableName: String): Unit = ???
	
	// 当主键存在时，更新数据；不存在时，插入数据
	def upsertData(kuduContext: KuduContext, tableName: String): Unit  = ???
	
	// 删除数据，只能依据主键删除
	def deleteData(kuduContext: KuduContext, tableName: String): Unit = ???
	
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象，设置相关属性
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[3]")
			// 设置Shuffle分区数目
			.config("spark.sql.shuffle.partitions", "3")
			.getOrCreate()
		import spark.implicits._
		
		// 2. 构建KuduContext实例对象
		val kuduContext: KuduContext = new KuduContext("node2.itcast.cn:7051", spark.sparkContext)
		val tableName: String = "kudu_itcast_users"
		
		// 3. 对Kudu表的数据进行CRUD操作
		// 插入数据insert
		//insertData(spark, kuduContext, tableName)
		// 查询数据
		selectData(spark, kuduContext, tableName)
		// 更新数据update
		//updateData(kuduContext, tableName)
		// 插入更新数据upsert
		//upsertData(kuduContext, tableName)
		// 删除数据delete
		//deleteData(kuduContext, tableName)
		
		// 程序结束，关闭资源
		spark.stop()
		
	}
	
}

```



## 25-[掌握]-Kudu 集成 Spark之DataFrame API 

> ​		在Kudu提供集成Spark类库中，按照SparkSQL外部数据源接口实现API，方便加载数据和保存数据。当从Kudu表加载或者保存数据时，需要设置参数如下所示：

![1612430230737](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612430230737.png)

> 其中2个参数必须要设置：
>
> - 1）、`kudu.master`：表示Kudu Master所在信息
> - 2）、`kudu.table`：表加载或者保存数据时表的名称
>
> 其中有个参数，需要留心，当保存DataFrame数据到Kudu表示，需要设置操作类型：`kudu.operation`,
>
> - 如果是向Kudu表插入数据或更新数据，设置为：`upsert`
> - 如果是向Kudu表删除数据，设置为：`delete`



- 1）、加载Kudu表的数据，核心代码

```scala
		// 2. 加载Kudu表的数据
		val kuduDF: DataFrame = spark.read
			.format("kudu")
			.option("kudu.master", "node2.itcast.cn:7051")
			.option("kudu.table", "kudu_itcast_users")
			.load()
		kuduDF.printSchema()
		kuduDF.show(10, truncate = false)
```



- 2）、保存数据至Kudu表，核心代码

```scala
		// 4. 保存数据至Kudu表中
		usersDF.write
			.mode(SaveMode.Append)
			.format("kudu")
			.option("kudu.master", "node2.itcast.cn:7051")
			.option("kudu.table", "kudu_users")
			.option("kudu.operation", "upsert")
			.save()
```



> 案例完整代码如下所示：

```scala
package cn.itcast.kudu.sql

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * 使用SparkSession读取Kudu数据，封装到DataFrame/Dataset集合中
 */
object SparkSQLKuduDemo {
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象，设置相关属性
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[3]")
			// 设置Shuffle分区数目
			.config("spark.sql.shuffle.partitions", "3")
			.getOrCreate()
		import spark.implicits._
		
		// 2. 加载Kudu表的数据
		val kuduDF: DataFrame = spark.read
			.format("kudu")
			.option("kudu.master", "node2.itcast.cn:7051")
			.option("kudu.table", "kudu_itcast_users")
			.load()
		/*
			root
			 |-- id: integer (nullable = false)
			 |-- name: string (nullable = true)
			 |-- age: integer (nullable = true)
			 |-- gender: string (nullable = true)
		 */
		//kuduDF.printSchema()
		/*
			+----+--------+---+------+
			|id  |name    |age|gender|
			+----+--------+---+------+
			|1001|zhangsan|23 |男    | -> F
			|1002|lisi    |22 |男    |
			|1004|zhaoliu2|33 |男    |
			|1003|xiaohong|24 |女    | -> M
			+----+--------+---+------+
		 */
		// kuduDF.show(10, truncate = false)
		
		// 3. 数据转换，自定义UDF函数实现：转换gender值
		val gender_to_udf: UserDefinedFunction = udf(
			(gender: String) => {
				// 模式匹配
				gender match {
					case "男" => "F"
					case "女" => "M"
					case _ => "未知"
				}
			}
		)
		
		val usersDF: DataFrame = kuduDF.select(
			$"id", $"name", //
			// 每个人年龄 加1
			$"age".plus(1).as("age"), //
			// 转换gender性别
			gender_to_udf($"gender").as("gender")
		)
		usersDF.persist(StorageLevel.MEMORY_AND_DISK)
		
		usersDF.printSchema()
		usersDF.show(10, truncate = false)
		
		// 4. 保存数据至Kudu表中
		usersDF.write
			.mode(SaveMode.Append)
			.format("kudu")
			.option("kudu.master", "node2.itcast.cn:7051")
			.option("kudu.table", "kudu_users")
			.option("kudu.operation", "upsert")
			.save()
		
		// 5. 程序结束，关闭资源
		spark.stop()
	}
	
}

```

