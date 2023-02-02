---
stypora-copy-images-to: img
typora-root-url: ./
---



# Logistics_Day12：自定义外部数据源ClickHouse



![1614420029129](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614420029129.png)



## 01-[复习]-上次课程内容回顾 

> 上次课程2个方面内容：ClickHouse 存储引擎（MergeTree 系列引擎）和==ClickHouse JDBC Client API==使用。
>
> - 1）、MergeTree系列引擎
>
>   - ReplacingMergeTree引擎：用来对主键重复的数据进行去重，使用命令【optimize】手动执行
>   - SummingMergeTree引擎：对表中除了主键以外的列数据进行sum函数操作
>   - AggregatingMergeTree引擎：可以指定对每列（除了主键以外），指定聚合函数
>   - CollapsingMergeTree引擎：变通的方式来实现状态的更新，指定Sign列（必须是Int8类型）-> 【1和-1】，1表示此行数据为状态行，-1表示此行数据为取消行。
>   - `VersionedCollapsingMergeTree`引擎： Version 列有助于正确折叠行
>
>   ![1614473025357](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614473025357.png)
>
> - 创建表时，构建DDL语句：
>
>   ```ini
>   CREATE TABLE IF NOT EXISTS test.tbl_order (
>   	areaName String,
>   	category String,
>   	id Int64,
>   	money String,
>   	timestamp String,
>   	sign Int8,
>   	version UInt8
>   )
>   ENGINE=VersionedCollapsingMergeTree(sign, version)
>   ORDER BY id ;
>   ```
>
> - 2）、ClickHouse JDBC Client API 使用
>
>   - 编写Java代码，使用JDBC方式查询ClickHouse表数据，[类似MySQL JDBC 查询]()
>   - 集成Spark代码，调用JDBC API，实现DDL操作（创建表和删除表）及DML操作（CUD）
>
>   ![1614473534008](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614473534008.png)
>
>   - [核心点：编写工具类ClickHouseUtils，其中封装方法，进行操作，无论DDL和DML，先构建SQL语句]()
>   - ![1614473661423](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614473661423.png)



## 02-[了解]-第12天：课程内容提纲 

> ​	实现功能SparkSQL外部数据源接口实现：==自定义外部数据源，实现ClickHouse数据库读写数据==。

![1614474045584](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614474045584.png)

> 安装SparkSQL官方提供外部数据源接口实现读写ClickHouse表数据的，基于DataSourceV2接口实现：
>
> - 1）、批量数据加载（read：SparkSQL）
>
>   - 从ClickHouse表读取数据，封装至DataFrame数据集
>
>     ![1614474671518](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614474671518.png)
>
> - 2）、批量数据保存（write：SparkSQL）
>
>   - 将DataFrame数据集保存至ClickHouse表中
>
>   ![1614474706071](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614474706071.png)
>
> - 3）、流式数据保存（writeStream：StructuredStreaming）
>
>   - ：将结构化流StructuredStreaming中每微批次Batch数据写入ClickHouse
>
>   ![1614474796183](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614474796183.png)



> 首先回顾，SparkSQL官方提供对MySQL关系型数据库集成，方便加载load和保存save数据

```Scala
// Loading data from a JDBC source
val jdbcDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql:dbserver")
  .option("dbtable", "schema.tablename")
  .option("user", "username")
  .option("password", "password")
  .load()

// Saving data to a JDBC source
jdbcDF.write
  .format("jdbc")
  .option("url", "jdbc:postgresql:dbserver")
  .option("dbtable", "schema.tablename")
  .option("user", "username")
  .option("password", "password")
  .save()
```

![1614474537796](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614474537796.png)

## 03-[理解]-实时ETL开发之历史数据加载流程 

> ​		在整个物流项目中，需要实时将业务数据进行采集，并进行ETL转换，最终存储到外部存储引擎中。
>
> [问题：原来存储在业务系统中业务数据如何将其导入到存储引擎，流式程序，实时导入当前数据，历史数据如何导入呢？？？？]()

![1614474889838](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614474889838.png)

> 历史数据导入：==批量加载导入，从业务系统批量读取数据，直接存储到大数据存储引擎。==
>

![1614475334361](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614475334361.png)

> 为什么 将RDBMs表数据导出为文本文件呢？而不还是SparkSQL直接读取RDBMs表数据呢？？
>
> [RDBMS表中存储业务数据，如果读取数据的话，对业务数据库产生很大负载压力，使用业务系统性能降低。]()



## 04-[了解]-SparkSQL之DataSource API V1概述 

> ​	从Spark 1.4版本开始，提供一套完整外部数据源接口（External DataSource Interface），以便用户可以实现SparkSQL与其他存储引擎集成整合，方便用户加载和保存数据：`DataSource API V1`（第一代API）。
>
> [比如SparkSQL内置数据源：JDBC、JSON、CSV等都是基于DataSource API V1实现功能。]()

> 自定义外部数据源实现**从HBase表批量加载load和保存save数据**，接口实现类继承图：
>
> [可以参考提供【06_扩展】提供代码实现，以及视频讲解。]()

![1614476513399](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614476513399.png)

> 这个版本的 Data Source API 有以下几个优点：
>
> - 接口实现非常简单；
> - 能够满足大部分的使用场景；
>
> 同时存在一些问题：
>
> - ==扩展能力有限，难以下推其他算子；缺乏对列式存储读取的支持；==
> - ==写操作不支持事务；缺乏分区和排序信息；不支持流处理；==



## 05-[了解]-SparkSQL之DataSource API V2概述 

> ​		`Data Source API V2`为了解决 Data Source V1 的一些问题，从 Apache `Spark 2.3.0` 版本开始，社区引入了 Data Source API V2，在保留原有的功能之外，还解决了 Data Source API V1 存在的一些问题，比如不再依赖上层 API，扩展能力增强。
>
> Data Source API V2 有以下几个优点：
>
> - ==DataSourceV2 API 使用`Java`编写==
> - 不依赖于上层API（DataFrame/RDD）
> - 易于扩展，可以添加新的优化，同时保持向后兼容
> - 提供物理信息，如大小、分区等
> - ==支持Streaming Source/Sink==
> - 灵活、强大和事务性的写入API

![1614477250933](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614477250933.png)

> 其中DataSource API V2版本标识接口：`DataSourceV2`
>
> - 1）、批量加载数据接口：`ReadSupport`
> - 2）、批量保存数据接口：`WriteSupport`
> - 3）、流式保存数据接口：`StreamWriteSupport`
>
> 此外，还提供结构化流流式数据加载接口：
>
> - 1）、微批加载数据：`MicroBatchReadSupport`
> - 2）、持续流加载数据：`ContinuesReadSupport`
>
> [内置数据源中Kafka消费数据时，实现此接口：]()

![1614477486270](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614477486270.png)



## 06-[理解]-DataSource API V2之如何定义数据源 

> 为了使用 Data Source API V2，需要使用到 Data Source API V2 包里面相关的类库：
>
> - 对于读取read数据程序，只需要实现 ReadSupport 相关接口；
> - 对于保存save数据程序，只需要实现WriteSupport相关接口；
> - 此外，需要继承基类：DataSourceV2，表示数据源

![1614477675437](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614477675437.png)

> 编写伪代码，看一看如何基于DataSource API V2接口实现外部数据源：
>
> - 1）、DataSourceV2接口：
>
>   - 继承或者实现此接口后，此类中必须存在一个公共public和无参（no-args）构造方法。
>   - 此接口中没有任何方法，仅仅属于标识接口
>
>   ![1614478013810](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614478013810.png)
>
> - 2）、ReadSupport接口和DataSourceReader接口
>
>   - [在Java8中，允许接口中存在具体方法，但是必须使用`default`声明，]()
>
>   ![1614478449367](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614478449367.png)
>
> - 3）、WriteSupport接口和DataSourceWriter接口
>
>   - Java8中提供`Optional`，可以返回有值和无值，类似Scala语言中Option（Some和None）
>
>   ![1614478906779](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614478906779.png)



## 07-[理解]-DataSource API V2之接口继承结构图

> ​		基于DataSource API V2接口定义外部数据源实现时，重点两个接口：DataSourceReader和DataSourceWriter实现，底层涉及到类继承结构图。
>
> - 1）、批量加载数据类继承结构图

![1614479516786](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614479516786.png)

> - 2）、批量保存数据类继承结构图

![1614479554891](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614479554891.png)

> - 3）、流式保存数据类继承结构图
>   - 针对流式数据保存来说，需要实现2个接口：DataSourceV2和BashStreamingSink（标识接口）
>   - 此外，结构流中数据保存，本质上还是针对每个微批次数据的保存，所以发现StreamWriteSupport底层实现，依赖批量保存接口：`DataSourceWriter`

![1614479091724](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614479091724.png)



## 08-[掌握]-数据源之ClickHouseDataSourceV2 结构

> ​		按照SparkSQL提供外部数据源接口DataSource API V2实现自定义数据源ClickHouse。

![1614480807203](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614480807203.png)

> 创建类：`ClickHouseDataSourceV2`，继承相关接口，代码如下：

```scala
package cn.itcast.logistics.spark.clickhouse

import java.util.Optional

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, StreamWriteSupport, WriteSupport}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * 依据SparkSQL中DataSource V2接口，自定义实现ClickHouse外部数据源，批量读写数据和流式写入数据
 */
class ClickHouseDataSourceV2 extends DataSourceV2
						     with ReadSupport with WriteSupport with StreamWriteSupport{
	/**
	 * 从外部数据源读取数据Reader(ReadSupport 方法）
	 *
	 * @param options 加载数据时传递option参数
	 */
	override def createReader(options: DataSourceOptions): DataSourceReader = ???
	
	/**
	 * 将数据保存外部数据源Writer（WriteSupport方法）
	 *
	 * @param writeUUID 表示JobID，针对SparkSQL中每个Job保存来说，就是JobID
	 * @param schema    保存数据Schema约束信息
	 * @param mode      保存模式
	 * @param options   保存数据时传递option参数
	 */
	override def createWriter(writeUUID: String,
	                          schema: StructType,
	                          mode: SaveMode,
	                          options: DataSourceOptions): Optional[DataSourceWriter] = ???
	
	/**
	 * 将流式数据中每批次结果保存外部数据源StreamWriter（StreamWriteSupport方法）
	 *
	 * @param queryId 流式应用中查询ID（StreamingQuery ID）
	 * @param schema  保存数据Schema约束
	 * @param mode    输出模式
	 * @param options 保存数据时传递option参数
	 */
	override def createStreamWriter(queryId: String, 
	                                schema: StructType, 
	                                mode: OutputMode, 
	                                options: DataSourceOptions): StreamWriter = ???

}

```

> 接下来，就需要实现其中三个方法：`createReader、createWriter和createStreamWriter。`
>



## 09-[掌握]-数据源之ClickHouseDataSourceV2 注册 

> ​		在实现外部数据源方法之前，首先将数据源进行注册，目的用于更好使用。
>

![1614481194209](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614481194209.png)

> 为了注册数据源，如下步骤进行操作：
>
> - 第一步、实现类，需要继承【`DataSourceRegister`】，实现其中方法：`shortName`，给简短名称。

![1614481233946](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614481233946.png)

> - 第二步、注册数据源
>   - 1）、在项目资源目录【`resouces`】，创建2层文件夹

![1614481355134](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614481355134.png)

> - 2）、创建文本文件，名称：【`org.apache.spark.sql.sources.DataSourceRegister`】
>
> - 3）、添加数据源实现类全名称：`cn.itcast.logistics.spark.clickhouse.ClickHouseDataSourceV2`

![1614481479280](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614481479280.png)



## 10-[掌握]-自定义DataSource之DataSourceOptions 解析

> ​		在具体实现方法（读取数据createReader、保存数据createWiter或creatStreamWriter）之前，首先需要解析传递过程参数信息，这些参数封装在：`DataSourceOptions`，类中使用Map集合存储在参数数据。

![1614481964908](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614481964908.png)

> 为了后续更好使用传递参数值，需要从Map集合中获取属性值，编写工具类：`ClickHouseOptions`

```scala
package cn.itcast.logistics.spark.clickhouse

import java.util

/**
 * 解析自定义ClickHouse数据源时，接收到的客户端封装的参数options，将参数的map集合进行解析
 */
class ClickHouseOptions(var options: util.Map[String, String]) extends Serializable {
	
	// 定义ClickHouse的驱动类名称
	val CLICKHOUSE_DRIVER_KEY = "clickhouse.driver"
	// 定义ClickHouse的连接地址
	val CLICKHOUSE_URL_KEY = "clickhouse.url"
	// 定义ClickHouse的用户名
	val CLICKHOUSE_USER_KEY = "clickhouse.user"
	// 定义ClickHouse的密码
	val CLICKHOUSE_PASSWORD_KEY = "clickhouse.password"
	
	// 定义ClickHouse的表名
	val CLICKHOUSE_TABLE_KEY = "clickhouse.table"
	// 定义ClickHouse的表不存在是否可以自动创建表
	val CLICKHOUSE_AUTO_CREATE_KEY = "clickhouse.auto.create"
	
	// 定义ClickHouse表的主键列
	val CLICKHOUSE_PRIMARY_KEY = "clickhouse.primary.key"
	val CLICKHOUSE_OPERATE_FIELD_KEY = "clickhouse.operate.field"
	
	// 根据key在map对象中获取指定key的value值，并且将value转换成指定的数据类型返回
	def getValue[T](key: String): T = {
		// 判断Key是否存在，如果存在获取值，不存在返回null
		val value = if (options.containsKey(key)) options.get(key) else null
		// 类型转换
		value.asInstanceOf[T]
	}
	
	// 根据driver的key获取driver的value
	def driver: String = getValue[String](CLICKHOUSE_DRIVER_KEY)
	
	// 根据url的key获取url的value
	def url: String = getValue[String](CLICKHOUSE_URL_KEY)
	
	// 根据user的key获取user的value
	def user: String = getValue[String](CLICKHOUSE_USER_KEY)
	
	// 根据user的key获取password的value
	def password: String = getValue[String](CLICKHOUSE_PASSWORD_KEY)
	
	// 根据table的key获取table的value
	def table: String = getValue[String](CLICKHOUSE_TABLE_KEY)
	
	// 获取是否自动创建表
	def autoCreateTable: Boolean = {
		// 先从Map集合获取是否创建表，默认值为false（不创建）
		val autoValue: String = options.getOrDefault(CLICKHOUSE_AUTO_CREATE_KEY, "false")
		// 将字符串true或false转换为Boolean类型
		autoValue.toLowerCase() match {
			case "true" => true
			case "false" => false
		}
	}
	
	// 获取主键列，默认主键名称为：id
	def getPrimaryKey: String = {
		options.getOrDefault(CLICKHOUSE_PRIMARY_KEY, "id")
	}
	
	// 获取数据的操作类型字段名称
	def getOperateField: String = {
		options.getOrDefault(CLICKHOUSE_OPERATE_FIELD_KEY, "opType")
	}
}


```

> 解析参数信息工具类，类结构如下图所示:

![1614482091246](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614482091246.png)

> 参数传递和解析示意图：

![1614482399659](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614482399659.png)



## 11-[理解]-ReadSupport 实现之DataSourceReader

> ​		当数据源实现接口：`ReadSupport`时，其中需要实现`createReader`方法，返回实例对象：`DataSourceReader`，具体实现类继承关系图如下所示：

![1614434398796](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614434398796.png)

> - 1）、在`createReader`方法中实现，代码如下：

![1614482677686](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614482677686.png)

> - 2）、定义类：ClickHouseDataSourceReader，实现其中方法，代码如下：

```scala
package cn.itcast.logistics.spark.clickhouse

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.StructType

/**
 * SparkSQL批量加载数据源ClickHouse表中的数据，封装DataFrame
 *      DataFrame = RDD[Row] + Schema
 */
class ClickHouseDataSourceReader(options: ClickHouseOptions) extends DataSourceReader{
	
	/**
	 * 数据集DataFrame约束Schema
	 */
	override def readSchema(): StructType = ???
	
	/**
	 * 读取数据源ClickHouse表的数据，按照分区进行读取，每条数据封装在Row对象中
	 */
	override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
		// 为了简单起见，读取clickHouse表数据时，所有数据放在1个分区中
		util.Arrays.asList(new ClickHouseInputPartition(readSchema, options))
	}
	
}

/**
 * 从ClickHouse表中加载数据时，每个分区进行读取，将每行数据封装在Row对象中
 */
class ClickHouseInputPartition(schema: StructType
                               ,options: ClickHouseOptions) extends InputPartition[InternalRow] {
	// 获取ClickHouse读取数据时每个分区读取器
	override def createPartitionReader(): InputPartitionReader[InternalRow] = ???
}

```



## 12-[理解]-ReadSupport 实现之Schema 构建

> ​	先实现`DataSourceReader`中：`readSchema`，构建DataFrame数据集结构信息。

> 首先思考：从ClickHouse表中加载数据，可以先获取ClickHouse表的结构，如下所示：
>

```SQL
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

> 当获取ClickHouse表结构以后，可以将其转换为DataFrame中Schema对象，进行封装，如下所示：
>

```ini
   root
	|-- areaName: string (nullable = false)
	|-- category: string (nullable = false)
	|-- id: long (nullable = false)
	|-- money: string (nullable = false)
	|-- timestamp: string (nullable = false)
	|-- sign: integer (nullable = false)
	|-- version: integer (nullable = false)
```

> 最终可以绘图如下一张图，展示ClickHouse表的结构与DataFrameSchema信息转换图：

![1614484369581](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614484369581.png)

> 实现转换代码，真正核心代码，封装在工具类：ClickHouseHelper中

```scala
	/**
	 * 数据集DataFrame约束Schema
	 */
	override def readSchema(): StructType = {
		// TODO: 先从ClickHouse数据库获取表的结构，将结构中字段名称和类型提取，封装到DataFrame Schema中即可
		// a). 创建 ClickHouseHelper 对象，传递参数
		val clickHouseHelper: ClickHouseHelper = new ClickHouseHelper(options)
		// b). 获取Schema信息
		clickHouseHelper.getSparkSQLSchema
	}
```



## 13-[理解]-ReadSupport 实现之InputPartitionReader

> ​		接下来，需要去实现，如何从ClickHouse表中读取数据，创建DataReader数据读取器。
>
> [当从ClickHouse表中加载数据时，可以将表的数据划分分区，每个分区称为：`InputPartition`，需要定义分区读取器：`InputPartitionReader`，加载数据]()

![1614485172183](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614485172183.png)



> 创建类：`ClickHouseInputPartitionReader`，继承`InputPartitionReader`接口，实现其中方法：

![1614485363858](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614485363858.png)

> - 1）、方法：`next`
>   - 是否还有下一条数据，返回值为布尔类型
> - 2）、方法：`get`
>   - 获取下一条数据，封装在Row对象中
> - 3）、方法：close
>   - 当获取数据完成以后，需要管理连接，比如Connection、Statement及ResultSet

```scala
/**
 * 从ClickHouse表中读取数据时，每个分区数据的具体加载
 *      TODO： 加载数据底层依然是ClickHouse JDBC方式, 编写SQL语句
 */
class ClickHouseInputPartitionReader(schema: StructType,
                                     options: ClickHouseOptions) extends InputPartitionReader[InternalRow] {
	// 创建ClickHouseHelper对象，传递参数
	private val clickHouseHelper: ClickHouseHelper = new ClickHouseHelper(options)
	
	// 定义变量
	var conn: ClickHouseConnection = _
	var stmt: ClickHouseStatement = _
	var result: ResultSet = _
	
	// 表示是否还有下一条数据，返回值为布尔类型
	override def next(): Boolean = {
		// 查询 ClickHouse 表中的数据，根据查询结果判断是否存在数据
		if((conn == null || conn.isClosed)
			&& (stmt == null || stmt.isClosed)
			&& (result ==null || result.isClosed)){
			// 实例化connection连接对象
			conn = clickHouseHelper.getConnection
			stmt = conn.createStatement()
			result = stmt.executeQuery(clickHouseHelper.getSelectStatementSQL(schema))
			// println("=======初始化ClickHouse数据库成功===========")
		}
		
		// 如果查询结果集对象不是空并且没有关闭的话在，则指针下移
		if(result != null && !result.isClosed){
			// 如果next是true，表示有数据，否则没有数据
			result.next()
		}else{
			// 返回false表示没有数据
			false
		}
	}
	
	/**
	 * 表示获取下一条数据，封装在Row对象中
	 */
	override def get(): InternalRow = {
		// TODO: next()返回true，则该方法被调用，如果返回false，该方法不被调用
		// println("======调用get函数，获取当前数据============")
		val fields: Array[StructField] = schema.fields
		//一条数据所有字段的集合
		val record: Array[Any] = new Array[Any](fields.length)
		
		// 循环取出来所有的列
		for (i <- record.indices) {
			// 每个字段信息
			val field: StructField = fields(i)
			// 列名称
			val fieldName: String = field.name
			// 列数据类型
			val fieldDataType: DataType = field.dataType
			// 根据字段类型，获取对应列的值
			fieldDataType match {
				case DataTypes.BooleanType => record(i) = result.getBoolean(fieldName)
				case DataTypes.DateType => record(i) = DateTimeUtils.fromJavaDate(result.getDate(fieldName))
				case DataTypes.DoubleType => record(i) = result.getDouble(fieldName)
				case DataTypes.FloatType => record(i) = result.getFloat(fieldName)
				case DataTypes.IntegerType => record(i) = result.getInt(fieldName)
				case DataTypes.ShortType => record(i) = result.getShort(fieldName)
				case DataTypes.LongType => record(i) = result.getLong(fieldName)
				case DataTypes.StringType => record(i) = UTF8String.fromString(result.getString(fieldName))
				case DataTypes.TimestampType => record(i) = DateTimeUtils.fromJavaTimestamp(result.getTimestamp(fieldName))
				case DataTypes.ByteType => record(i) = result.getByte(fieldName)
				case DataTypes.NullType => record(i) = StringUtils.EMPTY
				case _ => record(i) = StringUtils.EMPTY
			}
		}
		
		// 创建InternalRow对象
		new GenericInternalRow(record)
	}
	
	/**
	 *  数据读取完成以后，关闭资源
	 */
	override def close(): Unit = {
		clickHouseHelper.closeJdbc(conn, stmt, result)
	}

}
```



## 14-[掌握]-ReadSupport 实现之批量加载测试

> ​		前面已经编程实现批量加载数据代码，实现`ReadSupport`中方法，核心类：`InputPartitionReader`实现，接下来编程SparkSQL程序，采用标准数据源方式加载数据测试。

![1614494484605](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614494484605.png)

> 创建测试对象：`SparkSQLClickHouseTest`，放在测试包下面。

```scala
package cn.itcas.logistics.test.clickhouse

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 测试自定义数据源ClickHouse：
 *      1. 使用批的方式加载ClickHouse表中的数据
 *      2. 将数据集批量保存至ClickHouse表中
 */
object SparkSQLClickHouseTest {
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象，设置相关配置信息
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[2]")
			.config("spark.sql.shuffle.partitions", "2")
			// 设置Kryo序列化方式
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.getOrCreate()
		import spark.implicits._
		
		// 2. 从ClickHouse加载数据，封装至DataFrame中
		val clickHouseDF: DataFrame = spark.read
			.format("clickhouse")
			.option("clickhouse.driver", "ru.yandex.clickhouse.ClickHouseDriver")
			.option("clickhouse.url", "jdbc:clickhouse://node2.itcast.cn:8123/")
			.option("clickhouse.user", "root")
			.option("clickhouse.password", "123456")
			.option("clickhouse.table", "test.tbl_order")
			.load()
		clickHouseDF.printSchema()
		clickHouseDF.show(10, truncate = false)
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}

```

> 运行程序，控制台查看是否批量加载数据成功

![1614494869123](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614494869123.png)

> 结果数据：

![1614494891617](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614494891617.png)



## 15-[理解]-WriteSupport 实现之批量保存编程

> ​		前面完成批量从ClickHouse数据库表中加载数据，封装到DataFrame数据集中，接下来对数据进行聚合分析处理（简单分组统计），最终将结果再次保存到ClickHouse数据库表中。
>
> [将数据集DataFrame批量保存至ClickHouse表中。]()

![1614495501080](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614495501080.png)

> 针对上述聚合统计数据集：aggDF，如果在ClickHouse数据库中创建表的话，语句如下所示：

```SQL
CREATE TABLE IF NOT EXISTS default.tbl_order_agg
(
	`category` String,
	`count` Int64,
	`Sign` Int8,
	`Version` UInt8
)
ENGINE = VersionedCollapsingMergeTree(Sign, Version)
ORDER BY category
SETTINGS index_granularity = 8192
```

> 在批量保存数据到ClickHouse数据库时，首先判断表是否存在，如果不存在，自动创建，逻辑关系如下：

![1614495672112](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614495672112.png)

> ​		保存数据到ClickHouse表中数据，依据其中字段：opType（默认设置字段）数据操作类型，决定如何对这个数据进行保存操作：
>
> - 1）、`opType=insert`时，将数据转换为INSERT SQL语句，插入到表中
> - 2）、`opType=delete`或`update`时，将数据转换为ALTER UPDATE或 ALTER DELETE语句，应用到表，进行更新和删除数据操作。



## 16-[理解]-WriteSupport实现之DataSourceWriter

> ​		SparkSQL中外部数据源接口中，批量保存数据时，需要实现：`WriteSupport`接口，其中实现方法：`createWriter`，创建数据写入器`DataWriter`。

> - 1）、先实现方法：createWriter方法，具体代码如下所示：

```scala
	/**
	 * 将数据保存外部数据源Writer（WriteSupport方法）
	 *
	 * @param writeUUID 表示JobID，针对SparkSQL中每个Job保存来说，就是JobID
	 * @param schema    保存数据Schema约束信息
	 * @param mode      保存模式
	 * @param options   保存数据时传递option参数
	 */
	override def createWriter(writeUUID: String,
	                          schema: StructType,
	                          mode: SaveMode,
	                          options: DataSourceOptions): Optional[DataSourceWriter] = {
		// TODO: 依据保存模式SaveMode，决定如何将数据进行保存到存储引擎，此处只支持：Append
		mode match {
			case SaveMode.Append =>
				// 解析传递参数信息
				val clickHouseOptions = new ClickHouseOptions(options.asMap())
				// 构建数据源Writer对象
				val dataSourceWriter = new ClickHouseDataSourceWriter(clickHouseOptions, schema)
				// 返回对象
				Optional.of(dataSourceWriter)
			case _ => Optional.empty[DataSourceWriter]()
		}
	}
```

> 需要实现DataSourceWriter中方法，类继承结构如下：

![1614434414998](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614434414998.png)

> - 2）、创建类：`ClickHouseDataSourceWriter`，实现其中方法：`createWriteFactory`

```scala
package cn.itcast.logistics.spark.clickhouse

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/**
 * 批量保存数据至ClickHouse表中时，Writer对象
 */
class ClickHouseDataSourceWriter(options: ClickHouseOptions,
                                 schema: StructType) extends DataSourceWriter{
	/**
	 * 构建WriterFactory工厂对象，用于创建Writer实例，保存写入数据
	 */
	override def createWriterFactory(): DataWriterFactory[InternalRow] = {
		new ClickHouseDataWriterFactory(options, schema)
	}
	
	/**
	 * 当写入数据成功时，信息动作
	 */
	override def commit(messages: Array[WriterCommitMessage]): Unit = {
		// TODO： 此处代码暂且不实现
	}
	
	/**
	 * 当数据写入失败时，信息动作
	 */
	override def abort(messages: Array[WriterCommitMessage]): Unit = {
		// TODO： 此处代码暂且不实现
	}
}

```

> - 3）、创建：`ClickHouseDataWriterFactory`，实现方法：`createDataWriter`

```scala
package cn.itcast.logistics.spark.clickhouse

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/**
 * 继承DataWriterFactory基类，构建工厂，创建DataWriter对象
 *      TODO：此处单独提取出来，在SparkSQL中，无论批量保存还是流式保存，底层都是重用DataWriterFactory，进行获取Writer对象，保存数据
 */
class ClickHouseDataWriterFactory(options: ClickHouseOptions,
                                  schema: StructType)  extends DataWriterFactory[InternalRow]{
	/**
	 * 每个分区数据，获取一个DataWriter对象
	 */
	override def createDataWriter(partitionId: Int, // 分区ID
	                              taskId: Long, // TaksID，每个分区数据被一个Task处理分析
	                              epochId: Long): DataWriter[InternalRow] = {
		new ClickHouseDataWriter(options, schema)
	}
}

class ClickHouseDataWriter(options: ClickHouseOptions,
                           schema: StructType) extends DataWriter[InternalRow] {
	
	override def write(record: InternalRow): Unit = ???
	
	override def commit(): WriterCommitMessage = ???
	
	override def abort(): Unit = ???
}
```

> 可以发现，无论前面批量加载数据还是批量保存数据，针对读写数据实现类：`DataReader`和`DataWriter`，需要将Schema信息和参数Options进行传递，到真正获取数据和保存数据的使用。



## 17-[理解]-WriteSupport实现之DataWriter（上下）

> ​		批量保存数据到ClickHouse表中时，最终需要实现：`ClickHouseDataWriter`，数据写入器，方法：
>
> - 1）、`write`：针对每条记录写入数据
>   - 写入数据（被多次调用，每条数据都会调用一次该方法）
> - 2）、`commit`：事务提交，保存数据成功
>   - 提交方法（提交数据），可以用于批量提交（批量保存数据到外部存储系统）
> - 3）、`abort`：可以不管方法实现
>   - 保存数据异常，事务回滚时，一些操作信息

![1614497046821](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614497046821.png)

> 具体实现上述方法，分析思路：[针对每个分区数据保存时，首先转换每条数据Row为INSERT或ALTER语句，最后对分区中所有数据转的语句进行批量操作，提升性能。]()

![1614497452372](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614497452372.png)

> 在保存数据之前，首先在类中定义变量：`init`，通过代码块赋值，进行表的创建操作
>
> [在Scala中，当创建类的对象时，会将类中变量进行初始化操作，此时`init`变量就会被初始化，执行代码块中方法，自动判断是否创建表，如果是的话， 构建表的创建语句并执行。]()

![1614499401512](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614499401512.png)

> 在真正提交保存数据到ClickHouse数据库表中时，需要将SQL语句分为：insert和alter，不同操作

```scala
	/**
	 * 批量执行操作，执行INSERT、ALTER语句
	 */
	def executeUpdateBatch(sqls: Array[String]): Unit = {
		// 声明变量
		var conn: ClickHouseConnection = null
		var stmt: ClickHouseStatement = null
		try {
			// 创建实例对象
			conn = getConnection
			stmt = conn.createStatement()
			
			// TODO: 操作类型：INSERT
			val insertSQLs: Array[String] = sqls.filter(sql => sql.startsWith("INSERT"))
			val batchSQL = new StringBuilder()
			var counter: Int = 1
			insertSQLs.foreach{insertSQL =>
				if(1 == counter) {
					batchSQL.append(insertSQL)
				}else{
					// INSERT INTO test.tbl_order_agg (category, count, sign, version) VALUES ('电脑', 8, 1, 1610777043766)
					val offset = insertSQL.indexOf("VALUES")
					batchSQL.append(",").append(insertSQL.substring(offset + 6))
				}
				counter += 1
			}
			if(batchSQL.nonEmpty) {
				println(batchSQL.toString())
				stmt.executeUpdate(batchSQL.toString())
			}
			
			// TODO: 操作类型：ALTER(UPDATE和DELETE)
			val alterSQLs: Array[String] = sqls.filter(sql => sql.startsWith("ALTER"))
			alterSQLs.foreach{alterSQL =>
				stmt.executeUpdate(alterSQL)
			}
		} catch {
			case e: Exception => e.printStackTrace()
		}finally {
			if (stmt != null || ! stmt.isClosed) stmt.close()
			if (conn != null || ! conn.isClosed) conn.close()
		}
	}
```



## 18-[理解]-WriteSupport实现之 批量保存测试

> ​		前面已经实现WriteSupport批量保存数据接口中方式：`createWriter`，运行程序进行测试操作

```scala
package cn.itcas.logistics.test.clickhouse

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 测试自定义数据源ClickHouse：
 *      1. 使用批的方式加载ClickHouse表中的数据
 *      2. 将数据集批量保存至ClickHouse表中
 */
object SparkSQLClickHouseTest {
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象，设置相关配置信息
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[2]")
			.config("spark.sql.shuffle.partitions", "2")
			// 设置Kryo序列化方式
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.getOrCreate()
		import spark.implicits._
		
		// 2. 从ClickHouse加载数据，封装至DataFrame中
		val clickHouseDF: DataFrame = spark.read
			.format("clickhouse")
			.option("clickhouse.driver", "ru.yandex.clickhouse.ClickHouseDriver")
			.option("clickhouse.url", "jdbc:clickhouse://node2.itcast.cn:8123/")
			.option("clickhouse.user", "root")
			.option("clickhouse.password", "123456")
			.option("clickhouse.table", "default.tbl_order")
			.load()
		//clickHouseDF.printSchema()
		//clickHouseDF.show(10, truncate = false)
		
		// 3. 数据分析处理：按照category类别分组统计
		val aggDF: DataFrame = clickHouseDF.groupBy($"category").count()
		//aggDF.printSchema()
		//aggDF.show(10, truncate = false)
		
		// 4. 保存分析结果数据至ClickHouse表中
		aggDF
			// 添加字段，表示数据属于insert、update还是delete
			.withColumn("opType", lit("insert"))
			.write
			.mode(SaveMode.Append)
			.format("clickhouse")
			.option("clickhouse.driver", "ru.yandex.clickhouse.ClickHouseDriver")
			.option("clickhouse.url", "jdbc:clickhouse://node2.itcast.cn:8123/")
			.option("clickhouse.user", "root")
			.option("clickhouse.password", "123456")
			.option("clickhouse.table", "default.tbl_order_agg")
			.option("clickhouse.auto.create", "true")  // 表不存在，创建表
			.option("clickhouse.primary.key", "category") // 指定主键字段名称
			.option("clickhouse.operate.field", "opType") // 指定数据操作类型的字段
			.save()
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
```

> 上面测试程序代码，即从ClickHouse表中读取数据，又向ClickHouse表中写入数据。
>

```SQL
INSERT INTO default.tbl_order_agg (category, count, sign, version) VALUES ('电脑', 8, 1, 1), ('家电', 2, 1, 1), ('家具', 3, 1, 1), ('书籍', 4, 1, 1), ('食品', 5, 1, 1), ('服饰', 4, 1, 1) 
```

> 使用ClickHouse客户端clickhouse-client连接数据库，查询表的结构和数据

```ini
node2.itcast.cn :) show create table tbl_order_agg ;

SHOW CREATE TABLE tbl_order_agg

┌─statement─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.tbl_order_agg
(
    `category` String, 
    `count` Int64, 
    `sign` Int8, 
    `version` UInt32
)
ENGINE = VersionedCollapsingMergeTree(sign, version)
ORDER BY category
SETTINGS index_granularity = 8192 │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

1 rows in set. Elapsed: 0.003 sec. 

node2.itcast.cn :) select * from tbl_order_agg ;

SELECT *
FROM tbl_order_agg

┌─category─┬─count─┬─sign─┬─version─┐
│ 书籍     │     4 │    1 │       1 │
│ 家具     │     3 │    1 │       1 │
│ 家电     │     2 │    1 │       1 │
│ 服饰     │     4 │    1 │       1 │
│ 电脑     │     8 │    1 │       1 │
│ 食品     │     5 │    1 │       1 │
└──────────┴───────┴──────┴─────────┘

6 rows in set. Elapsed: 0.132 sec. 

```

> 编写程序，模拟产生数据，其中有更新update和删除delete类型数操作，再次测试程序，代码如下：

```scala
package cn.itcas.logistics.test.clickhouse

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 测试自定义数据源ClickHouse：
 *      将数据集批量保存至ClickHouse表中
 */
object SparkSQLClickHouseAlterTest {
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象，设置相关配置信息
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[2]")
			.config("spark.sql.shuffle.partitions", "2")
			// 设置Kryo序列化方式
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.getOrCreate()
		import spark.implicits._
		
		// 2. 模拟产生数据
		val dataframe: DataFrame = Seq(
			("书籍", 100, "update"), ("家具", 50, "update"), ("食品", 100, "delete")
		).toDF("category", "count", "opType")
		
		// 3. 保存分析结果数据至ClickHouse表中
		dataframe.write
			.mode(SaveMode.Append)
			.format("clickhouse")
			.option("clickhouse.driver", "ru.yandex.clickhouse.ClickHouseDriver")
			.option("clickhouse.url", "jdbc:clickhouse://node2.itcast.cn:8123/")
			.option("clickhouse.user", "root")
			.option("clickhouse.password", "123456")
			.option("clickhouse.table", "default.tbl_order_agg")
			.option("clickhouse.auto.create", "true")  // 表不存在，创建表
			.option("clickhouse.primary.key", "category") // 指定主键字段名称
			.option("clickhouse.operate.field", "opType") // 指定数据操作类型的字段
			.save()
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}

```

> 执行上述测试程序，查看ClickHouse表的数据如下图所示：

![1614500739206](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614500739206.png)



## 19-[理解]-StreamWriteSupport实现之StreamWriter

> ​		前面实现外部数据源：批量加载ReadSupport和批量保存WriteSupport，接下来，实现流式保存StreamWriteSupport，底层重用批量保存WriteSupport代码，[原因在于结构化流本质上还是微批处理，将每个微批次数据处理分析以后，保存至外部存储引擎中。]()

![1614501113887](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614501113887.png)

> - 1）、实现方法：`createStreamWriter`，创建流式数据写入器对象

```scala
	/**
	 * 将流式数据中每批次结果保存外部数据源StreamWriter（StreamWriteSupport方法）
	 *
	 * @param queryId 流式应用中查询ID（StreamingQuery ID）
	 * @param schema  保存数据Schema约束
	 * @param mode    输出模式
	 * @param options 保存数据时传递option参数
	 */
	override def createStreamWriter(queryId: String,
	                                schema: StructType,
	                                mode: OutputMode,
	                                options: DataSourceOptions): StreamWriter = {
            // a). 解析参数信息
            val clickHouseOptions = new ClickHouseOptions(options.asMap())
            // b). 创建流式写入器对象
            val writer = new ClickHouseStreamWriter(clickHouseOptions, schema)
            // c). 返回对象
            writer
		}
	}
```

> - 2）、编写类：`ClickHouseStreamWriter`，创建`WriterFactory`工厂对象，重用批量保存数据代码，直接调用接口，具体代码如下所示：

```scala
package cn.itcast.logistics.spark.clickhouse

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.types.StructType

/**
 * 流式数据保存至ClickHouse表中，主要StructuredStreaming Sink 实现，终端为ClickHouse
 */
class ClickHouseStreamWriter(options: ClickHouseOptions,
                             schema: StructType) extends StreamWriter{
	/**
	 * 获取DataWriter工厂，提供DataWriter对象，将数据写入ClickHouse表中
	 */
	override def createWriterFactory(): DataWriterFactory[InternalRow] = {
		new ClickHouseDataWriterFactory(options, schema)
	}
	
	/**
	 * 数据插入成功
	 */
	override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
		// TODO： 暂不考虑，编程实现
	}
	
	/**
	 * 数据插入失败
	 */
	override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
		// TODO： 暂不考虑，编程实现
	}

}

```

> 可以编写结构化流程序，将流式DataFrame保存至ClickHouse表中。



## 20-[掌握]-StreamWriteSupport实现之流式保存测试 

> ​		编写结构化流程序：从TCP Socket读取数据，进行词频统计，保存至ClickHouse表中。

```scala
package cn.itcas.logistics.test.clickhouse

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 从TCP Socket读取数据，进行词频统计，将最终结果存储至ClickHouse表中
 */
object StructuredStreamingClickHouseTest {
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[3]")
			.config("spark.sql.shuffle.partitions", "3")
    		.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.getOrCreate()
		import spark.implicits._
		
		// 2. 从TCP Socket消费数据
		val inputStreamDF: DataFrame = spark.readStream
			.format("socket")
			.option("host", "node2.itcast.cn")
			.option("port", "9998")
			.load()
		/*
			value: String
		 */
		inputStreamDF.printSchema()
		
		// 3. 词频统计
		val resultStreamDF: DataFrame = inputStreamDF
			.as[String] // 将DataFrame转换为Dataset，方便使用
			.filter(line => null != line && line.trim.length > 0)
			// 分割单词, 每个单词列名称：value -> String
			.flatMap(line => line.trim.split("\\s+"))
			// 按照单词进行分组 -> SELECT value, count().as("count") FROM words GROUP BY value ;
			.groupBy($"value").count()
			// 修改字段名称
			.select($"value".as("word"), $"count".as("total"))
		
		// 4. 保存流式数据到ClickHouse表中
		val query: StreamingQuery = resultStreamDF
			.withColumn("opType", lit("insert"))
			.writeStream
			.outputMode(OutputMode.Append())
			.queryName("query-clickhouse-tbl_word_count")
			.format("clickhouse")
			.option("clickhouse.driver", "ru.yandex.clickhouse.ClickHouseDriver")
			.option("clickhouse.url", "jdbc:clickhouse://node2.itcast.cn:8123/")
			.option("clickhouse.user", "root")
			.option("clickhouse.password", "123456")
			.option("clickhouse.table", "default.tbl_word_count")
			.option("clickhouse.auto.create", "true")
			.option("clickhouse.primary.key", "word")
			.option("clickhouse.operate.field", "opType")
			.option("checkpointLocation", "datas/ckpt-1001/")
			.start()
		
		// TODO: step5. 启动流式应用后，等待终止
		query.awaitTermination()
		query.stop()
	}
	
}

```

> ​		执行测试程序，在TCP Socket中输入【`flink hadoop spark spark flink spark spark` 】，查看ClickHouse数据库中是否创建表及插入数据：
>

![1614502410566](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614502410566.png)













