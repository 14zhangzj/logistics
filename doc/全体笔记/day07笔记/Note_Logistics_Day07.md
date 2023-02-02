---
stypora-copy-images-to: img
typora-root-url: ./
---



# Logistics_Day07：主题及指标开发

![1613867341272](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613867341272.png)

## 01-[复习]-上次课程内容回顾 

> ​		整个物流项目业务数据ETL流程：实时增量采集（OGG采集Oracle数据库和Canal采集MySQL数据库），发送到分布式消息队列Kafka中，编写结构化流实时消费数据，进行ETL转换操作，最终存储到Kudu表中。
>
> [流式应用程序，分为三个部分：load加载数据、process处理转换数据、save保存数据。]()

![1613867908777](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613867908777.png)

> - 1）、加载数据：==load==

```scala
	/**
	 * 读取数据的方法
	 *
	 * @param spark SparkSession
	 * @param topic 指定消费的主题
	 * @param selectExpr 默认值：CAST(value AS STRING)
	 * @return 从Kafka消费流式Stream DataFrame，默认情况下，其中仅仅包含value字段，类型String
	 */
	def load(spark: SparkSession, topic: String,
	         selectExpr: String = "CAST(value AS STRING)"): DataFrame = {
		spark
			.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", Configuration.KAFKA_ADDRESS)
			.option("subscribe", topic)
			.load()
			.selectExpr(selectExpr)
	}
```

> - 2）、转换数据：==process==，消费Kafka消息数据以后，进行ETL转换，分为2个部分
>
>   - 第一部分、JSON字符串转换为MessageBean对象
>
>     - 使用FastJson库进行转换操作，针对不同数据OGG和Canal采集数据进行不同转换
>
>     [此部分代码放在【`process`】方法中实现]()
>
>   ```scala
>   	/**
>   	 * 数据的处理，将JSON字符串转换为Bean对象
>   	 *
>   	 * @param streamDF 流式数据集StreamingDataFrame
>   	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
>   	 * @return 流式数据集StreamingDataFrame
>   	 */
>   	override def process(streamDF: DataFrame, category: String): DataFrame = {
>   		// 导入隐式转换
>   		import streamDF.sparkSession.implicits._
>   		
>   		// 1. 依据不同业务系统数据进行不同的ETL转换
>   		val etlStreamDF: DataFrame = category match {
>   			// TODO: 物流系统业务数据ETL转换
>   			case "logistics" =>
>   				// 第一步、TODO： a. 转换JSON为Bean对象
>   				val beanStreamDS: Dataset[OggMessageBean] = streamDF
>   					.as[String] // 将DataFrame转换为Dataset
>   					.filter(json => null != json && json.trim.length > 0) // 过滤数据
>   					// 由于OggMessageBean是使用Java语言自定义类，所以需要自己指定编码器Encoder
>   					.mapPartitions { iter =>
>   						iter.map { json => JSON.parseObject(json, classOf[OggMessageBean]) }
>   					}(Encoders.bean(classOf[OggMessageBean]))
>   				
>   				// 第三步、c. 返回转换后数据
>   				beanStreamDS.toDF()
>   				
>   			// TODO：CRM系统业务数据ETL转换
>   			case "crm" =>
>   				// 第一步、TODO： a. 转换JSON为Bean对象
>   				implicit val canalEncoder: Encoder[CanalMessageBean] = Encoders.bean(classOf[CanalMessageBean])
>   				val beanStreamDS: Dataset[CanalMessageBean] = streamDF
>   					.filter(row => ! row.isNullAt(0))
>   					.mapPartitions{iter =>
>   						iter.map{row =>
>   							val json = row.getAs[String](0)
>   							JSON.parseObject(json, classOf[CanalMessageBean])
>   						}
>   					}
>   				// 第三步、c. 返回转换后数据
>   				beanStreamDS.toDF()
>   			
>   			// TODO: 其他业务数据数据，直接返回即可
>   			case _ => streamDF
>   		}
>   		// 2. 返回转换后流数据
>   		etlStreamDF
>   	}
>   ```
>
>   
>
>   - 第二部分、提取MessageBean数据字段，封装为POJO对象
>
>     - 依据table名称，将数据字段值提取，封装到对应表的POJO对象中
>     - 使用FastJson库转换操作（Map或List转换为JSON字符串，再将JSON字符串转换为POJO对象）
>
>     [单独提取出来，分别放在对应方法中实现：`etlLogistics`和`etlCrm`]()

```scala
	/**
	 * 物流Logistics系统业务数据ETL转换处理及保存外部存储
	 *      1)、Bean -> POJO  2)、保存save
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 */
	def etlLogistics(streamDF: DataFrame): Unit = {
		import cn.itcast.logistics.common.BeanImplicits._
		
		// 转换DataFrame为Dataset
		val oggBeanStreamDS: Dataset[OggMessageBean] = streamDF.as[OggMessageBean]
		
		// 第二步、TODO：b. 提取Bean数据字段，封装POJO
		/*
			以Logistics物流系统：tbl_areas为例，从Bean对象获取数据字段值，封装POJO【AreasBean】
		 */
		val areasStreamDS: Dataset[AreasBean] = oggBeanStreamDS
			// 过滤获取AreasBean数据
			.filter(oggBean => oggBean.getTable == TableMapping.AREAS)
			// 提取数据字段值，进行封装为POJO
			.map(oggBean => DataParser.toAreas(oggBean))
			// 过滤不为null
			.filter(pojo => null != pojo)
		save(areasStreamDS.toDF(), TableMapping.AREAS)
    }    

// ============================================================
	/**
	 * 客户管理管理CRM系统业务数据ETL转换处理及保存外部存储
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 */
	def etlCrm(streamDF: DataFrame): Unit = {
		import cn.itcast.logistics.common.BeanImplicits._
		
		// 转换DataFrame为Dataset
		val canalBeanStreamDS: Dataset[CanalMessageBean] = streamDF.as[CanalMessageBean]
		
		// 第二步、TODO： b. Bean提取数据字段，封装为POJO对象
		val addressStreamDS: Dataset[AddressBean] = canalBeanStreamDS
			// 过滤获取地址信息表相关数据：tbl_address
			.filter(canalBean => canalBean.getTable == TableMapping.ADDRESS)
			// 提取数据字段，转换为POJO对象
			.map(canalBean => DataParser.toAddress(canalBean))
			.filter(pojo => null != pojo) // 过滤非空数据
		save(addressStreamDS.toDF(), TableMapping.ADDRESS)
     }   
```

> - 3）、保存数据：save，将ETL后数据保存至Kudu表
>
>   - 依据数据操作类型opType进行保存
>   - `opType`为`insert`或`update`时，设置`kudu.operation=upsert`，插入更新数据
>   - `opType`为`delete`时，设置`kudu.operation=delete`，依据主键删除数据
>
>   [此外，保存数据到Kudu表时，首先判断表是否存在，如果表不存在，允许创建时，需要创建表]()

```scala
	/**
	 * 数据的保存
	 *
	 * @param streamDF          保存数据集DataFrame
	 * @param tableName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	override def save(streamDF: DataFrame, tableName: String,
	                  isAutoCreateTable: Boolean = true): Unit = {
		// 将实时ETL数据存储在Kudu表中，属于数仓架构ODS层
		val odsTableName: String = s"${tableName}"
		
		// step1. 判断是否需要创建表（当表不存在时）
		if(isAutoCreateTable){
			// TODO: 判断Kudu中表是否存在，如果不存在，创建表
			KuduTools.createKuduTable(odsTableName, streamDF.drop("opType"))
		}
		
		/*
			依据opType数据操作类型：insert、update、delete，决定如何“保存”至Kudu表
			-1. opType为insert或update时，将数据插入更新到Kudu表
				kudu.operation=upsert
			-2. opType为delete时，将依据主键删除Kudu表数据
				kudu.operation=delete
		 */
		
		// step2. 保存数据至Kudu表
		// TODO: opType为insert或update时，将数据插入更新到Kudu表, kudu.operation=upsert
		streamDF
			// 过滤数据，opType为insert或update
			.filter(
				col("opType") === "insert" || col("opType") === "update"
			)
			// 删除opType列
			.drop("opType")
			.writeStream
    		.outputMode(OutputMode.Append())
    		.queryName(s"query-kudu-${odsTableName}-upsert")
    		.format(Configuration.SPARK_KUDU_FORMAT)
    		.option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
    		.option("kudu.table", odsTableName)
    		.option("kudu.operation", "upsert")
    		.option("kudu.socketReadTimeoutMs", "10000")
    		.start()
		
		// TODO: opType为delete时，将依据主键删除Kudu表数据, kudu.operation=delete
		streamDF
			// 过滤数据，opType为insert或update
			.filter(col("opType") === "delete")
			// 选择主键列
    		.select("id")
			.writeStream
			.outputMode(OutputMode.Append())
			.queryName(s"query-kudu-${odsTableName}-delete")
			.format(Configuration.SPARK_KUDU_FORMAT)
			.option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
			.option("kudu.table", odsTableName)
			.option("kudu.operation", "delete")
			.option("kudu.socketReadTimeoutMs", "10000")
			.start()
	}
```



## 02-[理解]-第6章：内容概述和学习目标

> 第6章内容主要讲解数据分析：离线报表统计（SparkSQL）和即席查询（Impala）：
>
> [当实时增量将业务数据ETL同步到Kudu表中以后，需要基于数据进行分析处理：离线分析和即席查询]()
>
> - 离线报表分析：==基于SparkSQL分析==
>
>   - 按照离线数仓分层架构管理数据（ODS、DW和DA/APP，典型三层架构）
>   - 定时调度执行（依赖调度执行），可以使用`Oozie`，另外调度框架：`Azkaban`
>
>   ![1613869364291](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613869364291.png)
>
> - 即席查询（Ad Hoc）：
>
>   [即席查询（Ad Hoc）是用户根据自己的需求，灵活的选择查询条件，系统能够根据用户的选
>   择生成相应的统计报表。即席查询与普通应用查询最大的不同是普通的应用查询是定制开发的，而
>   即席查询是由用户自定义查询条件的。]()
>
>   - 使用Impala基于内存分析引擎查询Kudu表数据，==Kudu与Impala一对CP，无缝集成==
>   - 可以使用Hue集成Impala，为用户提供可视化SQL界面即席查询分析
>
>   ![1613869565631](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613869565631.png)

![1613869031679](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613869031679.png)

> 第6章课程目标：非常重要

![1613869714517](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613869714517.png)



## 03-[了解]-第7天：课程内容提纲

> 主要讲解离线报表分析（一个主题）和Impala 分析引擎

![1613869812849](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613869812849.png)

> - 1）、离线报表分析：总共5个主题离线报表分析
>   - 主题开发业务流程（数仓分层架构，尤其针对物流项目来说）
>   - 每个主题报表开发时步骤：分为2个步骤，==首先宽表拉链操作，然后指标计算==
>   - 离线模板开发初始化
>   - 封装公共接口：基类BasicOfflineApp（加载数据load、处理数据process和保存数据save）
>   - ==以【快递单ExpressBill主题讲解业务报表开发】==

![1613870119993](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613870119993.png)

> - 2）、Impala 即席查询：
>   - 即席查询背景
>   - SQL on Hadoop 大数据技术框架发展：Hive -> Impala、SparkSQL等
>   - Impala 分析引擎介绍、架构原理（基于内存分布式分析数据）
>   - Impala 安装、部署、启动及基本使用（impala-shell）
>   - Impala与Kudu集成：Impala如何分析Kudu表 数据
>   - Impala与Hue集成，在Hue提供可视化界面，直接编写SQL语句即可

![1613870343136](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613870343136.png)

> 神策数据分析产品【用户自定义查询】，底层使用就是Impala分析引擎

![1613870517307](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613870517307.png)



## 04-[理解]-主题及指标开发之功能总述 

> 整个离线报表分析，按照主题Topic划分统计指标：5个主题（快递单、运单、仓库、车辆、用户）

![1613872133354](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613872133354.png)

> 使用SparkSQL进行报表分析，全部编写DSL代码（调用DataFrame API）。



## 05-[掌握]-主题及指标开发之数仓分层架构

> 回顾数仓分层架构：至少分为三层架构

![1613872375720](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613872375720.png)

> 如果将维度表数据单独从ODS层提取出来，放在DIM层，示意图如下所示：

![1613872428430](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613872428430.png)

> ​		主题指标开发，按照数据仓库分层结构进行存储数据，分为典型数仓三层架构：ODS 层、DW层和APP层，==更加有效的数据组织和管理，使得数据体系更加有序==。
>
> ==数仓分层本质：使用空间替换时间，==

![1613872570207](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613872570207.png)

> 数据分层的好处

![1613872641180](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613872641180.png)

> 数仓分层中每一层功能及存储数据是什么：

![1613872685026](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613872685026.png)

> 电商网站的数据体系设计，只关注用户访问日志这部分数据

![1613872792039](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613872792039.png)

> 物流项目主题报表开发，同样按照离线数仓3层架构划分：ODS层和DW层和DA层，但是名称不要而已。

采用的是京东的数据仓库分层模式，是根据标准的模型演化而来。

```
数据仓库分层：
BDM：缓冲数据，源数据的直接映像，缓冲：Buffer
FDM：基础数据层，数据拉链处理、分区处理，基础：Foundation
GDM：通用聚合，通用：Generic
ADM：高度聚合，聚合：Aggregation，应用层：Application
```

![1613873151107](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613873151107.png)

先把数据从源数据库中抽取加载到BDM层中，然后FDM层根据BDM层的数据按天分区

```
DIM：维度，全称为dimension，[daɪˈmenʃn] 
TMP：临时，全称为temporary，temprəri] 
DM：数据集市，全称为Data Mart，有时候也是数据挖掘（Data Mining）
```



## 06-[掌握]-主题及指标开发之三层架构流程

> 物流项目离线报表分析分为5个主题，每个主题报表指标开发按照数仓三层架构划分，管理数据。

![1613872943969](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613872943969.png)

> 每个主题指标开发，分为三层：
>
> - 第一步（最低层）：`ODS层`
>   - 数据来源：编程StructuredStreaming结构化流实时消费Kafka数据，ETL转换后同步而来
> - 第二层（中间层）：`DWD层，数据明细层`
>   - 数据来源：从ODS层加载【`事实表数据`和`维度表数据`】进行关联拉宽数据
>   - 开发程序：SparkSQL编程，业务数据拉宽操作
> - 第三层（最高层）：`DWS层，数据服务层`
>   - ==主题指标计算存储层，存储结果==
>   - 数据来源：从DWD层加载拉宽数据，按照业务指标统计分析
>   - 开发程序：SparkSQL编程，业务指标计算
>
> ==每个主题指标开发，各个层次数据都是存储在Kudu表中，类似前面讲解在线教育项目中各个主题指标层数据存储在Hive表中。==

![1613873267062](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613873267062.png)

> 在每个主题报表开始时，需要`编写2个SparkSQL应用程序`：
>
> - 第一个程序：DWD层数据，对业务数据进行拉宽操作
>   - 比如以快递单主题为例：`ExpressBillDWD`
> - 第二个程序：DWS层数据，按照业务指标进行计算操作
>   - 比如以快递单主题为例：`ExpressBillDWS`
>
> ==DWD层数据依赖ODS层数据，DWS层数据依赖DWD层数据，层层依赖关系。==

![1613873807646](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613873807646.png)

> 上图为实时同步过来业务数据，就属于ODS层数据。



## 07-[掌握]-主题及指标开发之离线模块初始化 

> ​		在进行各个主题报表开发之前，首先需要初始化操作：创建包、工具类等等

- step1、创建包结构

![1613873922461](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613873922461.png)

> 包创建完成结构如下所示：

![1613873937900](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613873937900.png)



- step2、时间处理工具类

> ​		任何一个大数据项目，都需要获取时间日期及对时间日期进行转换操作，比如物流项目来说，离线报表属于每日统计报表：今天统计昨天数据，所以需要获取昨天日期及今天时间。
>
> ==可以自己编写工具类，获取当前日期及以前日期，也可以使用SparkSQL提供日期函数==

![1613874117874](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613874117874.png)

> 代码如下：

```scala
package cn.itcast.logistics.common

import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

/**
 * 时间处理工具类
 */
object DateHelper {
	
	/**
	 * 返回昨天的时间
	 */
	def getYesterday(format: String = "yyyy-MM-dd"): String = {
		val dateFormat = FastDateFormat.getInstance(format)
		//当前时间减去一天（昨天时间）
		dateFormat.format(new Date(System.currentTimeMillis() - 1000 * 60 * 60 * 24))
	}
	
	/**
	 * 返回今天的时间
	 */
	def getToday(format: String = "yyyy-MM-dd HH:mm:ss"): String = {
		//获取指定格式的当前时间
		FastDateFormat.getInstance(format).format(new Date)
	}
	
	
	def main(args: Array[String]): Unit = {
		println(s"Today: ${getToday()}")
		println(s"Yesterday: ${getYesterday()}")
	}
}

```

> 直接SparkSQL中提供日期函数即可：

![1613874397501](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613874397501.png)



- step3、每个主题都需要拉宽操作，将拉宽后的数据存储到Kudu表中，同时指标计算的数据最终也需
  要落地到Kudu表，因此提前将各个主题相关表名定义出来。

![1613874515060](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613874515060.png)

> 代码如下：

```scala
package cn.itcast.logistics.common

/**
 * 自定义离线计算结果表
 */
object OfflineTableDefine {
	
	// 快递单明细表
	val EXPRESS_BILL_DETAIL: String = "tbl_express_bill_detail"
	// 快递单指标结果表
	val EXPRESS_BILL_SUMMARY: String = "tbl_express_bill_summary"
	
	// 运单明细表
	val WAY_BILL_DETAIL: String = "tbl_waybill_detail"
	// 运单指标结果表
	val WAY_BILL_SUMMARY: String = "tbl_waybill_summary"
	
	// 仓库明细表
	val WAREHOUSE_DETAIL: String = "tbl_warehouse_detail"
	// 仓库指标结果表
	val WAREHOUSE_SUMMARY: String = "tbl_warehouse_summary"
	
	// 网点车辆明细表
	val DOT_TRANSPORT_TOOL_DETAIL: String = "tbl_dot_transport_tool_detail"
	// 仓库车辆明细表
	val WAREHOUSE_TRANSPORT_TOOL_DETAIL: String = "tbl_warehouse_transport_tool_detail"
	// 网点车辆指标结果表
	val DOT_TRANSPORT_TOOL_SUMMARY: String = "tbl_dot_transport_tool_summary"
	// 仓库车辆指标结果表
	val WAREHOUSE_TRANSPORT_TOOL_SUMMARY: String = "tbl_warehouse_transport_tool_summary"
	
	// 客户明细表数据
	val CUSTOMER_DETAIL: String = "tbl_customer_detail"
	// 客户指标结果表数据
	val CUSTOMER_SUMMERY: String = "tbl_customer_summary"
	
}

```



- step4、根据物流字典表的数据类型定义成枚举工具类，物流字典表的

![1613874654020](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613874654020.png)

> 代码如下：

```scala
package cn.itcast.logistics.common

/**
 * 定义物流字典编码类型映射工具类
 */
object CodeTypeMapping {
	
	//注册渠道
	val REGISTER_CHANNEL: Int = 1
	//揽件状态
	val COLLECT_STATUS: Int = 2
	//派件状态
	val DISPATCH_STATUS: Int = 3
	//快递员状态
	val COURIER_STATUS: Int = 4
	//地址类型
	val ADDRESS_TYPE: Int = 5
	//网点状态
	val Dot_Status: Int = 6
	//员工状态
	val STAFF_STATUS: Int = 7
	//是否保价
	val IS_INSURED: Int = 8
	//运输工具类型
	val TRANSPORT_TYPE: Int = 9
	//运输工具状态
	val TRANSPORT_STATUS: Int = 10
	//仓库类型
	val WAREHOUSE_TYPE: Int = 11
	//是否租赁
	val IS_RENT: Int = 12
	//货架状态
	val GOODS_SHELVES_STATUE: Int = 13
	//回执单状态
	val RECEIPT_STATUS: Int = 14
	//出入库类型
	val WAREHOUSING_TYPE: Int = 15
	//客户类型
	val CUSTOM_TYPE: Int = 16
	//下单终端类型
	val ORDER_TERMINAL_TYPE: Int = 17
	//下单渠道类型
	val ORDER_CHANNEL_TYPE: Int = 18
}

```



## 08-[掌握]-主题及指标开发之公共接口【结构】

> ​		主题开发数据的来源都是来自于Kudu数据库，将数据进行拉宽或者将计算好的指标最终需要写入到Kudu表中，因此根据以上流程抽象出来公共接口。

![1613875910508](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613875910508.png)

> 离线主题指标开发公共接口：BasicOfflineApp，与前面实时ETL公共基类：BasicStreamApp类似。

```scala
package cn.itcast.logistics.offline

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 根据不同的主题开发，定义抽象方法
	 *- 1. 数据读取load：从Kudu数据库的ODS层读取数据，事实表和维度表
	 *- 2. 数据处理process：要么是拉链关联宽表，要么是依据业务指标分析得到结果表
	 *- 3. 数据保存save：将宽表或结果表存储Kudu数据库的DWD层或者DWS层
 */
trait BasicOfflineApp {
	
	/**
	 * 读取Kudu表的数据，依据指定Kudu表名称
	 *
	 * @param spark SparkSession实例对象
	 * @param tableName 表的名
	 * @param isLoadFullData 是否加载全量数据，默认值为false
	 */
	def load(spark: SparkSession, tableName: String, isLoadFullData: Boolean = false): DataFrame = ???
	
	/**
	 * 数据处理，要么对事实表进行拉链操作，要么对宽表进行指标计算
	 */
	def process(dataframe: DataFrame): DataFrame
	
	/**
	 * 数据存储: DWD及DWS层的数据都是需要写入到kudu数据库中，写入逻辑相同
	 *
	 * @param dataframe 数据集，主题指标结果数据
	 * @param tableName Kudu表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认为true，当表不存在时创建表
	 */
	def save(dataframe: DataFrame, tableName: String, isAutoCreateTable: Boolean = true)
	
}
```



## 09-[掌握]-主题及指标开发之公共接口【编程】

> 前面已经编写公共接口：`BasicOfflineApp` 程序结构，其中load和save都是与Kudu打交道：
>
> - 加载数据：`load` 方法，从Kudu表读取数据，可以具体实现
>   - 是否加载全量数据
>   - 如果是增量数据，就是昨日数据，使用日期时间函数
> - 保存数据：`save` 方法，将数据保存到Kudu表，可以具体实现
>   - 表是否存在，如果不存在，创建表
>   - 创建表时，主键使用默认值`keys=Seq(id)`

```scala
package cn.itcast.logistics.offline

import cn.itcast.logistics.common.{Configuration, KuduTools}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * 根据不同的主题开发，定义抽象方法
	 *- 1. 数据读取load：从Kudu数据库的ODS层读取数据，事实表和维度表
	 *- 2. 数据处理process：要么是拉链关联宽表，要么是依据业务指标分析得到结果表
	 *- 3. 数据保存save：将宽表或结果表存储Kudu数据库的DWD层或者DWS层
 */
trait BasicOfflineApp {
	
	/**
	 * 读取Kudu表的数据，依据指定Kudu表名称
	 *
	 * @param spark SparkSession实例对象
	 * @param tableName 表的名
	 * @param isLoadFullData 是否加载全量数据，默认值为false
	 */
	def load(spark: SparkSession, tableName: String, isLoadFullData: Boolean = false): DataFrame = {
		// 首先，需要判断是否全量加载数据，如果不是增量加载（加载昨日数据）
		if(isLoadFullData){
			// 全量加载数据
			spark.read
    			.format(Configuration.SPARK_KUDU_FORMAT)
				.option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
				.option("kudu.table", tableName)
				.option("kudu.socketReadTimeoutMs", "10000")
    			.load()
		}else{
			// 增量加载数据，加载昨日数据
			spark.read
				.format(Configuration.SPARK_KUDU_FORMAT)
				.option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
				.option("kudu.table", tableName)
				.option("kudu.socketReadTimeoutMs", "10000")
				.load()
    			.filter(
				    // cdt: 2002-06-17 04:38:00 , String -> 2002-06-17
				    date_sub(current_date(), 1).cast(StringType) === substring(col("cdt"), 0, 10)
			    )
		}
	}
	
	/**
	 * 数据处理，要么对事实表进行拉链操作，要么对宽表进行指标计算
	 */
	def process(dataframe: DataFrame): DataFrame
	
	/**
	 * 数据存储: DWD及DWS层的数据都是需要写入到kudu数据库中，写入逻辑相同
	 *
	 * @param dataframe 数据集，主题指标结果数据
	 * @param tableName Kudu表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认为true，当表不存在时创建表
	 */
	def save(dataframe: DataFrame, tableName: String, isAutoCreateTable: Boolean = true): Unit = {
		
		// 首先，判断是否允许创建表，如果允许，表不存在时再创建
		if(isAutoCreateTable){
			KuduTools.createKuduTable(tableName, dataframe) // TODO: 主键keys默认为id列名称
		}
		
		// 保存数据
		dataframe.write
			.mode(SaveMode.Append)
    		.format(Configuration.SPARK_KUDU_FORMAT)
			.option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
			.option("kudu.table", tableName)
			.option("kudu.operation", "upsert")
			.option("kudu.socketReadTimeoutMs", "10000")
    		.save()
	}
	
}

```



## 10-[理解]-快递单主题之数据调研及业务分析 

> 开发第一主题报表：快递单主题（ExpressBill），对于物流快递行业来说，快递单时主要业务数据。

![1613877296969](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613877296969.png)

> 快递单量的统计主要是从多个不同的维度计算快递单量，从而监测快递公司业务运营情况。

![1613877478216](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613877478216.png)

![1613877483767](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613877483767.png)

> 统计总的快递单数及按照`客户、渠道、网点、终端`维度统计快递单数。

> 快递单表（tbl_express_bill）相关维度表如下所示：

![1613878033645](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613878033645.png)

> 事实表（快递单表）与维度表关联关系示意图：

![1613878084214](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613878084214.png)

> 了解事实表与维度表关联关系，首先将事实表与维度进行关联拉宽操作，再进行指标计算。



## 11-[掌握]-快递单主题之数据拉宽【MAIN 方法】

> 每个主题业务报表开发，分为2个部分操作：
>
> - 第一部分、DWD层，将事实表数据进行拉宽，与维度表关联，选取字段
> - 第二部分、DWS层，对宽表进行安装业务指标进行计算

![1613878173250](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613878173250.png)

> ​		对快递单主题进行`拉宽（快递单表与维度表关联）`操作，将事实表【快递单表：tbl_express_bill】与相关维度表进行关联JOIN拉宽操作，最后保存至Kudu表（DWD：数据仓库明细层）中，为主题业务指标提供数据来源。

> ​		创建对象：`ExpressBillDWD`，继承基类`BasicOfflineApp`，实现其中`proess`数据处理的方法，在`MAIN`方法中定义程序逻辑。

```scala
package cn.itcast.logistics.offline.dwd

import cn.itcast.logistics.common.{Configuration, OfflineTableDefine, SparkUtils, TableMapping}
import cn.itcast.logistics.offline.BasicOfflineApp
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object ExpressBillDWD extends BasicOfflineApp{
	
	/**
	 * 数据处理，对事实表进行拉链操作
	 */
	override def process(dataframe: DataFrame): DataFrame = ???
	
	// SparkSQL 应用程序入口：MAIN 方法
	/*
	数据处理，实现步骤：
		step1. 创建SparkSession对象，传递SparkConf对象
		step2. 加载Kudu中的事实表数据
		step3. 加载维度表数据，与事实表进行关联
		step4. 将拉宽后的数据再次写回到Kudu数据库中
	*/
	def main(args: Array[String]): Unit = {
		// step1. 创建SparkSession对象，传递SparkConf对象
		val spark: SparkSession = SparkUtils.createSparkSession(
			SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass
		)
		import spark.implicits._
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		
		// step2. 加载Kudu中的事实表数据
		val expressBillDF: DataFrame = load(
			spark, TableMapping.EXPRESS_BILL, isLoadFullData = Configuration.IS_FIRST_RUNNABLE
		)
		expressBillDF.show(10, truncate = false)
		
	
		// step3. 加载维度表数据，与事实表进行关联
		val expressBillDetailDF: DataFrame = process(expressBillDF)
		expressBillDetailDF.show(10, truncate = false)
		
		// step4. 将拉宽后的数据再次写回到Kudu数据库中
		save(expressBillDetailDF, OfflineTableDefine.EXPRESS_BILL_DETAIL)
		
		// 应用结束，关闭资源
		spark.stop()
	}
}

```

> 完成MAIN中宽表拉链操作步骤，分为4个步骤：实例化SparkSession、加载数据、拉宽操作和保存数据



## 12-[掌握]-快递单主题之数据拉宽【process 方法】

> ​		接下来在`ExpressBillDWD`对象中实现`process`方法：将事实表（快递单表数据）与维度表数据进行关联操作。

```scala
	/**
	 * 数据处理，对事实表进行拉链操作
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		// dataframe：事实表数据ExpressBillDF
		
		// 获取SparkSession对象并隐式导入
		val spark: SparkSession = dataframe.sparkSession
		import spark.implicits._
		
		// 1. 加载Kudu中维度表数据
		// 1.1：加载快递员维度表的数据
		val courierDF: DataFrame = load(spark, TableMapping.COURIER, isLoadFullData = true)
		// 1.2：加载客户维度表的数据
		val customerDF: DataFrame = load(spark, TableMapping.CUSTOMER, isLoadFullData = true)
		// 1.3：加载物流码表的数据
		val codesDF: DataFrame = load(spark, TableMapping.CODES, isLoadFullData = true)
		// 1.4：客户地址关联表的数据
		val addressMapDF: DataFrame = load(spark, TableMapping.CONSUMER_ADDRESS_MAP, isLoadFullData = true)
		// 1.5：加载地址表的数据
		val addressDF: DataFrame = load(spark, TableMapping.ADDRESS, isLoadFullData = true)
		// 1.6：加载包裹表的数据
		val pkgDF: DataFrame = load(spark, TableMapping.PKG, isLoadFullData = true)
		// 1.7：加载网点表的数据
		val dotDF: DataFrame = load(spark, TableMapping.DOT, isLoadFullData = true)
		// 1.8：加载公司网点表的数据
		val companyDotMapDF: DataFrame = load(spark, TableMapping.COMPANY_DOT_MAP, isLoadFullData = true)
		// 1.9：加载公司表的数据
		val companyDF: DataFrame = load(spark, TableMapping.COMPANY, isLoadFullData = true)
		// 1.10：获取终端类型码表数据
		val orderTerminalTypeDF: DataFrame = codesDF
			.where($"type" === CodeTypeMapping.ORDER_TERMINAL_TYPE)
			.select(
				$"code".as("OrderTerminalTypeCode"),
				$"codeDesc".as("OrderTerminalTypeName")
			)
		// 1.11：获取下单渠道类型码表数据
		val orderChannelTypeDF: DataFrame = codesDF
			.where($"type" === CodeTypeMapping.ORDER_CHANNEL_TYPE)
			.select(
				$"code".as("OrderChannelTypeCode"),
				$"codeDesc".as("OrderChannelTypeName")
			)
		
		// 2. 将事实表与维度进行关联：leftJoin，左外连接
		val expressBillDF: DataFrame = dataframe
		val joinType: String = "left_outer"
		val joinDF: DataFrame = expressBillDF
			// 快递单表与快递员表进行关联
    		.join(courierDF, expressBillDF("eid") === courierDF("id"), joinType)
			// 快递单表与客户表进行关联
			.join(customerDF, expressBillDF("cid") === customerDF("id"), joinType)
			// 下单渠道表与快递单表关联
			.join(
				orderChannelTypeDF,
				orderChannelTypeDF("OrderChannelTypeCode") === expressBillDF("orderChannelId"),
				joinType
			)
			// 终端类型表与快递单表关联
			.join(
				orderTerminalTypeDF,
				orderTerminalTypeDF("OrderTerminalTypeCode") === expressBillDF("orderTerminalType"),
				joinType
			)
			// 客户地址关联表与客户表关联
			.join(addressMapDF, addressMapDF("consumerId") === customerDF("id"), joinType)
			// 地址表与客户地址关联表关联
			.join(addressDF, addressDF("id") === addressMapDF("addressId"), joinType)
			// 包裹表与快递单表关联
			.join(pkgDF, pkgDF("pwBill") === expressBillDF("expressNumber"), joinType)
			// 网点表与包裹表关联
			.join(dotDF, dotDF("id") === pkgDF("pwDotId"), joinType)
			// 公司网点关联表与网点表关联
			.join(companyDotMapDF, companyDotMapDF("dotId") === dotDF("id"), joinType)
			// 公司网点关联表与公司表关联
			.join(companyDF, companyDF("id") === companyDotMapDF("companyId"), joinType)
		
		// 3. 选取所需要的字段及添加字段date（表示数据属于哪一天）
		val expressBillDetailDF: DataFrame = joinDF
			// 按照快递时间进行排序
			.sort(expressBillDF("cdt").asc)
			// 选取字段
    		.select(
			    expressBillDF("id"), // 快递单id
			    expressBillDF("expressNumber").as("express_number"), //快递单编号
			    expressBillDF("cid"), //客户id
			    customerDF("name").as("cname"), //客户名称
			    addressDF("detailAddr").as("caddress"), //客户地址
			    expressBillDF("eid"), //员工id
			    courierDF("name").as("ename"), //员工名称
			    dotDF("id").as("dot_id"), //网点id
			    dotDF("dotName").as("dot_name"), //网点名称
			    companyDF("companyName").as("company_name"),//公司名称
			    expressBillDF("orderChannelId").as("order_channel_id"), //下单渠道id
			    orderChannelTypeDF("OrderChannelTypeName").as("order_channel_name"), //下单渠道id
			    expressBillDF("orderDt").as("order_dt"), //下单时间
			    orderTerminalTypeDF("OrderTerminalTypeCode").as("order_terminal_type"), //下单设备类型id
			    orderTerminalTypeDF("OrderTerminalTypeName").as("order_terminal_type_name"), //下单设备类型id
			    expressBillDF("orderTerminalOsType").as("order_terminal_os_type"),//下单设备操作系统
			    expressBillDF("reserveDt").as("reserve_dt"),//预约取件时间
			    expressBillDF("isCollectPackageTimeout").as("is_collect_package_timeout"),//是否取件超时
			    expressBillDF("timeoutDt").as("timeout_dt"),//超时时间
			    customerDF("type"),//客户类型
			    expressBillDF("cdt"),//创建时间
			    expressBillDF("udt"),//修改时间
			    expressBillDF("remark")//备注
		    )
			// 添加字段：day，从快递单中字段：cdt获取
			.withColumn("day", date_format(expressBillDF("cdt"), "yyyy-MM-dd"))
		
		// 4. 返回拉链后宽表
		expressBillDetailDF
	}
```



## 13-[掌握]-快递单主题之指标计算【MAIN 方法】

> ​		前面已经完成快递单表数据拉宽操作（事实表与维度表关联），接下来从DWD层加载宽表数据，按照业务指标进行计算。

![1613880827163](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613880827163.png)

> 创建对象：`ExpressBillDWS`，继承`BasicOfflineApp` 特征，实现其中`process`方法，进行指标计算

![1613880943692](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613880943692.png)

> 实现DWS层MAIN方法，按照4个步骤：初始化SparkSession实例，加载数据、指标计算和保存数据



```scala
package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common._
import cn.itcast.logistics.offline.BasicOfflineApp
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 快递单主题开发：
 *      加载Kudu中快递单宽表：dwd_tbl_express_bill_detail 数据，按照业务进行指标统计
 */
object ExpressBillDWS extends BasicOfflineApp{
	/**
	 * 数据处理，对宽表进行指标计算
	 */
	override def process(dataframe: DataFrame): DataFrame = ???
	
	// SparkSQL 程序入口
	def main(args: Array[String]): Unit = {
		// step1. 创建SparkSession对象，传递SparkConf对象
		val spark: SparkSession = SparkUtils.createSparkSession(
			SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass
		)
		import spark.implicits._
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		
		// step2. 加载Kudu中的事实表数据
		val expressBillDetailDF: DataFrame = load(
			spark, OfflineTableDefine.EXPRESS_BILL_DETAIL, isLoadFullData = Configuration.IS_FIRST_RUNNABLE
		)
		expressBillDetailDF.show(10, truncate = false)
		
		// step3. 按照业务进行指标计算
		val expressBillSummaryDF: DataFrame = process(expressBillDetailDF)
		expressBillSummaryDF.show(10, truncate = false)
		
		// step4. 将拉宽后的数据再次写回到Kudu数据库中
		save(expressBillSummaryDF, OfflineTableDefine.EXPRESS_BILL_SUMMARY)
		
		// 应用结束，关闭资源
		spark.stop()
	}
}

```



## 14-[掌握]-快递单主题之指标计算【process 方法】

> ​		按照快递单业务指标需要，进行统计分析，具体指标如下所示：

![1613889456129](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613889456129.png)

> 具体指标计算代码如下：

```scala
	/**
	 * 数据处理，对宽表进行指标计算
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		// 获取SparkSession实例对象并导入隐式转换
		val spark: SparkSession = dataframe.sparkSession
		import spark.implicits._
		
		/**
		    考虑加载宽表数据是否是全量数据，如果是，需要按照每天day划分
		 */
		// 定义一个列表，存储每天计算指标，类型Row
		val rowList: ListBuffer[Row] = new  ListBuffer[Row]()
		dataframe.select($"day").distinct().collect().foreach{dayRow =>
			// 获取日期day值
			val dayValue: String = dayRow.getString(0)
			
			// 获取每天宽表数据
			val expressBillDetailDF: DataFrame = dataframe.filter($"day" === dayValue)
			
			// 指标计算
			// 指标一、总快递单数
			val totalDF: DataFrame = expressBillDetailDF.agg(count($"id").as("total"))
			
			// 指标二、各类客户快递单数：最大、最小和平均
			val aggTypeTotalDF: DataFrame = expressBillDetailDF.groupBy($"type").count()
			val typeTotalDF: DataFrame = aggTypeTotalDF.agg(
				max($"count").as("maxTypeTotal"), //
				min($"count").as("minTypeTotal"), //
				round(avg($"count"), 0).as("avgTypeTotal")//
			)
			
			// 指标三、各网点快递单数：最大、最小和平均
			val aggDotTotalDF: DataFrame = expressBillDetailDF.groupBy($"dot_id").count()
			val dotTotalDF: DataFrame = aggDotTotalDF.agg(
				max($"count").as("maxDotTotal"), //
				min($"count").as("minDotTotal"), //
				round(avg($"count"), 0).as("avgDotTotal")//
			)
			
			// 指标四、各渠道快递单数：最大、最小和平均
			val aggChannelTotalDF: DataFrame = expressBillDetailDF.groupBy($"order_channel_id").count()
			val channelTotalDF: DataFrame = aggChannelTotalDF.agg(
				max($"count").as("maxChannelTotal"), //
				min($"count").as("minChannelTotal"), //
				round(avg($"count"), 0).as("avgChannelTotal")//
			)
			
			// 指标五、各终端快递单数：最大、最小和平均
			val aggTerminalTotalDF: DataFrame = expressBillDetailDF.groupBy($"order_terminal_type").count()
			val terminalTotalDF: DataFrame = aggTerminalTotalDF.agg(
				max($"count").as("maxTerminalTotal"), //
				min($"count").as("minTerminalTotal"), //
				round(avg($"count"), 0).as("avgTerminalTotal")//
			)
			
			// 将每天统计各个指标封装到Row对象中
			/*
				在SparkSQL中Row创建方式？？？
				-1. Row(v1, v2, v3, ...)
				-2. Row.fromSeq(Seq(v1, v2, v3, ...))
			 */
			val aggRow: Row = Row.fromSeq(
                Seq(dayValue) ++
					totalDF.first().toSeq ++
					typeTotalDF.first().toSeq ++
					dotTotalDF.first().toSeq ++
					channelTotalDF.first().toSeq ++
					terminalTotalDF.first().toSeq
			)
			// 加入列表
			rowList += aggRow
		}
		
		
		
		
		null
	}
```

> ​		其中计算指标时，需要考虑宽表数据`是否为全量数据`，如果是，需要按照每天划分，进行单独统计，最后将`每天统计指标结果封装到Row`中，放入`可变列表ListBuffer`中。



## 15-[掌握]-快递单主题之指标计算【转换DataFrame】

> ​		前面对宽表数据进行指标计算，每天指标封装到Row中，所有的都放入列表ListBuffer中，但是process方法，最终返回值类型为DataFrame，所以需要将ListBuffer转换为DataFrame进行返回。
>
> [将ListBuffer（RDD）转换为DataFrame有几种方式？？？？？]()
>
> - 1）、方式一、反射推断
>   - 要求：RDD[CaseClass]样例类
> - 2）、方式二、自定义Schema
>   - 要求：RDD[Row]，自定义Schema（StructType），SparkSession.createDataFrame转换
>
> ==通过并行化方式（parallelized）将ListBuffer转换为RDD分布式数据集==

```scala
		// TODO: 将ListBuffer转换为DataFrame，首先转换列表为RDD，然后自定义Schame，最后转换为DataFrame
		// step1. 采用并行化方式转换ListBuffer为RDD
		val rowRDD: RDD[Row] = spark.sparkContext.parallelize(rowList)
		// step2. 自定义Schema信息，必须与Row字段类型匹配
		val schema: StructType = new StructType()
    		.add("id", StringType, nullable = false)
			.add("total", LongType, nullable = true)
			.add("maxTypeTotal", LongType, nullable = true)
			.add("minTypeTotal", LongType, nullable = true)
			.add("avgTypeTotal", DoubleType, nullable = true)
			.add("maxDotTotal", LongType, nullable = true)
			.add("minDotTotal", LongType, nullable = true)
			.add("avgDotTotal", DoubleType, nullable = true)
			.add("maxChannelTotal", LongType, nullable = true)
			.add("minChannelTotal", LongType, nullable = true)
			.add("avgChannelTotal", DoubleType, nullable = true)
			.add("maxTerminalTotal", LongType, nullable = true)
			.add("minTerminalTotal", LongType, nullable = true)
			.add("avgTerminalTotal", DoubleType, nullable = true)
		// step3. 转换RDD[Row]为DataFrame
		val aggDF: DataFrame = spark.createDataFrame(rowRDD, schema)
```

> 至此快递单指标计算完成，可以进行测试，完整代码如下：

```scala
package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common._
import cn.itcast.logistics.offline.BasicOfflineApp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
 * 快递单主题开发：
 *      加载Kudu中快递单宽表：dwd_tbl_express_bill_detail 数据，按照业务进行指标统计
 */
object ExpressBillDWS extends BasicOfflineApp{
	/**
	 * 数据处理，对宽表进行指标计算
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		// 获取SparkSession实例对象并导入隐式转换
		val spark: SparkSession = dataframe.sparkSession
		import spark.implicits._
		
		/**
		    考虑加载宽表数据是否是全量数据，如果是，需要按照每天day划分
		 */
		// 定义一个列表，存储每天计算指标，类型Row
		val rowList: ListBuffer[Row] = new  ListBuffer[Row]()
		dataframe.select($"day").distinct().collect().foreach{dayRow =>
			// 获取日期day值
			val dayValue: String = dayRow.getString(0) // 20210118,20210119,20210120
			
			// 获取每天宽表数据
			val expressBillDetailDF: DataFrame = dataframe.filter($"day" === dayValue)
			// 由于后续使用多次，可以进行缓存
			expressBillDetailDF.persist(StorageLevel.MEMORY_AND_DISK)
			
			// 指标计算
			// 指标一、总快递单数
			val totalDF: DataFrame = expressBillDetailDF.agg(count($"id").as("total"))
			
			// 指标二、各类客户快递单数：最大、最小和平均
			val aggTypeTotalDF: DataFrame = expressBillDetailDF.groupBy($"type").count()
			val typeTotalDF: DataFrame = aggTypeTotalDF.agg(
				max($"count").as("maxTypeTotal"), //
				min($"count").as("minTypeTotal"), //
				round(avg($"count"), 0).as("avgTypeTotal")//
			)
			
			// 指标三、各网点快递单数：最大、最小和平均
			val aggDotTotalDF: DataFrame = expressBillDetailDF.groupBy($"dot_id").count()
			val dotTotalDF: DataFrame = aggDotTotalDF.agg(
				max($"count").as("maxDotTotal"), //
				min($"count").as("minDotTotal"), //
				round(avg($"count"), 0).as("avgDotTotal")//
			)
			
			// 指标四、各渠道快递单数：最大、最小和平均
			val aggChannelTotalDF: DataFrame = expressBillDetailDF.groupBy($"order_channel_id").count()
			val channelTotalDF: DataFrame = aggChannelTotalDF.agg(
				max($"count").as("maxChannelTotal"), //
				min($"count").as("minChannelTotal"), //
				round(avg($"count"), 0).as("avgChannelTotal")//
			)
			
			// 指标五、各终端快递单数：最大、最小和平均
			val aggTerminalTotalDF: DataFrame = expressBillDetailDF.groupBy($"order_terminal_type").count()
			val terminalTotalDF: DataFrame = aggTerminalTotalDF.agg(
				max($"count").as("maxTerminalTotal"), //
				min($"count").as("minTerminalTotal"), //
				round(avg($"count"), 0).as("avgTerminalTotal")//
			)
			
			// 数据不再使用，释放资源
			expressBillDetailDF.unpersist()
			
			// 将每天统计各个指标封装到Row对象中
			/*
				在SparkSQL中Row创建方式？？？
				-1. Row(v1, v2, v3, ...)
				-2. Row.fromSeq(Seq(v1, v2, v3, ...))
			 */
			val aggRow: Row = Row.fromSeq(
				Seq(dayValue) ++
				totalDF.first().toSeq ++
					typeTotalDF.first().toSeq ++
					dotTotalDF.first().toSeq ++
					channelTotalDF.first().toSeq ++
					terminalTotalDF.first().toSeq
			)
			// 加入列表
			rowList += aggRow
		}
		
		// TODO: 将ListBuffer转换为DataFrame，首先转换列表为RDD，然后自定义Schame，最后转换为DataFrame
		// step1. 采用并行化方式转换ListBuffer为RDD
		val rowRDD: RDD[Row] = spark.sparkContext.parallelize(rowList)
		// step2. 自定义Schema信息，必须与Row字段类型匹配
		val schema: StructType = new StructType()
    		.add("id", StringType, nullable = false)
			.add("total", LongType, nullable = true)
			.add("maxTypeTotal", LongType, nullable = true)
			.add("minTypeTotal", LongType, nullable = true)
			.add("avgTypeTotal", DoubleType, nullable = true)
			.add("maxDotTotal", LongType, nullable = true)
			.add("minDotTotal", LongType, nullable = true)
			.add("avgDotTotal", DoubleType, nullable = true)
			.add("maxChannelTotal", LongType, nullable = true)
			.add("minChannelTotal", LongType, nullable = true)
			.add("avgChannelTotal", DoubleType, nullable = true)
			.add("maxTerminalTotal", LongType, nullable = true)
			.add("minTerminalTotal", LongType, nullable = true)
			.add("avgTerminalTotal", DoubleType, nullable = true)
		// step3. 转换RDD[Row]为DataFrame
		val aggDF: DataFrame = spark.createDataFrame(rowRDD, schema)
		
		// 返回计算指标
		aggDF
	}
	
	// SparkSQL 程序入口
	def main(args: Array[String]): Unit = {
		// step1. 创建SparkSession对象，传递SparkConf对象
		val spark: SparkSession = SparkUtils.createSparkSession(
			SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass
		)
		import spark.implicits._
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		
		// step2. 加载Kudu中的宽表数据
		val expressBillDetailDF: DataFrame = load(
			spark, OfflineTableDefine.EXPRESS_BILL_DETAIL, isLoadFullData = Configuration.IS_FIRST_RUNNABLE
		)
		expressBillDetailDF.show(10, truncate = false)
		
		// step3.  按照业务进行指标计算
		val expressBillSummaryDF: DataFrame = process(expressBillDetailDF)
		expressBillSummaryDF.show(10, truncate = false)
		
		// step4. 将拉宽后的数据再次写回到Kudu数据库中
		save(expressBillSummaryDF, OfflineTableDefine.EXPRESS_BILL_SUMMARY)
		
		// 应用结束，关闭资源
		spark.stop()
	}
}

```



## 16-[理解]-即席查询之背景介绍及业务流程

> ​			==即席查询（Ad Hoc）是用户根据自己的需求，灵活的选择查询条件，系统能够根据用户的选择生成相应的统计报表==。即席查询与普通应用查询最大的不同是普通的应用查询是定制开发的，而即席查询是由用户自定义查询条件的。

![1613893454738](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613893454738.png)

![1613893462393](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613893462393.png)

> 即席查询：最基本使用方式，提供可视化界面，以便编写SQL语句，比如Hue与Impala集成方式

![1613893561884](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613893561884.png)

> ​		此外，即席查询最好方式，不需要编写SQL语句，给用户提供界面，可以UI上选择不同条件，后台拼凑SQL语句，进行查询分析，最终将结果呈现在UI界面上。

> 针对物流项目来说，实际中会有很多临时业务报表统计需求，往往都是程序员编写SQL查询效果。

![1613893814742](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613893814742.png)

> 针对即席查询（编写SQL分析数据）业务流程示意图如下所示：

![1613893835282](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613893835282.png)



## 17-[了解]-即席查询之SQL on Hadoop 发展史

> ​			讲解Impala分析引擎之前，首先了解SQL on Hadoop发展史。从大数据框架Hadoop 出现以后，有的公司开始考虑能否为用户提供SQL方式，自动将SQL转换MapReduce分析数据：`Hive`（小蜜蜂）

> ​		Hive数仓框架可以说是SQL on Hadoop第一个框架，提供编写SQL语句方式，分析存储在大数据Hadoop之上的数据，底层分析引擎为MapReduce框架。

![1613894141601](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613894141601.png)

> ​		随着Google论文Dremel发表，新一代大数据SQL分析引擎框架出现，比如Impala、`Presto`、SparkSQL、Drill等。

![1613894199216](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613894199216.png)

> ​		其中Impala内存分析引擎框架，使用内存分析数据，采用MPP计算模式，取代Hive框架底层MapReduce，示意图如下所示：
>
> [Impala分析引擎，类似Hive框架，但是Impala不能自己管理元数据，依赖Hive MetaStore管理元数据。]()

![1613894243740](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613894243740.png)

> ​		当Cloudera公司开发新一代存储引擎Kudu，主要集成HDFS和HBase存储优势以后，Impala天然集成Kudu，分析存储数据，更加的快速，结构示意图如下：

![1613894374147](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613894374147.png)

> 目前公司大数据项目中，尤其是金融和游戏公司，存储业务数据选择Kudu，分析数据时使用Impala分析。



## 18-[理解]-即席查询之Impala集成Kudu即席查询

> ​		当Kudu存储引擎出现以后，Cloudera公司宣布可以不在使用HDFS和HBase，直接使用Kudu与Impala既可以存储，又可以分析，都是基于内存。

![1613894606751](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613894606751.png)

![1613894624305](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613894624305.png)

> 从Impala某个版本开始，在Kudu中每张表都提供在Impala中创建表的语句，更加显示出Kudu与Impala一对CP。

![1613894695588](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613894695588.png)

> 查看Kudu中快递单tbl_express_bill表详细，其中展示出与Impala集成SQL语句：

![1613894816560](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613894816560.png)

> 文档：https://kudu.apache.org/docs/kudu_impala_integration.html

## 19-[了解]-即席查询之Impala 分析引擎介绍

> ​		Impala是Cloudera提供的一款高效率的SQL查询工具，提供实时的查询效果，官方测试性能比Hive快10到100倍，其SQL查询比SparkSQL还要更加快速，号称是当前大数据领域最快的查询SQL工具。
>
> [Impala是基于Hive并使用内存进行计算，兼顾数据仓库，具有实时，批处理，多并发等优点。]()
>
> - 1）、Impala基于内存分析引擎，数据放在内存进行分布式分析
> - 2）、Impala依赖HiveMetaStore管理元数据，比如数据库、表、字段等等信息

![1613895088465](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613895088465.png)



> ​		Kudu与Apache Impala （孵化）紧密集成，Impala天然就支持兼容Kudu，允许开发人员使用Impala的SQL语法从Kudu的tablets 插入，查询，更新和删除数据；

> [Impala是基于Hive的大数据分析查询引擎，直接使用Hive的元数据库metadata]()

![1613895190744](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613895190744.png)

> Hive适合于长时间的批处理查询分析，而Impala适合于实时交互式SQL查询。

![1613895335547](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613895335547.png)

> ​			总结：Impala基于内存分析引擎，依赖内存分析数据和Hive管理元数据，使用C++语言编写的，一开始出现取代Hive，读取HDFS和HBase表的数据进行分析；后来Kudu出现以后，直接从Kudu表加载数据分析，更加快速和高效。



## 20-[理解]-即席查询之Impala 架构原理

> ​		Impala是Cloudera在受到Google的Dremel启发下开发的实时交互SQL大数据查询工具（实时SQL查询引擎Impala），通过使用与商用并行关系数据库中类似的分布式查询引擎（由QueryPlanner、Query Coordinator和Query Exec Engine三部分组成），可以直接从HDFS或HBase中用SELECT、JOIN和统计函数查询数据，从而大大降低了延迟。

![1613896374570](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613896374570.png)

> - 1）、Impalad 服务
>   - 在每个节点上运⾏的进程，是Impala的核⼼组件，进程名是Impalad;
>   - 负责读写数据⽂件，接收来⾃Impala-shell，JDBC,ODBC等的查询请求，与集群其它Impalad分布式并⾏完成查询任务，并将查询结果返回给中⼼协调者。
>   - Impalad服务由三个模块组成：Query Planner、Query Coordinator和Query Executor，前两个模块组成前端，负责接收SQL查询请求，解析SQL并转换成执⾏计划，由后端执⾏。
> - 2）、Impala State Store
>   - 监控集群中Impalad的健康状况，并将集群健康信息同步给Impalad
> - 3）、catalogd
>   - 负责把这些元数据的变化同步给其它Impalad进程
>   - 在Impala集群启动的时候加载hive元数据信息到Impala，其他时候不会主动加载，需要使用`invalidate metadata，refresh`命令
>   - 官⽅建议让statestored进程与catalogd进程安排同个节点
> - 4）、HiveMetaStore
>   - 管理元数据，往往存储在MySQL数据库中。
> - 5）、impala-shell
>   - 提供交互式SQL命令行，方便编写SQL查询分析数据



## 21-[理解]-即席查询之Impala 查询流程

> ​		在Impala分析引擎中，最核心进程：Impalad进程，由三个部分组成：
>
> - 1）、Query Planner（查询计划）：将SQL语句生成查询计划
>   - [每个Impalad进程服务都可以接收任意SQL请求，由QueryPlanner生成查询计划]()
> - 2）、Query Coordinator（查询协调器）：将查询计划分发给所有Impalad中QueryExecutor
> - 3）、Query Executor（查询执行器/查询执行引擎）：真正进行查询分析数据
>   - 接收QueryCoordinator分发查询计划任务，并执行

![1613897852722](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613897852722.png)

> - 第一步、任意一个Impalad服务接收请求（SQL语句），QueryPlaner将其生成查询计划Planner
> - 第二步、Impalad服务中QueryCoordinator将查询计划Planner发送给所有Impalad服务QueryExecutor执行器
> - 第三步、所有Impalad服务QueryExecutor执行器接收到查询计划Planner以后，执行从存储引擎（HDFS中DataNode、HBase中RegionServer、Kudu中TabletServer）读取数据，基于内存分析；
> - 第四步、当所有Impalad服务QueryExecutor执行器查询分析获取结果以后，发送给QueryCoordinator协调器，协调器汇总所有QueryExecutor发送结构，最终返回客户端。

![1613898057072](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613898057072.png)

> 文档：http://impala.apache.org/overview.html



## 22-[掌握]-即席查询之Impala 服务启动及CLI使用 

> ​		关于Impala分析引擎安装，相对来说比较复杂的，服务组件较多。针对物流项目来说，基于CM6.x界面化安装部署Impala。
>
> - 1）、先安装HDFS分布式文件系统（实际项目中`HDFS HA，基于JN方式高可用`）
> - 2）、安装Hive MetaStore，使用MySQL数据库存储元数据
> - 3）、最后安装Impala相关服务组件：
>   - Impalad服务：每个从节点（数据节点）安装此服务
>   - Catalogd元数据服务
>   - StateStored：状态监控服务
>
> [当启动Impala相关服务时，首先启动HDFS服务组件，在启动HiveMetaStore，最后启动Impalad服务]()

![1613899261333](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613899261333.png)

> 当使用CM安装大数据框架时，基本配置说明：
>
> - 1）、impala安装客户端配置文件：`/etc/impala/conf`
> - 2）、日志目录：`/var/log/impalad`



> 当Impala服务启动完成以后，提供WEB UI界面：
>
> - 1）、catalogd  服务界面：http://node2.itcast.cn:25020/

![1613899509395](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613899509395.png)

> - 2）、statestored  服务界面：http://node2.itcast.cn:25010/

![1613899577816](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613899577816.png)



> - 3）、impalad  服务界面：http://node2.itcast.cn:25000/

![1613899657868](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613899657868.png)

> Impala分析引擎使用，类似Hive提供几种方式，具体如下所示：

![1613899685230](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613899685230.png)

> 首先使用提交交互式命令：impala-shell连接服务，编写SQL查询分析：

```ini
[root@node2 ~]# which impala-shell
/usr/bin/impala-shell
[root@node2 ~]# 
[root@node2 ~]# impala-shell --help
Usage: impala_shell.py [options]

Options:
  -h, --help            show this help message and exit
  -i IMPALAD, --impalad=IMPALAD
                        <host:port> of impalad to connect to
                        [default: node2.itcast.cn:21000]
  -b KERBEROS_HOST_FQDN, --kerberos_host_fqdn=KERBEROS_HOST_FQDN
                        If set, overrides the expected hostname of the
                        Impalad's kerberos service principal. impala-shell
                        will check that the server's principal matches this
                        hostname. This may be used when impalad is configured
                        to be accessed via a load-balancer, but it is desired
                        for impala-shell to talk to a specific impalad
                        directly. [default: none]
  -q QUERY, --query=QUERY
                        Execute a query without the shell [default: none]
  -f QUERY_FILE, --query_file=QUERY_FILE
                        Execute the queries in the query file, delimited by ;.
                        If the argument to -f is "-", then queries are read
                        from stdin and terminated with ctrl-d. [default: none]
  -k, --kerberos        Connect to a kerberized impalad [default: False]
  -o OUTPUT_FILE, --output_file=OUTPUT_FILE
                        If set, query results are written to the given file.
                        Results from multiple semicolon-terminated queries
                        will be appended to the same file [default: none]
  -B, --delimited       Output rows in delimited mode [default: False]
  --print_header        Print column names in delimited mode when pretty-
                        printed. [default: False]
  --output_delimiter=OUTPUT_DELIMITER
                        Field delimiter to use for output in delimited mode
                        [default: \t]
  -s KERBEROS_SERVICE_NAME, --kerberos_service_name=KERBEROS_SERVICE_NAME
                        Service name of a kerberized impalad [default: impala]
  -V, --verbose         Verbose output [default: True]
  -p, --show_profiles   Always display query profiles after execution
                        [default: False]
  --quiet               Disable verbose output [default: False]
  -v, --version         Print version information [default: False]
  -c, --ignore_query_failure
                        Continue on query failure [default: False]
  -d DEFAULT_DB, --database=DEFAULT_DB
                        Issues a use database command on startup
                        [default: none]
  -l, --ldap            Use LDAP to authenticate with Impala. Impala must be
                        configured to allow LDAP authentication.
                        [default: False]
  -u USER, --user=USER  User to authenticate with. [default: root]
  --ssl                 Connect to Impala via SSL-secured connection
                        [default: False]
  --ca_cert=CA_CERT     Full path to certificate file used to authenticate
                        Impala's SSL certificate. May either be a copy of
                        Impala's certificate (for self-signed certs) or the
                        certificate of a trusted third-party CA. If not set,
                        but SSL is enabled, the shell will NOT verify Impala's
                        server certificate [default: none]
  --config_file=CONFIG_FILE
                        Specify the configuration file to load options. The
                        following sections are used: [impala],
                        [impala.query_options]. Section names are case
                        sensitive. Specifying this option within a config file
                        will have no effect. Only specify this as an option in
                        the commandline. [default: /root/.impalarc]
  --history_file=HISTORY_FILE
                        The file in which to store shell history. This may
                        also be configured using the IMPALA_HISTFILE
                        environment variable. [default: ~/.impalahistory]
  --live_summary        Print a query summary every 1s while the query is
                        running. [default: False]
  --live_progress       Print a query progress every 1s while the query is
                        running. [default: False]
  --auth_creds_ok_in_clear
                        If set, LDAP authentication may be used with an
                        insecure connection to Impala. WARNING: Authentication
                        credentials will therefore be sent unencrypted, and
                        may be vulnerable to attack. [default: none]
  --ldap_password_cmd=LDAP_PASSWORD_CMD
                        Shell command to run to retrieve the LDAP password
                        [default: none]
  --var=KEYVAL          Defines a variable to be used within the Impala
                        session. Can be used multiple times to set different
                        variables. It must follow the pattern "KEY=VALUE", KEY
                        starts with an alphabetic character and contains
                        alphanumeric characters or underscores. [default:
                        none]
  -Q QUERY_OPTIONS, --query_option=QUERY_OPTIONS
                        Sets the default for a query option. Can be used
                        multiple times to set different query options. It must
                        follow the pattern "KEY=VALUE", KEY must be a valid
                        query option. Valid query options  can be listed by
                        command 'set'. [default: none]
  -t CLIENT_CONNECT_TIMEOUT_MS, --client_connect_timeout_ms=CLIENT_CONNECT_TIMEOUT_MS
                        Timeout in milliseconds after which impala-shell will
                        time out if it fails to connect to Impala server. Set
                        to 0 to disable any timeout. [default: 60000]
```

> 指定Impalad服务主机名称，连接Impalad服务

```ini
[root@node2 ~]# impala-shell -i node2.itcast.cn:21000
Starting Impala Shell without Kerberos authentication
Opened TCP connection to node2.itcast.cn:21000
Connected to node2.itcast.cn:21000
Server version: impalad version 3.2.0-cdh6.2.1 RELEASE (build 525e372410dd2ce206e2ad0f21f57cae7380c0cb)
***********************************************************************************
Welcome to the Impala shell.
(Impala Shell v3.2.0-cdh6.2.1 (525e372) built on Wed Sep 11 01:30:44 PDT 2019)

Press TAB twice to see a list of available commands.
***********************************************************************************
[node2.itcast.cn:21000] default> 
alter     create    describe  explain   insert    quit      set       source    tip       upsert    version   
compute   delete    drop      help      load      rerun     shell     src       unset     use       with      
connect   desc      exit      history   profile   select    show      summary   update    values    
[node2.itcast.cn:21000] default> 
[node2.itcast.cn:21000] default> show databases ;
Query: show databases
+------------------+----------------------------------------------+
| name             | comment                                      |
+------------------+----------------------------------------------+
| _impala_builtins | System database for Impala builtin functions |
| default          | Default Hive database                        |
| logistics        | ???????                                      |
+------------------+----------------------------------------------+
Fetched 3 row(s) in 0.34s
[node2.itcast.cn:21000] default> use logistics ;
Query: use logistics
[node2.itcast.cn:21000] logistics> show tables ;
Query: show tables
```



## 23-[理解]-即席查询之使用Impala操作Kudu 

> ​		当Impala操作Kudu时，首先需要启动相关服务：HDFS、Hive MetaStore、Impala及Kudu

![1613900161620](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613900161620.png)

> 使用Impala创建新的Kudu表时，可以将该表创建为内部表或外部表：
>
> - 1）、内部表：在Impala中创建表关联Kudu中表
>   - 当在Impala中删除表时，如果表时关联的，同时也删除Kudu中表。

![1613900209137](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613900209137.png)

```SQL
CREATE TABLE `my_first_table`
(
id BIGINT,
name STRING,
PRIMARY KEY(id)
)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
'kudu.num_tablet_replicas' = '1'
);

```

> 此时创建的表是内部表，从impala删除表的时候，在底层存储的kudu也会删除表

```SQL
drop table if exists my_first_table;
```

> - 2）、外部表：外部表（创建者CREATE EXTERNAL TABLE）不受Impala管理，并且删除此表不会将表从其源位置（此处为Kudu）丢弃。相反，它只会去除Impala和Kudu之间的映射。

![1613900499712](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613900499712.png)

```SQL
CREATE EXTERNAL TABLE `tbl_express_bill` STORED AS KUDU
TBLPROPERTIES(
    'kudu.table_name' = 'tbl_express_bill',
    'kudu.master_addresses' = 'node2.itcast.cn:7051') ;
```

```SQL
CREATE TABLE `tbl_users`
(
id BIGINT,
name STRING,
PRIMARY KEY(id)
)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES (
'kudu.num_tablet_replicas' = '1'
);

insert into tbl_users select * from my_first_table ;
```



## 24-[理解]-即席查询之Hue集成Impala 

> ​		前面使用Impala自带交互式命令操作Kudu，此外Hue可以与Impala集成。
>
> [首先需要启动Hue服务：Hue Server和Load Server，端口号：8888或8889]()

![1613901065050](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613901065050.png)

> 登录WEB UI界面：http://node2.itcast.cn:8889/， 用户名和密码：`admin/admin`

![1613901188251](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613901188251.png)



> 登录Hue以后，选择底层执行引擎为Impala即可，编写SQL执行查询
>

![1613901374641](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613901374641.png)







