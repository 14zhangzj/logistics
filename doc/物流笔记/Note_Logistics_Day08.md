---
stypora-copy-images-to: img
typora-root-url: ./
---



# Logistics_Day08：主题及报表开发与任务调度



## 01-[复习]-上次课程内容回顾 

> 上次课主要讲解：==离线报表分析（`SparkSQL`）==和==即席查询分析（`Impala`）==，业务数据存储在Kudu数据库。
>
> - 1）、==离线报表分析==：按照主题Topic划分报表业务，不同主题由不同指标需要计算分析。
>
>   [依据数仓分层架构：三层架构（ODS、DWD和DWS），管理数据，使得更加容易数据分析和规划化。]()

![1613962944032](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613962944032.png)

> 每个主题报表分析：首先需要进行DWD（事实表拉宽操作），然后再进行DWS（业务指标计算），抽象公共接口：BasicOfflineApp，从Kudu加载数据，进行process处理分析，将结果数据保存Kudu表。

![1614041454960](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614041454960.png)

> 每个主题报表开发时，编写2个SparkSQL应用程序：
>
> - 1）、DWD层，将事实表进行拉宽操作
>   - 加载事实表数据时，需要考虑是否全量加载；
>   - 与维度表关联时，采用作为连接LeftJoin
> - 2）、DWS层，加载宽表数据，按照业务指标计算
>   - 加载宽表数据如果是全量数据，需要按照day进行划分，对每天数据进行指标计算
>   - 将计算指标数据封装到Row对象中，最终转换为DataFrame进行返回
>
> [以【快递单主题】为例，讲解如何进行报表开发，整个流程。]()

![1613962957638](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613962957638.png)

> - 2）、即席查询Impala
>   - 基于内存分布式查询引擎，诞生目标就是取代Hive框架，使用内存分析
>   - Impala引擎架构：
>     - Impalad服务进程：包含三个部分（Query Planner、Query Coordinator、Query Executor）
>       - 与存储引擎数据存储节点放在一起，比如DataNode、RegionServer、TabletServer
>     - StateStored服务进程：管理Impalad服务状态，并且同步信息
>     - Catalogd服务进程：管理元数据，一开始加载HiveMetaStore元数据，同步数据给所有Impalad
>     - Hive MetaStore：管理元数据

![1614041732722](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614041732722.png)

> 本身Impala提供交互式命令行：`impala-shell`，直接编写SQL语句。

![1614041952644](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614041952644.png)

> 此外，可以使用Hue集成Impala，编写SQL，底层使用Impala查询分析。
>
> [Impala也支持JDBC/ODBC方式连接，类似MySQL数据库连接，编写SQL语句，提交执行即可。]()

![1614042102984](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614042102984.png)

> 当使用JDBC方式连接Impalad服务时，使用端口号为：`21050`，使用DBeave连接Impalad服务。

![1614042209429](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614042209429.png)

> 使用DBeave连接以后，编写SQL查询分析数据

![1614042366172](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614042366172.png)



## 02-[了解]-第7天：课程内容提纲

> 主要讲解2个方面内容：离线报表开发（运单`WayBill`主题和仓库`Warehouse`主题）和工作流调度框架Azkaban。
>
> - 1）、离线报表分析
>   - 重构公共接口`BasicOfflineApp`，基于`模板方法设计模式`，使得开发报表时编写更少代码。
>   - 主题：运单`WayBill`主题，报表开发，DWD层和DWS层
>   - 主题：仓库`WareHouse`主题，报表开发，DWD层和DWS层

![1614042929181](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614042929181.png)

> - 2）、工作流调度：`Azkaban`
>   - 只要是离线报表，通常都是每日统计，需要定时执行，此外，每个报表分析时，需要多个程序完成，程序执行依赖关系。
>   - 定时调度执行、依赖调度执行

![img](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/855959-20170118105739281-1040838706.png)

> 当然，也可以使用前面讲解Oozie调度执行报表。



## 03-[理解]-运单主题之数据调研及业务分析 

> 运单数据（tbl_way_bill），在物流快递中，除了快递单数据以外，第二个重要业务数据。

![1614044001296](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614044001296.png)

> ​		“运单是运输合同的证明，是承运人已经接收货物的收据。一份运单，填写托运人、收货人、起运港、到达港。
>
> ​	运单统计根据区域Area、公司Company、网点Dot、线路Route、运输工具Tool等维度进行统计，可以对各个维度运单数量进行排行，如对网点运单进行统计可以反映该网点的运营情况，对线路运单进行统计可以观察每个线路的运力情况。
>
> [运单指标与快递单指标类似，首先统计总的运单数目，再按照不同维度（区域、分公司、网点、线路、运输工具、客户类型）进行统计最大、最小和平均运单数目。]()

![1614044360601](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614044360601.png)

![1614044375491](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614044375491.png)

> 事实表：业务数据表tbl_waybill

![1614044406026](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614044406026.png)

> 运单表与维度表的关联关系如下：

![1614044578239](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614044578239.png)

> 知道运单主题指标及事实表与维度表，接下来编写DWD层和DWS层应用程序。



## 04-[掌握]-主题及指标开发之重构公共接口【思路】

> ​		在开发每个主题指标程序之前，发现DWD层和DWS层应用程序中，尤其是MAIN方法，代码重复度很高，进行传递表名称不一样。

- 1）、DWD层程序MAIN方法：

![1614044743644](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614044743644.png)



- 2）、DWS层程序MAIN方法

![1614044782330](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614044782330.png)



> ​		原因在于MAIN方法中，规定程序执行步骤（也就是说，第一步、第二步、第三步、….），可以将其提取为方法execute，放到公共接口中，在子类中直接调用execute方法，传递参数即可。
>
> [使用面向对象中设计模式：模板方法设计模式Template Parttern]()

> ​		==模板方法模式（Template Pattern），是一种类继承模式，主要是通过一个抽象类，把子类一些共有的类提取出来（称为基本方法）放到抽象类中，并在抽象类中定义一个模板方法，在模板方法中规定基本方法的执行顺序==。将不同的实现细节交给子类去实现。

![1614045041467](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614045041467.png)

> 案例说明如下：小明和小华每天上学【叠被子、吃早餐和乘坐交通工具】，其中交通工具不一样

![1614045245263](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614045245263.png)

> [重构离线报表分析公共接口，使用模板方法设计模式完成，框架代码如下：]()

```scala
package cn.itcast.logistics.offline

import org.apache.spark.sql.{DataFrame, SparkSession}

trait AbstractOfflineApp {
	
	// 定义变量，SparkSession实例对象
	var spark: SparkSession = _
	
	/**
	 * 初始化SparkSession实例
	 */
	def init(clazz: Class[_]): Unit = {
	
	}
	
	/**
	 * 从Kudu数据源加载数据，指定表的名称
	 * @param tableName 表的名称
	 * @return 分布式数据集DataFrame
	 */
	def loadKuduSource(tableName: String): DataFrame = ???
	
	/**
	 * 对数据集DataFrame按照业务需求编码，由子类复杂实现
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	def process(dataframe: DataFrame): DataFrame
	
	/**
	 * 将数据集DataFrame保存至Kudu表中，指定表的名
	 *
	 * @param dataframe 要保存的数据集
	 * @param tableName 保存表的名称
	 */
	def saveKuduSink(dataframe: DataFrame, tableName: String,
	                 isAutoCreateTable: Boolean = true, keys: Seq[String] = Seq("id")): Unit = ???
	
	/**
	 * 程序结束，关闭资源，SparkSession关闭
	 */
	def close(): Unit = ???
	
	/**
	 * 模板方法，确定基本方法执行顺序
	 */
	def execute(clazz: Class[_], srcTable: String, dstTable: String,
	            isAutoCreateTable: Boolean = true, keys: Seq[String] = Seq("id")): Unit = {
		// step1. 实例化SparkSession对象
		init(clazz)
		
		try{
			// step2. 从Kudu表加载数据
			val kuduDF: DataFrame = loadKuduSource(srcTable)
			kuduDF.show(10, truncate = false)
			
			// step3. 处理数据
			val resultDF: DataFrame = process(kuduDF)
			resultDF.show(10, truncate = false)
			
			// step4. 保存数据到Kudu表
			saveKuduSink(resultDF, dstTable, isAutoCreateTable, keys)
			
		}catch {
			case e: Exception => e.printStackTrace()
		}finally {
			// step5. 关闭资源
			close()
		}
	
	}
}

```

![1614045318315](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614045318315.png)

## 05-[掌握]-主题及指标开发之重构公共接口【编程】

> ​		将重构抽象公共接口：AbstractOfflineApp中每个基本方法实现，代码如下所示：

```scala
package cn.itcast.logistics.offline

import cn.itcast.logistics.common.{Configuration, KuduTools, SparkUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, current_date, date_sub, substring}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

trait AbstractOfflineApp {
	
	// 定义变量，SparkSession实例对象
	var spark: SparkSession = _
	
	/**
	 * 初始化SparkSession实例
	 */
	def init(clazz: Class[_]): Unit = {
		// 获取SparkConf对象
		var sparkConf: SparkConf = SparkUtils.sparkConf()
		// 设置运行模型，依据运行环境
		sparkConf = SparkUtils.autoSettingEnv(sparkConf)
		// 实例化SparkSession对象
		spark = SparkSession.builder().config(sparkConf).getOrCreate()
	}
	
	/**
	 * 从Kudu数据源加载数据，指定表的名称
	 * 
	 * @param tableName 表的名称
	 * @param isLoadFullData 是否加载全量数据，默认值为false
	 * @return 分布式数据集DataFrame
	 */
	def loadKuduSource(tableName: String, isLoadFullData: Boolean = false): DataFrame = {
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
	 * 对数据集DataFrame按照业务需求编码，由子类复杂实现
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	def process(dataframe: DataFrame): DataFrame
	
	/**
	 * 将数据集DataFrame保存至Kudu表中，指定表的名
	 *
	 * @param dataframe 要保存的数据集
	 * @param tableName 保存表的名称
	 */
	def saveKuduSink(dataframe: DataFrame, tableName: String,
	                 isAutoCreateTable: Boolean = true, keys: Seq[String] = Seq("id")): Unit = {
		// 首先，判断是否允许创建表，如果允许，表不存在时再创建
		if(isAutoCreateTable){
			KuduTools.createKuduTable(tableName, dataframe, keys) // TODO: 主键keys默认为id列名称
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
	
	/**
	 * 程序结束，关闭资源，SparkSession关闭
	 */
	def close(): Unit = {
		if(null != spark) spark.close()
	}
	
	/**
	 * 模板方法，确定基本方法执行顺序
	 */
	def execute(clazz: Class[_], 
	            srcTable: String, dstTable: String,
	            isLoadFullData: Boolean = false,
	            isAutoCreateTable: Boolean = true, keys: Seq[String] = Seq("id")): Unit = {
		// step1. 实例化SparkSession对象
		init(clazz)
		
		try{
			// step2. 从Kudu表加载数据
			val kuduDF: DataFrame = loadKuduSource(srcTable, isLoadFullData)
			kuduDF.show(10, truncate = false)
			
			// step3. 处理数据
			val resultDF: DataFrame = process(kuduDF)
			resultDF.show(10, truncate = false)
			
			// step4. 保存数据到Kudu表
			saveKuduSink(resultDF, dstTable, isAutoCreateTable, keys)
		}catch {
			case e: Exception => e.printStackTrace()
		}finally {
			// step5. 关闭资源
			close()
		}
	}
}

```

> ​		在`execute`模板方法中，需要定义**所有基本方法需要参数**，由于在子类中，仅仅调用execute模板方法，不会调用其他基本方法，所以需要传递参数。
>



## 06-[掌握]-运单主题之数据拉宽开发

> ​		重构完成公共接口以后，编写运单主题数据拉宽操作应用程序：WayBillDWD，继承Trait：AbstractOfflineAPP。

```scala
package cn.itcast.logistics.offline.dwd

import cn.itcast.logistics.common._
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 运单主题开发：
 * 将运单事实表的数据与相关维度表的数据进行关联，然后将拉宽后的数据写入到运单宽表中
 */
object WayBillDWD extends AbstractOfflineApp{
	/**
	 * 对数据集DataFrame按照业务需求编码，由子类复杂实现
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		// 获取SparkSession实例对象
		val session = dataframe.sparkSession
		import session.implicits._
		
		// 1. 加载事实表相关维度表数据，全部是全量加载
		// 加载快递员表
		val courierDF: DataFrame = loadKuduSource(TableMapping.COURIER, isLoadFullData = true)
		// 加载网点表
		val dotDF: DataFrame =  loadKuduSource(TableMapping.DOT, isLoadFullData = true)
		// 加载区域表
		val areasDF: DataFrame =  loadKuduSource(TableMapping.AREAS, isLoadFullData = true)
		// 加载转运记录表
		val recordDF: DataFrame =  loadKuduSource(TableMapping.TRANSPORT_RECORD, isLoadFullData = true)
		// 加载起始仓库表
		val startWarehouseDF: DataFrame = loadKuduSource(TableMapping.WAREHOUSE, isLoadFullData = true)
		// 加载到达仓库表
		val endWarehouseDF: DataFrame = loadKuduSource(TableMapping.WAREHOUSE, isLoadFullData = true)
		// 加载车辆表
		val toolDF: DataFrame = loadKuduSource(TableMapping.TRANSPORT_TOOL, isLoadFullData = true)
		// 加载线路表
		val routeDF: DataFrame = loadKuduSource(TableMapping.ROUTE, isLoadFullData = true)
		// 加载起始仓库关联表
		val startCompanyWarehouseDF: DataFrame = loadKuduSource(TableMapping.COMPANY_WAREHOUSE_MAP, isLoadFullData = true)
		// 加载到达仓库关联表
		val endCompanyWarehouseDF: DataFrame = loadKuduSource(TableMapping.COMPANY_WAREHOUSE_MAP, isLoadFullData = true)
		// 加载起始仓库所在公司表
		val startCompanyDF: DataFrame = loadKuduSource(TableMapping.COMPANY, isLoadFullData = true)
		// 加载到达仓库所在公司表
		val endCompanyDF: DataFrame = loadKuduSource(TableMapping.COMPANY, isLoadFullData = true)
		// 加载物流码表
		val codesDF: DataFrame = loadKuduSource(TableMapping.CODES, isLoadFullData = true)
		// 加载客户表
		val customerDF: DataFrame = loadKuduSource(TableMapping.CUSTOMER, isLoadFullData = true)
		
		// 下单渠道类型表
		val orderChannelTypeDF: DataFrame = codesDF
			.where(col("type") === CodeTypeMapping.ORDER_CHANNEL_TYPE)
			.select(
				col("code").as("orderChannelTypeCode"), col("codeDesc").as("orderChannelTypeName")
			)
		// 客户类型表
		val customerTypeDF: DataFrame = codesDF
			.where(col("type") === CodeTypeMapping.CUSTOM_TYPE)
			.select(
				col("code").as("customerTypeCode"), col("codeDesc").as("customerTypeName")
			)
		
		// 2. 事实表与维度表进行关联，leftJoin，并且选取字段和添加day字段
		val left_outer = "left_outer"
		val wayBillDF: DataFrame = dataframe
		val wayBillDetailDF = wayBillDF
			// 运单表与快递员表进行关联
			.join(courierDF, wayBillDF("eid") === courierDF("id"), left_outer)
			// 网点表与快递员表进行关联
			.join(dotDF, courierDF("dotId") === dotDF("id"), left_outer)
			// 网点表与区域表进行关联
			.join(areasDF, areasDF("id") === dotDF("manageAreaId"), left_outer)
			// 转运记录表与运单表关联
			.join(recordDF, recordDF("pwWaybillNumber") === wayBillDF("waybillNumber"), left_outer)
			// 起始仓库与转运记录表关联
			.join(startWarehouseDF, startWarehouseDF("id") === recordDF("swId"), left_outer)
			// 到达仓库与转运记录表关联
			.join(endWarehouseDF, endWarehouseDF("id") === recordDF("ewId"), left_outer)
			// 转运记录表与交通工具表关联
			.join(toolDF, toolDF("id") === recordDF("transportToolId"), left_outer)
			// 转运记录表与路线表关联
			.join(routeDF, routeDF("id") === recordDF("routeId"), left_outer)
			// 起始仓库表与仓库公司关联表关联
			.join(startCompanyWarehouseDF, startCompanyWarehouseDF("warehouseId") === startWarehouseDF("id"), left_outer)
			// 公司表与起始仓库公司关联表关联
			.join(startCompanyDF, startCompanyDF("id") === startCompanyWarehouseDF("companyId"), left_outer)
			// 到达仓库表与仓库公司关联表关联
			.join(endCompanyWarehouseDF, endCompanyWarehouseDF("warehouseId") === endWarehouseDF("id"), left_outer)
			// 公司表与到达仓库公司关联表关联
			.join(endCompanyDF, endCompanyDF("id") === endCompanyWarehouseDF("companyId"), left_outer)
			// 运单表与客户表关联
			.join(customerDF, customerDF("id") === wayBillDF("cid"), left_outer)
			// 下单渠道表与运单表关联
			.join(orderChannelTypeDF, orderChannelTypeDF("orderChannelTypeCode") ===  wayBillDF("orderChannelId"), left_outer)
			// 客户类型表与客户表关联
			.join(customerTypeDF, customerTypeDF("customerTypeCode") === customerDF("type"), left_outer)
			// 选择字段
			.select(
				wayBillDF("id"), //运单id
				wayBillDF("expressBillNumber").as("express_bill_number"), //快递单编号
				wayBillDF("waybillNumber").as("waybill_number"), //运单编号
				wayBillDF("cid"), //客户id
				customerDF("name").as("cname"), //客户名称
				customerDF("type").as("ctype"), //客户类型
				customerTypeDF("customerTypeName").as("ctype_name"), //客户类型名称
				wayBillDF("eid"), //快递员id
				courierDF("name").as("ename"), //快递员名称
				dotDF("id").as("dot_id"), //网点id
				dotDF("dotName").as("dot_name"), //网点名称
				areasDF("id").as("area_id"), //区域id
				areasDF("name").as("area_name"), //区域名称
				wayBillDF("orderChannelId").as("order_channel_id"), //渠道id
				orderChannelTypeDF("orderChannelTypeName").as("order_chanel_name"), //渠道名称
				wayBillDF("orderDt").as("order_dt"), //下单时间
				wayBillDF("orderTerminalType").as("order_terminal_type"), //下单设备类型
				wayBillDF("orderTerminalOsType").as("order_terminal_os_type"), //下单设备操作系统类型
				wayBillDF("reserveDt").as("reserve_dt"), //预约取件时间
				wayBillDF("isCollectPackageTimeout").as("is_collect_package_timeout"), //是否取件超时
				wayBillDF("pkgId").as("pkg_id"), //订装ID
				wayBillDF("pkgNumber").as("pkg_number"), //订装编号
				wayBillDF("timeoutDt").as("timeout_dt"), //超时时间
				wayBillDF("transformType").as("transform_type"), //运输方式
				wayBillDF("deliveryAddr").as("delivery_addr"),
				wayBillDF("deliveryCustomerName").as("delivery_customer_name"),
				wayBillDF("deliveryMobile").as("delivery_mobile"),
				wayBillDF("deliveryTel").as("delivery_tel"),
				wayBillDF("receiveAddr").as("receive_addr"),
				wayBillDF("receiveCustomerName").as("receive_customer_name"),
				wayBillDF("receiveMobile").as("receive_mobile"),
				wayBillDF("receiveTel").as("receive_tel"),
				wayBillDF("cdt"),
				wayBillDF("udt"),
				wayBillDF("remark"),
				recordDF("swId").as("sw_id"),
				startWarehouseDF("name").as("sw_name"),
				startCompanyDF("id").as("sw_company_id"),
				startCompanyDF("companyName").as("sw_company_name"),
				recordDF("ewId").as("ew_id"),
				endWarehouseDF("name").as("ew_name"),
				endCompanyDF("id").as("ew_company_id"),
				endCompanyDF("companyName").as("ew_company_name"),
				toolDF("id").as("tt_id"),
				toolDF("licensePlate").as("tt_name"),
				recordDF("routeId").as("route_id"),
				concat(routeDF("startStation"), routeDF("endStation")).as("route_name")
			)
			// 3). 添加字段，日期字段day增加日期列
			.withColumn("day", date_format(wayBillDF("cdt"), "yyyyMMdd"))
			// 根据运单表的创建时间顺序排序
			.sort(wayBillDF.col("cdt").asc)
		
		// 3. 返回宽表数据
		wayBillDetailDF
	}
	
	def main(args: Array[String]): Unit = {
		// 直接调用模板方法execute，传递参数即可
		execute(
			this.getClass, //
			TableMapping.WAY_BILL, // 事实表
			OfflineTableDefine.WAY_BILL_DETAIL, // 拉链宽表
			isLoadFullData = Configuration.IS_FIRST_RUNNABLE
		)
	}
}

```

> 运行DWD应用程序，查看Kudu表中数据，如下截图所示：

![1614047151257](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614047151257.png)



## 07-[掌握]-运单主题之指标计算【MAIN 方法】

> ​		前面已经对运单业务数据进行拉宽操作，将数据存储到Kudu的DWD层：tbl_waybill_detail，接下来，编写DWS层指标计算程序：`WayBillDWS`，继承特质：`AbstractOfflineApp`。

```scala
package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common.{Configuration, OfflineTableDefine}
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * 运单主题指标开发：
 * 从Kudu表加载宽表数据，按照业务指标进行统计分析：基于不同维度分组聚合，类似快递单指表指标。
 */
object WayBillDWS extends AbstractOfflineApp {
	/**
	 * 对数据集DataFrame按照业务需求编码，由子类复杂实现
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		// 获取SparkSession实例对象
		val session = dataframe.sparkSession
		import session.implicits._
		
		
		
	}
	
	def main(args: Array[String]): Unit = {
		// 调用模板方法，传递参数
		execute(
			this.getClass, //
			OfflineTableDefine.WAY_BILL_DETAIL, //
			OfflineTableDefine.WAY_BILL_SUMMARY, //
			isLoadFullData = Configuration.IS_FIRST_RUNNABLE //
		)
	}
}

```

> ​		此时可以发现，重构公共接口以后，只需要在子类MAIN方法中调用父类中execute模板方法，传递参数即可，将重点放在process方法中，不用去管理执行执行逻辑顺序。
>



## 08-[掌握]-运单主题之指标计算【process 方法】

> ​		每个主题指标计算，需要考虑是否对全量数据指标计算，如果是，需要按照day天划分宽表数据，对每天业务数据进行指标计算，最后将指标封装到Row对象中，并且转换为DataFrame数据集进行返回。

```scala
package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common.{Configuration, OfflineTableDefine}
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
 * 运单主题指标开发：
 * 从Kudu表加载宽表数据，按照业务指标进行统计分析：基于不同维度分组聚合，类似快递单指表指标。
 */
object WayBillDWS extends AbstractOfflineApp {
	/**
	 * 对数据集DataFrame按照业务需求编码，由子类复杂实现
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		// 此时dataframe为 宽表数据，其中有字段day
		
		// 获取SparkSession实例对象
		val session = dataframe.sparkSession
		import session.implicits._
		
		/*
			无论是全量数据还是增量数据，都是按照day进行划分指标计算，将每天指标计算封装到Row中，存储到列表ListBuffer中
		 */
		// step1. 按天day划分业务数据，计算指标，封装为Row对象
		val rowList: ListBuffer[Row] = new ListBuffer[Row]()
		dataframe.select($"day").distinct().collect().foreach{dayRow =>
			// 获取day值
			val dayValue: String = dayRow.getString(0)
			
			// 过滤获取每天运单表数据
			val wayBillDetailDF: DataFrame = dataframe.filter($"day".equalTo(dayValue))
			
			// 计算指标
			// 指标一、总运单数
			
			// 指标二、各区域运单数：最大、最小和平均
			
			// 指标三、各分公司运单数：最大、最小和平均
			
			// 指标四、各网点运单数：最大、最小和平均
			
			// 指标五、各线路运单数：最大、最小和平均
			
			// 指标六、各运输工具运单数：最大、最小和平均
			
			// 指标七、各客户类型运单数：最大、最小和平均
			
			// 封装指标到Row对象
			val aggRow = Row.fromSeq(
				dayRow.toSeq
			)
			// 加入到列表中
			rowList += aggRow
		}
		
		// step2. 将计算指标，存储到DataFrame中
		
		// step3. 返回计算指标数据集
		null
	}
	
	def main(args: Array[String]): Unit = {
		// 调用模板方法，传递参数
		execute(
			this.getClass, //
			OfflineTableDefine.WAY_BILL_DETAIL, //
			OfflineTableDefine.WAY_BILL_SUMMARY, //
			isLoadFullData = Configuration.IS_FIRST_RUNNABLE //
		)
	}
}

```

```
面试题：RDD、DataFrame及Dataset区别是什么？？？？？
```

> 前面将指标思路步骤思路已经写出来接下来逐步完成每一步代码编写。

```scala
		/*
			无论是全量数据还是增量数据，都是按照day进行划分指标计算，将每天指标计算封装到Row中，存储到列表ListBuffer中
		 */
		// step1. 按天day划分业务数据，计算指标，封装为Row对象
		val rowList: ListBuffer[Row] = new ListBuffer[Row]()
		dataframe.select($"day").distinct().collect().foreach{dayRow =>
			// 获取day值
			val dayValue: String = dayRow.getString(0)
			
			// 过滤获取每天运单表数据
			val wayBillDetailDF: DataFrame = dataframe.filter($"day".equalTo(dayValue))
			
			// 计算指标
			// 指标一、总运单数
			val totalDF: DataFrame = wayBillDetailDF.agg(count($"id").as("total"))
			
			// 指标二、各区域运单数：最大、最小和平均
			val areaTotalDF: DataFrame = wayBillDetailDF.groupBy($"area_id").count()
			val areaAggTotalDF: DataFrame = areaTotalDF.agg(
				max($"count").as("maxAreaTotal"), //
				min($"count").as("minAreaTotal"), //
				round(avg($"count"), 0).as("avgAreaTotal") //
			)
			
			// 指标三、各分公司运单数：最大、最小和平均
			val companyTotalDF: DataFrame = wayBillDetailDF.groupBy($"sw_company_name").count()
			val companyAggTotalDF: DataFrame = companyTotalDF.agg(
				max($"count").as("maxCompanyTotal"), //
				min($"count").as("minCompanyTotal"), //
				round(avg($"count"), 0).as("avgCompanyTotal") //
			)
			
			// 指标四、各网点运单数：最大、最小和平均
			val dotTotalDF: DataFrame = wayBillDetailDF.groupBy($"dot_id").count()
			val dotAggTotalDF: DataFrame = dotTotalDF.agg(
				max($"count").as("maxDotTotal"), //
				min($"count").as("minDotTotal"), //
				round(avg($"count"), 0).as("avgDotTotal") //
			)
			
			// 指标五、各线路运单数：最大、最小和平均
			val routeTotalDF: DataFrame = wayBillDetailDF.groupBy($"route_id").count()
			val routeAggTotalDF: DataFrame = routeTotalDF.agg(
				max($"count").as("maxRouteTotal"), //
				min($"count").as("minRouteTotal"), //
				round(avg($"count"), 0).as("avgRouteTotal") //
			)
			
			// 指标六、各运输工具运单数：最大、最小和平均
			val ttTotalDF: DataFrame = wayBillDetailDF.groupBy($"tt_id").count()
			val ttAggTotalDF: DataFrame = ttTotalDF.agg(
				max($"count").as("maxTtTotal"), //
				min($"count").as("minTtTotal"), //
				round(avg($"count"), 0).as("avgTtTotal") //
			)
			
			// 指标七、各客户类型运单数：最大、最小和平均
			val typeTotalDF: DataFrame = wayBillDetailDF.groupBy($"ctype").count()
			val typeAggTotalDF: DataFrame = typeTotalDF.agg(
				max($"count").as("maxTypeTotal"), //
				min($"count").as("minTypeTotal"), //
				round(avg($"count"), 0).as("avgTypeTotal") //
			)
			
			// 封装指标到Row对象
			val aggRow = Row.fromSeq(
				dayRow.toSeq ++
					totalDF.first().toSeq ++
					areaAggTotalDF.first().toSeq ++
					companyAggTotalDF.first().toSeq ++
					dotAggTotalDF.first().toSeq ++
					routeAggTotalDF.first().toSeq ++
					ttAggTotalDF.first().toSeq ++
					typeAggTotalDF.first().toSeq
			)
			// 加入到列表中
			rowList += aggRow
		}
```

> 上述代码，仅仅针对每天运单数据进行指标计算，由于每天数据被使用多次计算指标，可以将将其缓存。

## 09-[掌握]-运单主题之指标计算【转换DataFrame】

> ​	需要将计算指标，转换为DataFrame数据集，前面使用自定义Schema方式，此处换一种方式实现。

![1614050354359](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614050354359.png)

> 由于需要将Row中数据放到元组中，操作比较复杂，所以继续使用原来方式。

```scala
		// step2. 将计算指标，存储到DataFrame中
		// a. RDD[Row]
		val rowRDD: RDD[Row] = session.sparkContext.parallelize(rowList.toList)
		// b. 自定义Schema
		val schema: StructType = new StructType()
			.add("id", StringType, nullable = false) // 针对每天数据进行聚合得到一个结果，设置day为结果表中id
			.add("total", LongType, nullable = true)
			.add("maxAreaTotal", LongType, nullable = true)
			.add("minAreaTotal", LongType, nullable = true)
			.add("avgAreaTotal", DoubleType, nullable = true)
			.add("maxCompanyTotal", LongType, nullable = true)
			.add("minCompanyTotal", LongType, nullable = true)
			.add("avgCompanyTotal", DoubleType, nullable = true)
			.add("maxDotTotal", LongType, nullable = true)
			.add("minDotTotal", LongType, nullable = true)
			.add("avgDotTotal", DoubleType, nullable = true)
			.add("maxRouteTotal", LongType, nullable = true)
			.add("minRouteTotal", LongType, nullable = true)
			.add("avgRouteTotal", DoubleType, nullable = true)
			.add("maxToolTotal", LongType, nullable = true)
			.add("minToolTotal", LongType, nullable = true)
			.add("avgToolTotal", DoubleType, nullable = true)
			.add("maxCtypeTotal", LongType, nullable = true)
			.add("minCtypeTotal", LongType, nullable = true)
			.add("avgCtypeTotal", DoubleType, nullable = true)
		// c. 构建DataFrame
		val aggDF: DataFrame = session.createDataFrame(rowRDD, schema)
		
		// step3. 返回计算指标数据集
		aggDF
```

> 运单主题指标计算完整代码如下所示：

```scala
package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common.{Configuration, OfflineTableDefine}
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
 * 运单主题指标开发：
 * 从Kudu表加载宽表数据，按照业务指标进行统计分析：基于不同维度分组聚合，类似快递单指表指标。
 */
object WayBillDWS extends AbstractOfflineApp {
	/**
	 * 对数据集DataFrame按照业务需求编码，由子类复杂实现
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		// 此时dataframe为 宽表数据，其中有字段day
		
		// 获取SparkSession实例对象
		val session = dataframe.sparkSession
		import session.implicits._
		
		/*
			无论是全量数据还是增量数据，都是按照day进行划分指标计算，将每天指标计算封装到Row中，存储到列表ListBuffer中
		 */
		// step1. 按天day划分业务数据，计算指标，封装为Row对象
		val rowList: ListBuffer[Row] = new ListBuffer[Row]()
		dataframe.select($"day").distinct().collect().foreach{dayRow =>
			// 获取day值
			val dayValue: String = dayRow.getString(0)
			
			// 过滤获取每天运单表数据
			val wayBillDetailDF: DataFrame = dataframe.filter($"day".equalTo(dayValue))
			wayBillDetailDF.persist(StorageLevel.MEMORY_AND_DISK)
			
			// 计算指标
			// 指标一、总运单数
			val totalDF: DataFrame = wayBillDetailDF.agg(count($"id").as("total"))
			
			// 指标二、各区域运单数：最大、最小和平均
			val areaTotalDF: DataFrame = wayBillDetailDF.groupBy($"area_id").count()
			val areaAggTotalDF: DataFrame = areaTotalDF.agg(
				max($"count").as("maxAreaTotal"), //
				min($"count").as("minAreaTotal"), //
				round(avg($"count"), 0).as("avgAreaTotal") //
			)
			
			// 指标三、各分公司运单数：最大、最小和平均
			val companyTotalDF: DataFrame = wayBillDetailDF.groupBy($"sw_company_name").count()
			val companyAggTotalDF: DataFrame = companyTotalDF.agg(
				max($"count").as("maxCompanyTotal"), //
				min($"count").as("minCompanyTotal"), //
				round(avg($"count"), 0).as("avgCompanyTotal") //
			)
			
			// 指标四、各网点运单数：最大、最小和平均
			val dotTotalDF: DataFrame = wayBillDetailDF.groupBy($"dot_id").count()
			val dotAggTotalDF: DataFrame = dotTotalDF.agg(
				max($"count").as("maxDotTotal"), //
				min($"count").as("minDotTotal"), //
				round(avg($"count"), 0).as("avgDotTotal") //
			)
			
			// 指标五、各线路运单数：最大、最小和平均
			val routeTotalDF: DataFrame = wayBillDetailDF.groupBy($"route_id").count()
			val routeAggTotalDF: DataFrame = routeTotalDF.agg(
				max($"count").as("maxRouteTotal"), //
				min($"count").as("minRouteTotal"), //
				round(avg($"count"), 0).as("avgRouteTotal") //
			)
			
			// 指标六、各运输工具运单数：最大、最小和平均
			val ttTotalDF: DataFrame = wayBillDetailDF.groupBy($"tt_id").count()
			val ttAggTotalDF: DataFrame = ttTotalDF.agg(
				max($"count").as("maxTtTotal"), //
				min($"count").as("minTtTotal"), //
				round(avg($"count"), 0).as("avgTtTotal") //
			)
			
			// 指标七、各客户类型运单数：最大、最小和平均
			val typeTotalDF: DataFrame = wayBillDetailDF.groupBy($"ctype").count()
			val typeAggTotalDF: DataFrame = typeTotalDF.agg(
				max($"count").as("maxTypeTotal"), //
				min($"count").as("minTypeTotal"), //
				round(avg($"count"), 0).as("avgTypeTotal") //
			)
			
			// 数据不再使用，释放缓存
			wayBillDetailDF.unpersist()
			
			// 封装指标到Row对象
			val aggRow = Row.fromSeq(
				dayRow.toSeq ++
					totalDF.first().toSeq ++
					areaAggTotalDF.first().toSeq ++
					companyAggTotalDF.first().toSeq ++
					dotAggTotalDF.first().toSeq ++
					routeAggTotalDF.first().toSeq ++
					ttAggTotalDF.first().toSeq ++
					typeAggTotalDF.first().toSeq
			)
			// 加入到列表中
			rowList += aggRow
		}
		
		// step2. 将计算指标，存储到DataFrame中
		// a. RDD[Row]
		val rowRDD: RDD[Row] = session.sparkContext.parallelize(rowList.toList)
		// b. 自定义Schema
		val schema: StructType = new StructType()
			.add("id", StringType, nullable = false) // 针对每天数据进行聚合得到一个结果，设置day为结果表中id
			.add("total", LongType, nullable = true)
			.add("maxAreaTotal", LongType, nullable = true)
			.add("minAreaTotal", LongType, nullable = true)
			.add("avgAreaTotal", DoubleType, nullable = true)
			.add("maxCompanyTotal", LongType, nullable = true)
			.add("minCompanyTotal", LongType, nullable = true)
			.add("avgCompanyTotal", DoubleType, nullable = true)
			.add("maxDotTotal", LongType, nullable = true)
			.add("minDotTotal", LongType, nullable = true)
			.add("avgDotTotal", DoubleType, nullable = true)
			.add("maxRouteTotal", LongType, nullable = true)
			.add("minRouteTotal", LongType, nullable = true)
			.add("avgRouteTotal", DoubleType, nullable = true)
			.add("maxToolTotal", LongType, nullable = true)
			.add("minToolTotal", LongType, nullable = true)
			.add("avgToolTotal", DoubleType, nullable = true)
			.add("maxCtypeTotal", LongType, nullable = true)
			.add("minCtypeTotal", LongType, nullable = true)
			.add("avgCtypeTotal", DoubleType, nullable = true)
		// c. 构建DataFrame
		val aggDF: DataFrame = session.createDataFrame(rowRDD, schema)
		
		// step3. 返回计算指标数据集
		aggDF
	}
	
	def main(args: Array[String]): Unit = {
		// 调用模板方法，传递参数
		execute(
			this.getClass, //
			OfflineTableDefine.WAY_BILL_DETAIL, //
			OfflineTableDefine.WAY_BILL_SUMMARY, //
			isLoadFullData = Configuration.IS_FIRST_RUNNABLE //
		)
	}
}

```

> 运行DWS程序，查看Kudu表数据，截图如下所示：

![1614050961770](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614050961770.png)



## 10-[掌握]-仓库主题之数据调研及业务分析 

> ​		物流快递行业，除了快递单数据和运单数据以外，最重要的数据就是仓库相关数据，进行指标计算。

![1614052316580](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614052316580.png)

> 仓库主题指标，与前面运单主题类似，按照不同维度分区统计，获取最大、最小及平均仓库发车次数。

![1614052380777](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614052380777.png)

> 事实表相关维度表如下：依据指标计算时涉及到维度查找

![1614052437188](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614052437188.png)

> 运输记录表与维度表的关联关系如下：
>

![1614052544470](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614052544470.png)

> 接下来，按照主题指标计算步骤进行开发：DWD层宽表操作和DWS操作指标计算操作。



## 11-[掌握]-仓库主题之数据拉宽开发

> ​		在dwd目录下创建 WarehouseDWD 单例对象，继承自AbstractOfflineApp特质，实现事实表的拉宽操作。

```scala
package cn.itcast.logistics.offline.dwd

import cn.itcast.logistics.common.{CodeTypeMapping, Configuration, OfflineTableDefine, TableMapping}
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 * 仓库主题宽表：
 *      从ODS层加载事实表（运输记录表）和相关维度表数据，进行JOIN关联拉链为宽表
 */
object WarehouseDWD extends AbstractOfflineApp{
	/**
	 * 对数据集DataFrame按照业务需求编码，由子类复杂实现
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		// 获取SparkSession实例对象
		val session = dataframe.sparkSession
		import session.implicits._
		
		// step1. 加载事实表相关维度表数据
		// 加载公司仓库关联表的数据
		val companyWareHouseMapDF: DataFrame = loadKuduSource(TableMapping.COMPANY_WAREHOUSE_MAP, isLoadFullData = true)
		// 加载公司表的数据
		val companyDF: DataFrame = loadKuduSource(TableMapping.COMPANY, isLoadFullData = true)
		// 加载区域表的数据
		val areasDF: DataFrame = loadKuduSource(TableMapping.AREAS, isLoadFullData = true)
		// 加载运单表的数据
		val wayBillDF: DataFrame = loadKuduSource( TableMapping.WAY_BILL, isLoadFullData = true)
		// 加载快递单表的数据
		val expressBillDF: DataFrame = loadKuduSource(TableMapping.EXPRESS_BILL, isLoadFullData = true)
		// 加载客户寄件信息表数据
		val senderInfoDF: DataFrame = loadKuduSource(TableMapping.CONSUMER_SENDER_INFO, isLoadFullData = true)
		// 加载包裹表数据
		val expressPackageDF: DataFrame = loadKuduSource(TableMapping.EXPRESS_PACKAGE, isLoadFullData = true)
		// 加载客户表数据
		val customerDF: DataFrame = loadKuduSource(TableMapping.CUSTOMER, isLoadFullData = true)
		// 加载物流码表数据
		val codesDF: DataFrame = loadKuduSource(TableMapping.CODES, isLoadFullData = true)
		// 加载仓库表数据
		val warehouseDF: DataFrame = loadKuduSource(TableMapping.WAREHOUSE, isLoadFullData = true)
		// 加载入库数据
		val phWarehouseDF: DataFrame = loadKuduSource(TableMapping.PUSH_WAREHOUSE, isLoadFullData = true)
		// 加载入库数据
		val dotDF: DataFrame = loadKuduSource(TableMapping.DOT, isLoadFullData = true)
		// 客户类型表
		val customerTypeDF: DataFrame = codesDF
			.where(col("type") === CodeTypeMapping.CUSTOM_TYPE)
			.select(col("code").as("customerTypeCode"), col("codeDesc").as("customerTypeName"))
		
		// step2. 进行关联（leftJoin），选取字段并添加day天字段
		val joinType: String = "left_outer"
		val recordDF: DataFrame = dataframe
		val recordDetailDF: Dataset[Row] = recordDF
			// 转运记录表与公司仓库关联表关联
			.join(
				companyWareHouseMapDF,
				recordDF.col("swId") === companyWareHouseMapDF.col("warehouseId"),
				joinType
			)
			// 公司仓库关联表与公司表关联
			.join(
				companyDF,
				companyWareHouseMapDF.col("companyId") === companyDF.col("id"),
				joinType
			)
			// 公司表与区域表关联
			.join(areasDF, companyDF.col("cityId") === areasDF.col("id"), joinType)
			// 运单表与转运记录表关联
			.join(
				wayBillDF,
				recordDF.col("pwWaybillNumber") === wayBillDF.col("waybillNumber"),
				joinType)
			//运单表与快递单表关联
			.join(
				expressBillDF,
				wayBillDF.col("expressBillNumber") === expressBillDF.col("expressNumber"),
				joinType
			)
			// 客户寄件信息表与快递单表关联
			.join(
				senderInfoDF,
				expressBillDF.col("cid") === senderInfoDF.col("ciid"),
				joinType
			)
			// 客户寄件信息表与包裹表关联
			.join(
				expressPackageDF,
				senderInfoDF.col("pkgId") === expressPackageDF.col("id"),
				joinType
			)
			// 客户寄件信息表与客户表关联
			.join(
				customerDF,
				senderInfoDF.col("ciid") === customerDF.col("id"),
				joinType
			)
			//客户表与客户类别表关联
			.join(
				customerTypeDF,
				customerDF.col("type") === customerTypeDF.col("customerTypeCode"),
				joinType
			)
			// 转运记录表与仓库表关联
			.join(warehouseDF, recordDF.col("swId")===warehouseDF.col("id"), joinType)
			// 入库表与仓库表关联
			.join(
				phWarehouseDF,
				phWarehouseDF.col("warehouseId")=== warehouseDF.col("id"),
				joinType
			)
			// 转运记录表与网点表关联
			.join(dotDF, dotDF("id")=== phWarehouseDF.col("pwDotId"), joinType)
			.select(
				recordDF("id"),         //转运记录id
				recordDF("pwId").as("pw_id"),   //入库表的id
				recordDF("pwWaybillId").as("pw_waybill_id"),    //入库运单id
				recordDF("pwWaybillNumber").as("pw_waybill_number"),  //入库运单编号
				recordDF("owId").as("ow_id"),       //出库id
				recordDF("owWaybillId").as("ow_waybill_id"),  //出库运单id
				recordDF("owWaybillNumber").as("ow_waybill_number"),  //出库运单编号
				recordDF("swId").as("sw_id"),     //起点仓库id
				warehouseDF.col("name").as("sw_name"),  //起点仓库名称
				recordDF("ewId").as("ew_id"),    //到达仓库id
				recordDF("transportToolId").as("transport_tool_id"),  //运输工具id
				recordDF("pwDriver1Id").as("pw_driver1_id"),    //入库车辆驾驶员
				recordDF("pwDriver2Id").as("pw_driver2_id"),    //入库车辆驾驶员2
				recordDF("pwDriver3Id").as("pw_driver3_id"),    //入库车辆驾驶员3
				recordDF("owDriver1Id").as("ow_driver1_id"),    //出库车辆驾驶员
				recordDF("owDriver2Id").as("ow_driver2_id"),    //出库车辆驾驶员2
				recordDF("owDriver3Id").as("ow_driver3_id"),    //出库车辆驾驶员3
				recordDF("routeId").as("route_id"),  //线路id
				recordDF("distance").cast(IntegerType),   //运输里程
				recordDF("duration").cast(IntegerType),    //运输耗时
				recordDF("state").cast(IntegerType),      //转运状态id
				recordDF("startVehicleDt").as("start_vehicle_dt"),    //发车时间
				recordDF("predictArrivalsDt").as("predict_arrivals_dt"), //预计到达时间
				recordDF("actualArrivalsDt").as("actual_arrivals_dt"),   //实际到达时间
				recordDF("cdt"), //创建时间
				recordDF("udt"), //修改时间
				recordDF("remark"), //备注
				companyDF("id").alias("company_id"), //公司id
				companyDF("companyName").as("company_name"), //公司名称
				areasDF("id").alias("area_id"),   //区域id
				areasDF("name").alias("area_name"), //区域名称
				expressPackageDF.col("id").alias("package_id"), //包裹id
				expressPackageDF.col("name").alias("package_name"), //包裹名称
				customerDF.col("id").alias("cid"),
				customerDF.col("name").alias("cname"),
				customerTypeDF("customerTypeCode").alias("ctype"),
				customerTypeDF("customerTypeName").alias("ctype_name"),
				dotDF("id").as("dot_id"),
				dotDF("dotName").as("dot_name")
			)
			// 虚拟列,可以根据这个日期列作为分区字段，可以保证同一天的数据保存在同一个分区中
			.withColumn("day", date_format(recordDF("cdt"), "yyyyMMdd"))
			.sort(recordDF.col("cdt").asc)
		
		// step3. 返回拉宽数据
		recordDetailDF
	}
	
	// SparkSQL程序运行入口
	def main(args: Array[String]): Unit = {
		// 调用模板方法execute，传递参数
		execute(
			this.getClass, //
			TableMapping.TRANSPORT_RECORD, //
			OfflineTableDefine.WAREHOUSE_DETAIL, //
			isLoadFullData = Configuration.IS_FIRST_RUNNABLE //
		)
	}
}

```

> 开发完成以后，运行程序，查看Kudu表中数据。
>

![1614053081005](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614053081005.png)



## 12-[掌握]-仓库主题之指标计算【MAIN 方法】

> ​		前面已经完成运输记录表拉宽操作，并且存储到Kudu表中（DWD层），编写程序按照业务指标进行计算，对象名称：`WarehouseDWS`，继承类：`AbstractOfflineApp`，实现其中`process`方法（指标计算）。

```scala
package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common.{Configuration, OfflineTableDefine}
import cn.itcast.logistics.offline.AbstractOfflineApp
import cn.itcast.logistics.offline.dws.WayBillDWS.execute
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

/**
 * 仓库主题报表开发：
 *      从DWD层加载仓库宽表数据，按照具体指标计算，使用不同维度进行分组统计。
 */
object WarehouseDWS extends AbstractOfflineApp {
	/**
	 * 对数据集DataFrame按照业务需求编码，由子类复杂实现
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		// 此时dataframe为 宽表数据，其中有字段day
		
		// 获取SparkSession实例对象
		val session = dataframe.sparkSession
		import session.implicits._
		
		// step1. 按天day统计业务数据指标
		// TODO: 各个主题指标计算时，无论是全量数据还是增量数据，都是按照day天进行划分计算的
		val rowList: ListBuffer[Row] = new ListBuffer[Row]()
		dataframe.select($"day").distinct().collect().foreach{dayRow =>
			// 获取每天day值
			val dayValue: String = dayRow.getString(0)
			
			// 获取每天业务数据
			val recordDetailDF: DataFrame = dataframe.filter($"day" === dayValue)
			
			// 指标计算
			
			// 封装Row中
			val aggRow = Row.fromSeq(
				dayRow.toSeq
			)
			rowList += aggRow
		}
		
		// step2. 转换列表ListBuffer为DataFrame
		
		// step3. 返回计算指标结果数据集DataFrame
		
		null
	}
	
	def main(args: Array[String]): Unit = {
		// 调用模板方法，传递参数
		execute(
			this.getClass, //
			OfflineTableDefine.WAREHOUSE_DETAIL, //
			OfflineTableDefine.WAREHOUSE_SUMMARY, //
			isLoadFullData = Configuration.IS_FIRST_RUNNABLE //
		)
	}
}

```



## 13-[掌握]-仓库主题之指标计算【process 方法】

> ​		前面已经将指标计算步骤逻辑捋清楚，接下来编写代码，与前面讲解运单主题报表计算基本类似。

```scala
package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common.{Configuration, OfflineTableDefine}
import cn.itcast.logistics.offline.AbstractOfflineApp
import cn.itcast.logistics.offline.dws.WayBillDWS.execute
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
 * 仓库主题报表开发：
 *      从DWD层加载仓库宽表数据，按照具体指标计算，使用不同维度进行分组统计。
 */
object WarehouseDWS extends AbstractOfflineApp {
	/**
	 * 对数据集DataFrame按照业务需求编码，由子类复杂实现
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		// 此时dataframe为 宽表数据，其中有字段day
		
		// 获取SparkSession实例对象
		val session = dataframe.sparkSession
		import session.implicits._
		
		// step1. 按天day统计业务数据指标
		// TODO: 各个主题指标计算时，无论是全量数据还是增量数据，都是按照day天进行划分计算的
		val rowList: ListBuffer[Row] = new ListBuffer[Row]()
		dataframe.select($"day").distinct().collect().foreach{dayRow =>
			// 获取每天day值
			val dayValue: String = dayRow.getString(0)
			
			// 获取每天业务数据
			val warehouseDetailDF: DataFrame = dataframe.filter($"day" === dayValue)
			warehouseDetailDF.persist(StorageLevel.MEMORY_AND_DISK)
			
			// 指标计算
			// 指标一：各仓库发车次数，最大、最小和平均
			val whSwTotalDF: DataFrame  = warehouseDetailDF.groupBy($"sw_id").count()
			val whSwTotalAggDF: DataFrame = whSwTotalDF.agg(
				max($"count").as("maxWsTotal"), //
				min($"count").as("minWsTotal"), //
				round(avg($"count").as("avgWsTotal"), 0) //
			)
			
			// 指标二：各网点发车次数，最大、最小和平均
			val whDotTotalDF: DataFrame  = warehouseDetailDF.groupBy($"dot_id").count()
			val whDotTotalAggDF: DataFrame = whDotTotalDF.agg(
				max($"count").as("maxDotTotal"), //
				min($"count").as("minDotTotal"), //
				round(avg($"count").as("avgDotTotal"), 0) //
			)
			
			// 指标三：各线路发车次数，最大、最小和平均
			val whRouteTotalDF: DataFrame  = warehouseDetailDF.groupBy($"route_id").count()
			val whRouteTotalAggDF: DataFrame = whRouteTotalDF.agg(
				max($"count").as("maxRouteTotal"), //
				min($"count").as("minRouteTotal"), //
				round(avg($"count").as("avgRouteTotal"), 0) //
			)
			
			// 指标四：各类型客户发车次数，最大、最小和平均
			val whCtypeTotalDF: DataFrame  = warehouseDetailDF.groupBy($"ctype").count()
			val whCtypeTotalAggDF: DataFrame = whCtypeTotalDF.agg(
				max($"count").as("maxCtypeTotal"), //
				min($"count").as("minCtypeTotal"), //
				round(avg($"count").as("avgCtypeTotal"), 0) //
			)
			
			// 指标五：各类型包裹发车次数，最大、最小和平均
			val whPackageTotalDF: DataFrame  = warehouseDetailDF.groupBy($"package_id").count()
			val whPackageTotalAggDF: DataFrame = whPackageTotalDF.agg(
				max($"count").as("maxPackageTotal"), //
				min($"count").as("minPackageTotal"), //
				round(avg($"count").as("avgPackageTotal"), 0) //
			)
			
			// 指标六：各区域发车次数，最大、最小和平均
			val whAreaTotalDF: DataFrame  = warehouseDetailDF.groupBy($"area_id").count()
			val whAreaTotalAggDF: DataFrame = whAreaTotalDF.agg(
				max($"count").as("maxAreaTotal"), //
				min($"count").as("minAreaTotal"), //
				round(avg($"count").as("avgAreaTotal"), 0) //
			)
			
			// 指标七：各公司发车次数，最大、最小和平均
			val whCompanyTotalDF: DataFrame  = warehouseDetailDF.groupBy($"company_id").count()
			val whCompanyTotalAggDF: DataFrame = whCompanyTotalDF.agg(
				max($"count").as("maxCompanyTotal"), //
				min($"count").as("minCompanyTotal"), //
				round(avg($"count").as("avgCompanyTotal"), 0) //
			)
			
			// 数据不再使用，释放缓存
			warehouseDetailDF.unpersist()
			
			// 封装Row中
			val aggRow = Row.fromSeq(
				dayRow.toSeq ++
					whSwTotalAggDF.first().toSeq ++
					whDotTotalAggDF.first().toSeq ++
					whRouteTotalAggDF.first().toSeq ++
					whCtypeTotalAggDF.first().toSeq ++
					whPackageTotalAggDF.first().toSeq ++
					whAreaTotalAggDF.first().toSeq ++
					whCompanyTotalAggDF.first().toSeq
			)
			rowList += aggRow
		}
		
		// step2. 转换列表ListBuffer为DataFrame
		// 第一步、将列表转换为RDD
		val rowsRDD: RDD[Row] = spark.sparkContext.parallelize(rowList.toList) // 将可变集合对象转换为不可变的
		// 第二步、自定义Schema信息
		val aggSchema: StructType = new StructType()
			.add("id", StringType, nullable = false) // 针对每天数据进行聚合得到一个结果，设置day为结果表中id
			.add("maxSwTotal", LongType, nullable = true)
			.add("minSwTotal", LongType, nullable = true)
			.add("avgSwTotal", DoubleType, nullable = true)
			.add("maxDotTotal", LongType, nullable = true)
			.add("minDotTotal", LongType, nullable = true)
			.add("avgDotTotal", DoubleType, nullable = true)
			.add("maxRouteTotal", LongType, nullable = true)
			.add("minRouteTotal", LongType, nullable = true)
			.add("avgRouteTotal", DoubleType, nullable = true)
			.add("maxCtypeTotal", LongType, nullable = true)
			.add("minCtypeTotal", LongType, nullable = true)
			.add("avgCtypeTotal", DoubleType, nullable = true)
			.add("maxPackageTotal", LongType, nullable = true)
			.add("minPackageTotal", LongType, nullable = true)
			.add("avgPackageTotal", DoubleType, nullable = true)
			.add("maxAreaTotal", LongType, nullable = true)
			.add("minAreaTotal", LongType, nullable = true)
			.add("avgAreaTotal", DoubleType, nullable = true)
			.add("maxCompanyTotal", LongType, nullable = true)
			.add("minCompanyTotal", LongType, nullable = true)
			.add("avgCompanyTotal", DoubleType, nullable = true)
		// 第三步、调用SparkSession中createDataFrame方法，组合RowsRDD和Schema为DataFrame
		val aggDF: DataFrame = spark.createDataFrame(rowsRDD, aggSchema)
		
		// step3. 返回计算指标结果数据集DataFrame
		aggDF
	}
	
	def main(args: Array[String]): Unit = {
		// 调用模板方法，传递参数
		execute(
			this.getClass, //
			OfflineTableDefine.WAREHOUSE_DETAIL, //
			OfflineTableDefine.WAREHOUSE_SUMMARY, //
			isLoadFullData = Configuration.IS_FIRST_RUNNABLE //
		)
	}
}

```

> 程序编写完成，运行查看Kudu表结果：

![1614054126385](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614054126385.png)



## 14-[理解]-Kudu 原理及优化之数据存储模型

> ​		针对整个物流项目来说，业务数据主要存储在Kudu表中，类似HBase数据存数据，接下来看一下Kudu存储引擎如何管理存储数据，了解存储原理和数据模型，通过类比HBase存储数据类型。
>
> 文档：https://kudu.apache.org/overview.html、https://kudu.apache.org/docs/

- 1）、表与schema

![1614062301840](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614062301840.png)

> ​		从用户角度来看，Kudu是一种存储结构化数据表的存储系统。在一个Kudu集群中可以定义任意数量的table，每个table都需要预先定义好schema。每个table的列数是确定的，每一列都需要有名字和类型，每个表中可以把其中一列或多列定义为主键。



- 2）、Kudu 底层数据模型

> 首先回顾HBase数据库如何存储数据？？？
>
> ```
> 面试题一：HBase 中表是如何设计的？？？或  RowKey是如何设计？？？？？
> 
> 面试题二：Client从HBase表读写数据流程怎么样的？？？？
> ```
>
> 

![1614062789027](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614062789027.png)

> Kudu 存储引擎底层数据存储：

![1614062859258](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614062859258.png)

> 有两个在内存中处理的数据集：MemRowSet和DelteMem
>
> - 1）、MemRowSet：存储新增的数据，对该内存数据集中还未flush的数据的更新；
> - 2）、DeltaMem：对已flush到磁盘内的数据的更新；

![1614063249746](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614063249746.png)

> 在KUDU中，把DiskRowSet分为了两部分：base data（基础数据）和delta stores（变更数据）

![1614063339983](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614063339983.png)



## 15-[理解]-Kudu 原理及优化之数据读写原理

> Kudu的工作模式如下图：

![1614063547994](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614063547994.png)

> 1、每个kudu table按照hash或range分区为多个tablet；
> 2、每个tablet中包含一个MemRowSet以及多个DiskRowSet；
> 3、每个DiskRowSet包含BaseData以及DeltaStores；
> 4、DeltaStores由多个DeltaFile和一个DeltaMemStore组成；
> 5、insert请求的新增数据以及对MemRowSet中数据的update操作（新增的数据还没有来得及触
> 发compaction操作再次进行更新操作的新数据） 会先进入到MemRowSet；
> 6、当触发flush条件时将新增数据真正的持久化到磁盘的DiskRowSet内；
> 7、对老数据的update和delete操作是提交到内存中的DeltaMemStore；
> 8、当触发flush条件时会将更新和删除操作持久化到磁盘DIskRowSet中的DeltaFile内，此时老数据
> 还在BaseData内（逻辑删除），新数据已在DeltaFile内；
> 9、当触发compaction条件时，将DeltaFile和BaseData进行合并，DiskRowSet进行合并，此时老
> 数据才真正的从磁盘内消失掉（物理删除），只留下更新后的数据记录



> 当Client从Kudu表读取数据时流程：
>
> - 1、客户端向Kudu Master请求tablet所在位置
> - 2、Kudu Master返回tablet所在位置
> - 3、为了优化读取和写入，客户端将元数据进行缓存
> - 4、根据主键范围过滤目标tablet，请求Tablet Follower
> - 5、根据主键过滤scan范围，定位DataRowSets
> - 6、加载BaseData，并与DeltaStores合并，得到老数据的最新结果
> - 7、拼接第6步骤得到的老数据与MemRowSet数据 得到所需数据
> - 8、将数据返回给客户端
>
> [从Kudu表读取数据时，首先加载对应RowSet中DiskRowSet（BaseData和DelteFile）数据，然后在读取MemRowSet数据，进行合并，最终将数据返回给client客户端。]()

![1614063739519](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614063739519.png)



> 向Kudu表写入数据流程：
>
> - 1.客户端向Kudu Master请求tablet所在位置；
> - 2.Kudu Master返回tablet所在位置；
> - 3.为了优化读取和写入，客户端将元数据进行缓存；
> - 4.根据分区策略，路由到对应Tablet，请求Tablet Leader；
> - 5.根据RowSet记录的主键范围过滤掉不包含新增数据主键的RowSet；
> - 6.根据RowSet 布隆过滤器再进行一次过滤，过滤掉不包含新数据主键的RowSet；
> - 7.查询RowSet中的B树索引判断是否命中新数据主键，若命中则报错主键冲突，否则新数据写入MemRowSet；
> - 8.返回响应给客户端
>
> [插入Insert数据时，需要经过三次判断主键是否存在，当不存在才插入数据，将数据写入MemRowSet内存中：第一次、主键范围过滤；第二次、布隆过滤器过滤；第三次、B树索引过滤]()

![1614064091516](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614064091516.png)



> Kudu表数据更新与删除流程：
>
> - 1.客户端向Kudu Master请求tablet所在位置
> - 2.Kudu Master返回tablet所在位置
> - 3.为了优化读取和写入，客户端将元数据进行缓存
> - 4.根据分区策略，路由到对应Tablet，请求Tablet Leader
> - 5.根据RowSet记录的主键范围过滤掉不包含修改的数据主键的RowSet
> - 6.根据RowSet 布隆过滤器再进行一次过滤，过滤掉不包含修改的数据主键的RowSet
> - 7.查询RowSet中的B树索引判断是否命中修改的数据主键，若命中则修改至DeltaStores，否则报错数据不存在
> - 8.返回响应给客户端

![1614064347946](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614064347946.png)

## 16-[理解]-Kudu 原理及优化之基本优化设置

> ​		Kudu存储引擎是介于HDFS和HBase中间产品，属于折中产品，在实际项目中使用时，需要一些注意事项。

- 1）、Kudu 关键配置：TabletServer内存配置

![1614064637141](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614064637141.png)

![1614064641161](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614064641161.png)



- 2）、Kudu 使用限制

![1614064705162](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614064705162.png)



- 3）、字段

![1614064783766](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614064783766.png)



- 4）、表

![1614064826027](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614064826027.png)



- 5）、其他限制

![1614064867906](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614064867906.png)



- 6）、分区限制

![1614064929186](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614064929186.png)



- 7）、扩展建议和限制（记录一下）

![1614064974179](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614064974179.png)

![1614065018053](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614065018053.png)



- 8）、守护进程

![1614065109850](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614065109850.png)

> Kudu集群对时间同步非常严格



- 9）、集群管理限制

![1614065187021](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614065187021.png)



- 10）、Kudu表的数据如何备份

> 编写Spark程序或者ImpalaSQL将数据导出，存储到HDFS文件系统中



- 11）、Impala 集成限制

![1614065454361](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614065454361.png)



- 12、Spark 集成限制

![1614065535377](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614065535377.png)



## 17-[理解]-工作流调度之调度需求及框架

> ​		在大数据中离线报表分析，通过流程图如下所示：（传统大数据分析技术框架）
>
> [一个完整的数据分析系统通常都是由多个前后依赖的模块组合构成的：数据采集、数据预处理、数据分析、数据展示等。各个模块单元之间存在时间先后==依赖关系==，且存在着==周期性重复==。]()

![1614066459254](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614066459254.png)

> 针对物流项目来说，数据采集实时增量采集（可以不用管理执行），但是离线报表来说，需要管理执行。
>
> - 1）、每日报表：每天分析前一天业务数据，计算报表
>   - 定时调度执行，比如每天凌晨1:00开始运行程序，进行指标计算
> - 2）、每个主题报表程序运行时，先执行DWD程序（拉宽操作），再执行DWS程序（指标计算）
>   - 依赖调度执行，DWS程序执行，必须依赖于DWD程序执行成功

![1613963167534](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613963167534.png)

> ​		所以，需要选择工作流任务调度框架，帮助进行各个主题报表执行调度，既能够进行定时调度执行，又能够进行依赖调度执行：
>
> - 1）、`Azkaban`，由领英Linkedlin公司开源工作流调度框架，目前在企业使用较多，主要版本：2.x和3.x
> - 2）、`Oozie`，由Cloudera公司开源工作流调度框架，功能非常全面，就是针对大数据分析引擎开发调度执行框架，但是上手使用比较麻烦，通常与Hue集成使用，进行可视化配置
>   - [如果公司使用CDH大数据框架，基本上都使用Oozie和Hue集成调度工作流任务]()
>   - Oozie如何调度执行Spark程序：https://www.bilibili.com/video/BV1uT4y1F7ap?p=151
> - 3）、`AirFlow`，最近几年比较火工作流调度框架，http://airflow.apache.org/

![1614067195673](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614067195673.png)



## 18-[理解]-工作流调度之Azkaban 架构部署

> ​		Azkaban是由linkedin（领英）公司推出的一个批量工作流任务调度器，用于在一个工作流内以一个特定的顺序运行一组工作和流程。
>
> 网址：https://azkaban.github.io/

![Azkaban](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/app.png)

> ​		Azkaban使用==job配置文件==建立`任务之间的依赖关系`，并提供一个易于使用的==web用户界面==维护和跟踪你的工作流。

![1614067736418](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614067736418.png)

> Azkaban 原理架构：分为三个部分
>
> - 1）、Azkaban Web Server（WEB 服务），提供WEB 界面，方便用户创建工作流WorkFlow和监控执行
> - 2）、Azkaban Executor Server（执行服务），调度执行工作流
> - 3）、MySQL（数据库），存储创建工作流及执行信息

![1614067766952](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614067766952.png)

> Azkaban三种部署模式：区别与Web Server和Executor Server运行机器
>
> - 1）、`solo server mode`（单机模式）
>   - WebServer和ExecutorServer合为一体，就是一个Server，既可以对外提供WEB 服务，又可执行工作流
>   - WebServer与EexcutorServer运行在同一JVM 进程中

![1614068026707](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614068026707.png)

> - 2）、`two-server mode`（服务模式）
>   - Web Server 和 Executor Server各自运行在独立JVM 进程中，推荐方式

![1614068128106](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614068128106.png)



> - 3）、**multiple-executor mode**（高可用HA模式），建议在生产环境使用
>   - 在Azkaban中，Executor为执行服务，用于调度执行工作流，如果服务GG，任何工作流都不能调度执行，为了考虑高可用性能，可以运行多个ExecutorServer，集群高可用。
>   - 从Azkaban 3.x开始支持的

[此处物流项目中，采集最简易部署方式安装：单机模式，将WebServer和ExecutorServer运行在同一个JVM进程。]()

【第4章 客快物流大数据之大数据服务器环境配置】中【3.8节内容】。

![1614068370667](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614068370667.png)

## 19-[掌握]-工作流调度之Azkaban 快速入门

> ​		在大数据服务器【node2.itcast.cn】中已经安装部署完成，大家可以参考文档。
>
> - 1）、启动 Azkaban
>   - 首先启动node2.itcast.cn虚拟机，一切配置OK

![1614068496382](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614068496382.png)

```ini
[root@node2 ~]# cd /export/services/azkaban
[root@node2 azkaban]# ll
total 360
drwxr-xr-x. 3 root root     67 Apr  3  2020 bin
drwxr-xr-x. 2 root root     82 May 28  2020 conf
-rw-r--r--. 1 root root      6 Apr  3  2020 currentpid
drwxr-xr-x. 3 root root     31 Apr  3  2020 examples
drwxr-sr-x. 2 root root      6 Apr  3  2020 executions
-rw-r--r--. 1 root root      5 Apr  3  2020 executor.port
drwxr-xr-x. 2 root root   4096 Apr  3  2020 lib
drwxr-xr-x. 2 root root     35 Apr  3  2020 local
drwxr-xr-x. 3 root root     22 Apr  3  2020 plugins
drwxr-xr-x. 5 root root     39 Apr  3  2020 projects
-rw-r--r--. 1 root root 350840 Apr  8  2020 soloServerLog__2020-04-03+19:03:49.out
drwxr-xr-x. 2 root root   4096 Apr  3  2020 sql
drwxr-xr-x. 2 root root      6 Apr  3  2020 temp
drwxr-xr-x. 6 root root     73 Apr  3  2020 web
[root@node2 azkaban]# 
[root@node2 azkaban]# ls bin/
internal  shutdown-solo.sh  start-solo.sh
[root@node2 azkaban]# bin/start-solo.sh 
```

> 使用【JPS】查看进程时，不存在，读取日志信息：`[root@node2 azkaban]# more soloServerLog__2021-02-23+16\:22\:05.out` 

![1614068660630](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614068660630.png)

> 从日志信息可以看到：启动Azkaban服务时，连接MySQL数据库时，找不到【azkaban】数据库。

```ini
[root@node2 ~]# mysql -uroot -pAbcd1234.
mysql: [Warning] Using a password on the command line interface can be insecure.
Welcome to the MySQL monitor.  Commands end with ; or \g.
Your MySQL connection id is 11
Server version: 5.7.29 MySQL Community Server (GPL)

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
| am                 |
| amon               |
| hive               |
| hue                |
| metastore          |
| mysql              |
| nav                |
| oozie              |
| performance_schema |
| rman               |
| scm                |
| sentry             |
| sys                |
+--------------------+
14 rows in set (0.37 sec)

mysql> create database azkaban default character set utf8 collate utf8_general_ci;
Query OK, 1 row affected (0.11 sec)
```

> 创建完成数据库以后，再次重启Azkaban服务

```ini
[root@node2 ~]# cd /export/services/azkaban
[root@node2 azkaban]# bin/start-solo.sh 
[root@node2 azkaban]# 
[root@node2 azkaban]# jps
2208 -- process information unavailable
12202 AzkabanSingleServer
12218 Jps
1226 Main
2206 -- process information unavailable
```

> 登录提供WEB UI界面：http://node2.itcast.cn:8081/，输入用户名和秘密：`azkaban/azkaban`

![1614068937937](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614068937937.png)

> 在Azkaban中将工作流（workflow)称呼为project工程。
>
> [入门案例：创建Project，调度执行Shell命令【echo “hello azkaban!!”】]()

![1614070152850](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614070152850.png)

> 通过azkaban的web管理平台创建project并上传job压缩包

![1614070175225](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614070175225.png)

> 上传完成xx.job文件以后，可以执行Flow，查看运行结果截图：

![1614070886827](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614070886827.png)

> 注意：编写JOB属性文件时，不能有多余空格和换行，尤其是最后一行不能为空。



## 20-[掌握]-工作流调度之Azkaban 使用实战

> ​		接下来，使用Azkaban进行依赖调度执行和定时调度执行。

- 1）、依赖调度执行

> 创建两个文件one.job two.job,内容如下，打包成zip包。

![1614071024862](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614071024862.png)

> 创建Project，如下图所示：

![1614071040402](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614071040402.png)

> 上传压缩包，执行即可

![1614071054611](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614071054611.png)

![1614071059422](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614071059422.png)

> 执行成功页面：

![1614071257967](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614071257967.png)

- 2）、定义调度

> 在Azkaban中对Project中WorkFlow指定定时调度规则即可。[以定时执行HelloProject为例，每1分钟执行一次]()

![1614071371772](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614071371772.png)

> 选择【Scheduler】设置定时规则

![1614071405154](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614071405154.png)

> 设置1分钟执行一次

![1614071571002](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614071571002.png)

> 查看调度执行页面

![1614071681353](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614071681353.png)









