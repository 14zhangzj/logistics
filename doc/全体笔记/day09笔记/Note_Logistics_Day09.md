---
stypora-copy-images-to: img
typora-root-url: ./
---



# Logistics_Day09：主题及报表开发与ES全文检索



## 01-[复习]-上次课程内容回顾 

> 上次课主要讲解两个方面内容：离线报表分析（2个主题：运单主题和仓库主题）和调度框架Azkaban。
>
> - 1）、离线报表分析：2个主题
>
>   - 第一个主题：运单主题（业务数据：tbl_way_bill）
>
>     - 含义：运单是运输合同的证明，是承运人已经接收货物的收据。
>     - 按照数仓分层结构管理数据，进行开发：DWD层宽表数据和DWS层指标计算
>
>     ![1614127358881](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614127358881.png)
>
>   - 公共接口重构，基于模板方法设计模式，类继承设计模式
>     - 基本方法：具体方法和抽象方法（被子类实现）
>     - 模板方法：决定基本方法执行顺序
>     - 基类：`AbstractOfflineApp`
>   - 第二主题：仓库主题（业务数据：`tbl_transport_record`）
>     
>     - 与运单主题报表分析类似，按照数仓分层进行管理数据和开发应用程序。

![1614127315872](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614127315872.png)

> - 2）、Kudu 存储引擎底层原理和优化
>
>   - Kudu 底层数据模型：Table -> Tablet -> MemRowSet和DiskRowSet（base data和delta stores）
>
>   ![1614127626443](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614127626443.png)
>
>   - Kudu 读写原理
>     - 读取数据流程
>     - 写入数据流程
>     - 更新数据和删除数据流程，基本类似
>   - 使用Kudu 优化与限制
>
> - 3）、调度框架：`Azkaban`，开源工作流调度引擎
>
>   - 提供基于JOB属性文件配置，并且WEB UI界面（创建Project、执行工作流和查看运行状态）
>   - 实现功能：定时调度和依赖调度，主要执行Shell Command命令
>
>   ![1614127927085](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614127927085.png)



## 02-[了解]-第9天：课程内容提纲 

> 主要分为2个方面内容：离线报表分析（2个主题：车辆主题和用户主题）和实时业务数据存储Es索引。
>
> - 1）、主题报表开发
>   - 车辆主题报表开发，分为网点车辆报表开发和仓库车辆报表开发
>   - 用户主题报表开发，基于用户相关业务数据（快递单数据）进行用户画像构建，打标签。
> - 2）、基于Azkaban开发调度程序，运行离线报表程序
>   - 如何编写Shell 脚本，传递不同参数，执行对应主题程序，计算报表
> - 3）、基于Impala进行分析业务数据
>   - 针对用户数据进行标签开发，主要感知Impala分析数据SQL编写
> - 4）、实时ETL数据存储Elasticsearch索引
>   - 方便语句快递单号查询每个快递物流信息
>   - Elasticsearch索引中存储业务数据，最多3个月
>   - 涉及知识点：Spark集成Es，Es自身提供与Spark整合类库，直接使用即可。

![1614128824883](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614128824883.png)



## 03-[理解]-车辆主题之数据调研及业务分析

> 车辆主题主要是统计各个网点、区域、公司的发车情况，反映了区域或者公司的吞吐量及运营状况。

![1614129098638](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614129098638.png)

> 车辆主题报表分析分为：`仓库`车辆使用数据统计和`网点`车辆使用数据统计。

![1614129214114](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614129214114.png)

> 事实表与维度表关联关系：

![1614129350351](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614129350351.png)

> 针对车辆主题进行报表分析开发，需要分别对网点车辆和仓库车辆进行报表分析：
>
> - 1）、网点车辆报表开发：DWD层宽表开发和DWS层指标计算
> - 2）、仓库车辆报表开发：DWD层宽表开发和DWS层指标计算



## 04-[掌握]-网点车辆之数据拉宽开发

> ​			针对网点车辆数据进行报表开发，首先将网点车辆数据信息与相关维度表进行JOIN关联，获取宽表数据，将其存储到Kudu表中（DWD层数据明细表）。

![1614129623393](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614129623393.png)

> 如果编写SQL语句，如下所示：

![1614129663258](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614129663258.png)

> 创建对象：`TransportToolDotDWD`，继承公共接口：`AbstractOfflineApp`，代码如下：

```scala
package cn.itcast.logistics.offline.dwd

import cn.itcast.logistics.common.{CodeTypeMapping, OfflineTableDefine, TableMapping}
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 * 车辆主题：
 *      网点车辆拉宽开发，将网点车辆事实表与相关的维度表进行JOIN关联拉宽，存储Kudu的DWD层详细表。
 */
object TransportToolDotDWD extends AbstractOfflineApp{
	/**
	 * 对数据集DataFrame按照业务需求编码，数据拉宽操作
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		import dataframe.sparkSession.implicits._
		
		// step1. 加载事实表相关维度表数据
		// 加载仓库车辆表数据
		val ttDF: DataFrame = loadKuduSource(TableMapping.TRANSPORT_TOOL, isLoadFullData = true)
		// 加载网点表的数据
		val dotDF: DataFrame = loadKuduSource(TableMapping.DOT, isLoadFullData = true)
		// 加载公司网点关联表的数据
		val companyDotDF: DataFrame = loadKuduSource(TableMapping.COMPANY_DOT_MAP, isLoadFullData = true)
		// 加载公司表的数据
		val companyDF: DataFrame = loadKuduSource(TableMapping.COMPANY, isLoadFullData = true)
		// 加载物流码表数据
		val codesDF: DataFrame = loadKuduSource(TableMapping.CODES, isLoadFullData = true)
		// 获取运输工具类型
		val transportTypeDF: DataFrame = codesDF
			.where($"type" === CodeTypeMapping.TRANSPORT_TYPE)
			.select($"code".as("ttType"), $"codeDesc".as("ttTypeName"))
		// 获取运输工具状态
		val transportStatusDF: DataFrame = codesDF
			.where($"type" === CodeTypeMapping.TRANSPORT_STATUS)
			.select($"code".as("ttStatus"), $"codeDesc".as("ttStateName"))
		
		// step2. 将事实表与维度表数据进行关联（leftJoin），并且选取字段和添加日期字段
		val joinType: String = "left_outer"
		val ttDotDF: DataFrame = dataframe
		val ttDotDetailDF = ttDotDF
			// 网点车辆表关联车辆表
			.join(ttDF, ttDotDF.col("transportToolId") === ttDF.col("id"), joinType)
			// 车辆表类型关联字典表类型
			.join(transportTypeDF, transportTypeDF("ttType") === ttDF("type"), joinType)
			// 车辆表状态管理字典表状态
			.join(transportStatusDF, transportStatusDF("ttStatus") === ttDF("state"), joinType)
			// 网点车辆表关联网点
			.join(dotDF, dotDF.col("id") === ttDotDF.col("dotId"), joinType)
			// 网点车辆管连网点公司关联表
			.join(companyDotDF, ttDotDF.col("dotId") === companyDotDF.col("dotId"), joinType)
			// 网点车辆表关联公司表
			.join(companyDF, companyDotDF.col("companyId") === companyDF.col("id"), joinType)
			// 虚拟列,可以根据这个日期列作为分区字段，可以保证同一天的数据保存在同一个分区中
			.withColumn("day", date_format(ttDotDF("cdt"), "yyyyMMdd"))
			.select(
				ttDF("id"), //车辆表id
				ttDF("brand"), //车辆表brand
				ttDF("model"), //车辆表model
				ttDF("type").cast(IntegerType), //车辆表type
				transportTypeDF("ttTypeName").as("type_name"), // 车辆表type对应字典表车辆类型的具体描述
				ttDF("givenLoad").cast(IntegerType).as("given_load"), //车辆表given_load
				ttDF("loadCnUnit").as("load_cn_unit"), //车辆表load_cn_unit
				ttDF("loadEnUnit").as("load_en_unit"), //车辆表load_en_unit
				ttDF("buyDt").as("buy_dt"), //车辆表buy_dt
				ttDF("licensePlate").as("license_plate"), //车辆表license_plate
				ttDF("state").cast(IntegerType), //车辆表state
				transportStatusDF("ttStateName").as("state_name"), // 车辆表state对应字典表类型的具体描述
				ttDF("cdt"), //车辆表cdt
				ttDF("udt"), //车辆表udt
				ttDF("remark"), //车辆表remark
				dotDF("id").as("dot_id"), //网点表dot_id
				dotDF("dotNumber").as("dot_number"), //网点表dot_number
				dotDF("dotName").as("dot_name"), //网点表dot_name
				dotDF("dotAddr").as("dot_addr"), //网点表dot_addr
				dotDF("dotGisAddr").as("dot_gis_addr"), //网点表dot_gis_addr
				dotDF("dotTel").as("dot_tel"), //网点表dot_tel
				dotDF("manageAreaId").as("manage_area_id"), //网点表manage_area_id
				dotDF("manageAreaGis").as("manage_area_gis"), //网点表manage_area_gis
				companyDF("id").alias("company_id"), //公司表id
				companyDF("companyName").as("company_name"), //公司表company_name
				companyDF("cityId").as("city_id"), //公司表city_id
				companyDF("companyNumber").as("company_number"), //公司表company_number
				companyDF("companyAddr").as("company_addr"), //公司表company_addr
				companyDF("companyAddrGis").as("company_addr_gis"), //公司表company_addr_gis
				companyDF("companyTel").as("company_tel"), //公司表company_tel
				companyDF("isSubCompany").as("is_sub_company"), //公司表is_sub_company
				$"day"
			)
		
		
		// step3. 返回宽表数据
		ttDotDetailDF
	}
	
	def main(args: Array[String]): Unit = {
		execute(
			this.getClass, //
			TableMapping.DOT_TRANSPORT_TOOL, //
			OfflineTableDefine.DOT_TRANSPORT_TOOL_DETAIL, //
			isLoadFullData = true //
		)
	}
}

```

> 运行DWD层数据拉宽程序。

![1614130368262](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614130368262.png)



## 05-[掌握]-网点车辆之指标计算开发

> ​		接下来，对网点车辆拉宽后宽表数据进行指标计算，按照各个维度（网点、区域和公司）进行统计（总的发车次数、最大、最小和平均发车次数）。
>
> [创建对象：TransportToolDotDWS，继承公共接口：AbstractOfflineApp，实现其中process方法]()

```scala
package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common.OfflineTableDefine
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}

import scala.collection.mutable.ListBuffer

/**
 * 网点车辆相关指标聚合统计：
 *      从Kudu表加载网点车辆详细宽表数据，按照不同维度进行分组，计算相关指标。
 */
object TransportToolDotDWS extends AbstractOfflineApp{
	/**
	 * 对数据集DataFrame按照业务需求编码，对宽表数据进行指标计算
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		// 导入隐式转换
		val session = dataframe.sparkSession
		import session.implicits._
		
		// step1. 按照day每天划分业务数据，进行指标计算，封装到Row对象中
		val rowList: ListBuffer[Row] = new ListBuffer[Row]()
		dataframe.select($"day").distinct().collect().foreach{dayRow =>
			// 获取每天值
			val dayValue: String = dayRow.getAs[String]("day")
			
			// 过滤获取每天数据
			val ttDetailDF: DataFrame = dataframe.filter($"day" === dayValue)
			
			// 指标计算
			// 指标一：网点发车次数及最大、最小和平均
			val ttDotTotalDF: DataFrame = ttDetailDF.groupBy($"dot_id").count()
			val ttDotTotalAggDF: DataFrame = ttDotTotalDF.agg(
				sum($"count").as("sumDotTotal"), // 使用sum函数，计算所有网点车次数之和
				max($"count").as("maxDotTotal"), //
				min($"count").as("minDotTotal"), //
				round(avg($"count"), 0).as("avgDotTotal") //
			)
			// 指标二：城市发车次数及最大、最小和平均
			val ttCityTotalDF: DataFrame = ttDetailDF.groupBy($"city_id").count()
			val ttCityTotalAggDF: DataFrame = ttCityTotalDF.agg(
				sum($"count").as("sumCityTotal"),  //
				max($"count").as("maxCityTotal"),  //
				min($"count").as("minCityTotal"), //
				round(avg($"count"), 0).as("avgCityTotal") //
			)
			// 指标三：公司发车次数及最大、最小和平均
			val ttCompanyTotalDF: DataFrame = ttDetailDF.groupBy($"company_id").count()
			val ttCompanyTotalAggDF: DataFrame = ttCompanyTotalDF.agg(
				sum($"count").as("sumCompanyTotal"),  //
				max($"count").as("maxCompanyTotal"),  //
				min($"count").as("minCompanyTotal"), //
				round(avg($"count"), 0).as("avgCompanyTotal") //
			)
			
			// 需要将计算所有指标结果提取出来，并且组合到Row对象中
			val aggRow: Row = Row.fromSeq(
				dayRow.toSeq ++ //
					ttDotTotalAggDF.first().toSeq ++  //
					ttCityTotalAggDF.first().toSeq ++  //
					ttCompanyTotalAggDF.first().toSeq  //
			)
			// 将每天聚合计算结果加入列表中
			rowList += aggRow
		}
		
		// step2. 将指标结果存储到DataFrame
		// 第一步、将列表转换为RDD
		val rowsRDD: RDD[Row] = session.sparkContext.parallelize(rowList.toList) // 将可变集合对象转换为不可变的
		// 第二步、自定义Schema信息
		val aggSchema: StructType = new StructType()
			.add("id", StringType, nullable = false) // 针对每天数据进行聚合得到一个结果，设置day为结果表中id
			.add("sumDotTotal", LongType, nullable = true)
			.add("maxDotTotal", LongType, nullable = true)
			.add("minDotTotal", LongType, nullable = true)
			.add("avgDotTotal", DoubleType, nullable = true)
			.add("sumCityTotal", LongType, nullable = true)
			.add("maxCityTotal", LongType, nullable = true)
			.add("minCityTotal", LongType, nullable = true)
			.add("avgCityTotal", DoubleType, nullable = true)
			.add("sumCompanyTotal", LongType, nullable = true)
			.add("maxCompanyTotal", LongType, nullable = true)
			.add("minCompanyTotal", LongType, nullable = true)
			.add("avgCompanyTotal", DoubleType, nullable = true)
		
		// 第三步、调用SparkSession中createDataFrame方法，组合RowsRDD和Schema为DataFrame
		val aggDF: DataFrame = session.createDataFrame(rowsRDD, aggSchema)
		
		// step3. 返回指标计算结果
		aggDF
	}
	
	def main(args: Array[String]): Unit = {
		execute(
			this.getClass, //
			OfflineTableDefine.DOT_TRANSPORT_TOOL_DETAIL, //
			OfflineTableDefine.DOT_TRANSPORT_TOOL_SUMMARY, //
			isLoadFullData = true //
		)
	}
}

```

> 开发完成DWS层指标计算代码，运行执行，截图如下：

![1614132207635](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614132207635.png)



## 06-[掌握]-仓库车辆之数据拉宽开发

> ​	 继续开发仓库车辆指标开发，与前面讲解网点车辆指标开发类似，仅仅业务数据不同而已，指标都是一样
>
> [事实表与维度表数据关联关系图如下所示：]()

![1614133272099](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614133272099.png)

> ​		创建对象：`TransportToolWarehouseDWD`，继承基类：`BasicOfflineApp`，实现其中`process`方法，将数据进行join关联，具体代码如下：

```scala
package cn.itcast.logistics.offline.dwd

import cn.itcast.logistics.common.{CodeTypeMapping, OfflineTableDefine, TableMapping}
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 * 车辆主题：
 *      仓库车辆拉宽开发，将仓库车辆事实表与相关的维度表进行JOIN关联拉宽，存储Kudu的DWD层详细表。
 */
object TransportToolWarehouseDWD extends AbstractOfflineApp{
	/**
	 * 对数据集DataFrame按照业务需求编码，将事实表与维度表进行关联JOIN
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		import dataframe.sparkSession.implicits._
		
		// step1. 加载事实表相关维度表数据
		//加载车辆表数据（事实表）
		val ttDF: DataFrame = loadKuduSource(TableMapping.TRANSPORT_TOOL, isLoadFullData = true)
		//加载公司表的数据
		val companyDF: DataFrame = loadKuduSource(TableMapping.COMPANY, isLoadFullData = true)
		//加载仓库公司关联表
		val companyWareHouseMapDF: DataFrame = loadKuduSource(TableMapping.COMPANY_WAREHOUSE_MAP, isLoadFullData = true)
		//加载仓库表数据
		val wsDF: DataFrame = loadKuduSource(TableMapping.WAREHOUSE, isLoadFullData = true)
		//加载物流码表数据
		val codesDF: DataFrame = loadKuduSource(TableMapping.CODES, isLoadFullData = true)
		//获取运输工具类型
		val transportTypeDF: DataFrame = codesDF
			.where($"type" === CodeTypeMapping.TRANSPORT_TYPE)
			.select($"code".as("ttType"), $"codeDesc".as("ttTypeName"))
		//获取运输工具状态
		val transportStatusDF: DataFrame = codesDF
			.where($"type" === CodeTypeMapping.TRANSPORT_STATUS)
			.select($"code".as("ttStatus"), $"codeDesc".as("ttStateName"))
		
		// step2. 将事实表与维度表数据进行关联（leftJoin），并且选取字段和添加日期字段
		val joinType: String = "left_outer"
		val ttWsDF: DataFrame = dataframe
		val ttWsDetailDF = ttWsDF
			.join(ttDF, ttWsDF.col("transportToolId") === ttDF.col("id"), joinType) //仓库车辆表关联车辆表
			.join(transportTypeDF, transportTypeDF("ttType") === ttDF("type"), joinType) //车辆表类型关联字典表类型
			.join(transportStatusDF, transportStatusDF("ttStatus") === ttDF("state"), joinType) //车辆表状态管理字典表状态
			.join(wsDF, wsDF.col("id") === ttWsDF.col("warehouseId"), joinType) //仓库车辆表关联仓库
			.join(
				companyWareHouseMapDF,
				ttWsDF.col("warehouseId") === companyWareHouseMapDF.col("warehouseId"),
				joinType
			) //仓库车辆管连仓库公司关联表
			.join(
				companyDF,
				companyDF.col("id") === companyWareHouseMapDF.col("companyId"),
				joinType
			)
			.withColumn("day", date_format(ttWsDF("cdt"), "yyyyMMdd"))//虚拟列,可以根据这个日期列作为分区字段，可以保证同一天的数据保存在同一个分区中
			.select(
				ttDF("id"), //车辆表id
				ttDF("brand"), //车辆表brand
				ttDF("model"), //车辆表model
				ttDF("type").cast(IntegerType), //车辆表type
				transportTypeDF("ttTypeName").as("type_name"), // 车辆表type对应字典表车辆类型的具体描述
				ttDF("givenLoad").as("given_load").cast(IntegerType), //车辆表given_load
				ttDF("loadCnUnit").as("load_cn_unit"), //车辆表load_cn_unit
				ttDF("loadEnUnit").as("load_en_unit"), //车辆表load_en_unit
				ttDF("buyDt").as("buy_dt"), //车辆表buy_dt
				ttDF("licensePlate").as("license_plate"), //车辆表license_plate
				ttDF("state").cast(IntegerType), //车辆表state
				transportStatusDF("ttStateName").as("state_name"), // 车辆表type对应字典表车辆类型的具体描述
				ttDF("cdt"), //车辆表cdt
				ttDF("udt"), //车辆表udt
				ttDF("remark"), //车辆表remark
				wsDF("id").as("ws_id"), //仓库表id
				wsDF("name"), //仓库表name
				wsDF("addr"), //仓库表addr
				wsDF("addrGis").as("addr_gis"), //仓库表addr_gis
				wsDF("employeeId").as("employee_id"), //仓库表employee_id
				wsDF("type").as("ws_type").cast(IntegerType), //仓库表type
				wsDF("area"), //仓库表area
				wsDF("isLease").as("is_lease").cast(IntegerType), //仓库表is_lease
				companyDF("id").alias("company_id"), //公司表id
				companyDF("companyName").as("company_name"), //公司表company_name
				companyDF("cityId").as("city_id"), //公司表city_id
				companyDF("companyNumber").as("company_number"), //公司表company_number
				companyDF("companyAddr").as("company_addr"), //公司表company_addr
				companyDF("companyAddrGis").as("company_addr_gis"), //公司表company_addr_gis
				companyDF("companyTel").as("company_tel"), //公司表company_tel
				companyDF("isSubCompany").as("is_sub_company"), //公司表is_sub_company
				$"day"
			)
		
		// step3. 返回宽表数据
		ttWsDetailDF
	}
	
	def main(args: Array[String]): Unit = {
		execute(
			this.getClass, //
			TableMapping.WAREHOUSE_TRANSPORT_TOOL, //
			OfflineTableDefine.WAREHOUSE_TRANSPORT_TOOL_DETAIL, //
			isLoadFullData = true //
		)
	}
}

```

> 编写完成，运行程序，Kudu表数据，如下所示：

![1614133678220](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614133678220.png)



## 07-[掌握]-仓库车辆之指标计算开发

> ​		对仓库车辆宽表数据进行指标计算，基本上与网点车辆宽表数据指标计算逻辑类似。
>
> [创建对象：`TransportToolWarehouseDWS`，继承特质：`AbstractOfflineApp`，实现其中`process`方法]()

```scala
package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common.OfflineTableDefine
import cn.itcast.logistics.offline.AbstractOfflineApp
import cn.itcast.logistics.offline.dws.TransportToolDotDWS.execute
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}

import scala.collection.mutable.ListBuffer

object TransportToolWarehouseDWS extends AbstractOfflineApp{
	/**
	 * 对数据集DataFrame按照业务需求编码，宽表数据指标计算
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		// 导入隐式转换
		val session = dataframe.sparkSession
		import session.implicits._
		
		// dataframe 表示从Kudu表加载宽表数据：tbl_warehouse_transport_tool_detail
		val rowList: ListBuffer[Row] = ListBuffer[Row]()
		dataframe.select($"day").distinct().collect().foreach{dayRow =>
			// 获取具体日期day
			val day: String = dayRow.getAs[String](0)
			// 依据day过滤出对应宽表数据
			val ttWsDetailDF: DataFrame = dataframe.filter($"day" === day)
			
			// 指标计算
			// 指标一：各网点发车次数及最大、最小和平均
			val ttWsTotalDF: DataFrame = ttWsDetailDF.groupBy($"ws_id").count()
			val ttWsTotalAggDF: DataFrame = ttWsTotalDF.agg(
				sum($"count").as("sumDotTotal"),  //
				max($"count").as("maxDotTotal"),  //
				min($"count").as("minDotTotal"), //
				round(avg($"count"), 0).as("avgDotTotal") //
			)
			// 指标二：各城市发车次数及最大、最小和平均
			val ttCityTotalDF: DataFrame = ttWsDetailDF.groupBy($"city_id").count()
			val ttCityTotalAggDF: DataFrame = ttCityTotalDF.agg(
				sum($"count").as("sumCityTotal"),  //
				max($"count").as("maxCityTotal"),  //
				min($"count").as("minCityTotal"), //
				round(avg($"count"), 0).as("avgCityTotal") //
			)
			// 指标三：各公司发车次数及最大、最小和平均
			val ttCompanyTotalDF: DataFrame = ttWsDetailDF.groupBy($"company_id").count()
			val ttCompanyTotalAggDF: DataFrame = ttCompanyTotalDF.agg(
				sum($"count").as("sumCompanyTotal"),  //
				max($"count").as("maxCompanyTotal"),  //
				min($"count").as("minCompanyTotal"), //
				round(avg($"count"), 0).as("avgCompanyTotal") //
			)
			
			// TODO： 需要将计算所有指标结果提取出来，并且组合到Row对象中
			val aggRow: Row = Row.fromSeq(
				dayRow.toSeq ++ //
					ttWsTotalAggDF.first().toSeq ++  //
					ttCityTotalAggDF.first().toSeq ++  //
					ttCompanyTotalAggDF.first().toSeq  //
			)
			// 将每天聚合计算结果加入列表中
			rowList += aggRow
		}
		
		// 第一步、将列表转换为RDD
		val rowsRDD: RDD[Row] = session.sparkContext.parallelize(rowList.toList) // 将可变集合对象转换为不可变的
		// 第二步、自定义Schema信息
		val aggSchema: StructType = new StructType()
			.add("id", StringType, nullable = false) // 针对每天数据进行聚合得到一个结果，设置day为结果表中id
			.add("sumWsTotal", LongType, nullable = true)
			.add("maxWsTotal", LongType, nullable = true)
			.add("minWsTotal", LongType, nullable = true)
			.add("avgWsTotal", DoubleType, nullable = true)
			.add("sumCityTotal", LongType, nullable = true)
			.add("maxCityTotal", LongType, nullable = true)
			.add("minCityTotal", LongType, nullable = true)
			.add("avgCityTotal", DoubleType, nullable = true)
			.add("sumCompanyTotal", LongType, nullable = true)
			.add("maxCompanyTotal", LongType, nullable = true)
			.add("minCompanyTotal", LongType, nullable = true)
			.add("avgCompanyTotal", DoubleType, nullable = true)
		// 第三步、调用SparkSession中createDataFrame方法，组合RowsRDD和Schema为DataFrame
		val aggDF: DataFrame = session.createDataFrame(rowsRDD, aggSchema)
		
		// step3. 返回计算指标结果
		aggDF
	}
	
	def main(args: Array[String]): Unit = {
		execute(
			this.getClass, //
			OfflineTableDefine.WAREHOUSE_TRANSPORT_TOOL_DETAIL, //
			OfflineTableDefine.WAREHOUSE_TRANSPORT_TOOL_SUMMARY, //
			isLoadFullData = true //
		)
	}
}

```



## 08-[理解]-客户主题之数据调研及业务分析

> ​		针对电商互联网行业来说，客户资源属于第一个资源，如果没有客户，等于什么没有。
>
> [客户主题主要是通过分析用户的下单情况，给用户打上标签，最终构建用户画像。]()

![1614135266356](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614135266356.png)

> 针对物流项目来说，可以将用户划分为：
>
> - 1）、新增用户：今日注册
> - 2）、活跃用户：近10天下单
> - 3）、相当活跃用户：近30天下单
> - 4）、沉睡用户：90天至180天
> - 5）、流失用户：270天

> 对于电商公司来说，可以将用户活跃度划分为如下两个方面：

![1614135708740](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614135708740.png)

> 在电商公司中，通过一张桑葚图表示用户活跃度关系

![1614135751148](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614135751148.png)

> 事实表就是：用户表`tbl_customer`，加载数据时属于全量加载

> 相关维度表如下所示：

![1614135888975](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614135888975.png)

> 事实表与维度表关联关系

![1614135906759](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614135906759.png)



## 09-[掌握]-客户主题之数据拉宽开发

> ​		编写DWD层宽表开发操作，加载Kudu表中客户tbl_customer表数据与相关维度表数据进行关联。

[其中宽表中有如下几个字段：]()

![1614136093105](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614136093105.png)

> 上述几个指标属于聚合统计出来：
>
> - 1）、用户首次和末次下单时间：max和min
>   - 按照用户分组统计出，所有订单中最大下单时间：首次下单和最小下单时间：末次下单
> - 2）、用户下单总数和总金额
>   - 按照用户分组统计出，每个用户总的订单数：count和所有订单金额：sum

![1614136270386](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614136270386.png)

```scala
SELECT
	"ciid", 
	min(sender_info."id") first_id, -- 首次下单订单ID
	max(sender_info."id") last_id, -- 末次下单订单ID
	min(sender_info."cdt")first_cdt, -- 首次下单订单日期时间
	max(sender_info."cdt") last_cdt, -- 末次下单订单ID
	COUNT(sender_info."id" )billCount, -- 用户下单数目
	sum(express_package."actual_amount") totalAmount -- 用户下单总金额
FROM 
	"tbl_consumer_sender_info" sender_info
LEFT JOIN 
	"tbl_express_package" express_package
ON 
	SENDER_INFO."pkg_id" =express_package."id"
GROUP BY 
	sender_info."ciid ;
```

> 创建对象：`CustomerDWD`，继承基类：继承自`AbstractOfflineApp`特质，实现其中`process`方法

```scala
package cn.itcast.logistics.offline.dwd

import cn.itcast.logistics.common.{CodeTypeMapping, Configuration, OfflineTableDefine, TableMapping}
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

/**
 * 客户主题拉链宽表：
 *      将Kudu中ODS层客户表：tbl_customer与相关维度表数据进行JOIN关联，拉链为宽表存储Kudu中DWD层。
 */
object CustomerDWD extends AbstractOfflineApp{
	/**
	 * 对数据集DataFrame按照业务需求编码，事实表与维度表进行关联
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		import dataframe.sparkSession.implicits._
		
		// TODO: 1). 加载维度表的数据
		val customerSenderInfoDF: DataFrame = loadKuduSource(
			TableMapping.CONSUMER_SENDER_INFO, isLoadFullData = Configuration.IS_FIRST_RUNNABLE
		)
		val expressPackageDF: DataFrame = loadKuduSource(TableMapping.EXPRESS_PACKAGE, isLoadFullData = true)
		val codesDF: DataFrame = loadKuduSource(TableMapping.CODES, isLoadFullData = true)
		val customerTypeDF : DataFrame= codesDF.where($"type" === CodeTypeMapping.CUSTOM_TYPE)
		
		// TODO: step2. 定义维度表与事实表的关联
		val joinType: String = "left_outer"
		// 获取每个用户的首尾单发货信息及发货件数和总金额
		val customerSenderDetailInfoDF: DataFrame = customerSenderInfoDF
			.join(expressPackageDF, expressPackageDF("id") === customerSenderInfoDF("pkgId"), joinType)
			.groupBy(customerSenderInfoDF("ciid"))
			.agg(
				min(customerSenderInfoDF("id")).alias("first_id"), //
				max(customerSenderInfoDF("id")).alias("last_id"),  //
				min(expressPackageDF("cdt")).alias("first_cdt"),  //
				max(expressPackageDF("cdt")).alias("last_cdt"),  //
				count(customerSenderInfoDF("id")).alias("totalCount"),  //
				sum(expressPackageDF("actualAmount")).alias("totalAmount")  //
			)
		
		val customerDF: DataFrame = dataframe
		val customerDetailDF: DataFrame = customerDF
			.join(customerSenderDetailInfoDF, customerDF("id") === customerSenderInfoDF("ciid"), joinType)
			.join(customerTypeDF, customerDF("type") === customerTypeDF("code").cast(IntegerType), joinType)
			.sort(customerDF("cdt").asc)
			.select(
				customerDF("id"),
				customerDF("name"),
				customerDF("tel"),
				customerDF("mobile"),
				customerDF("type").cast(IntegerType),
				customerTypeDF("codeDesc").as("type_name"),
				customerDF("isownreg").as("is_own_reg"),
				customerDF("regdt").as("regdt"),
				customerDF("regchannelid").as("reg_channel_id"),
				customerDF("state"),
				customerDF("cdt"),
				customerDF("udt"),
				customerDF("lastlogindt").as("last_login_dt"),
				customerDF("remark"),
				customerSenderDetailInfoDF("first_id").as("first_sender_id"), //首次寄件id
				customerSenderDetailInfoDF("last_id").as("last_sender_id"), //尾次寄件id
				customerSenderDetailInfoDF("first_cdt").as("first_sender_cdt"), //首次寄件时间
				customerSenderDetailInfoDF("last_cdt").as("last_sender_cdt"), //尾次寄件时间
				customerSenderDetailInfoDF("totalCount"), //寄件总次数
				customerSenderDetailInfoDF("totalAmount") //总金额
			)
		
		// 返回拉链后宽表数据
		customerDetailDF
	}
	
	def main(args: Array[String]): Unit = {
		execute(
			this.getClass, //
			TableMapping.CUSTOMER, //
			OfflineTableDefine.CUSTOMER_DETAIL, //
			isLoadFullData = true //
		)
	}
}

```

> 运行程序，发现有的用户没有下过单，所以统计聚合结果为null。

![1614137135488](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614137135488.png)

## 10-[掌握]-客户主题之指标计算开发

> ​		前面已经将客户宽表数据构建完成，编写程序，对宽表数据进行指标计算。

![1614137195468](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614137195468.png)

```scala
package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common.OfflineTableDefine
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}

/**
 * 客户主题指标开发：
 *      从Kudu表加载客户详情宽表数据，按照具体业务进行统计分析，最后结果存储Kudu中DWS层。
 */
object ConsumerDWS extends AbstractOfflineApp{
	/**
	 * 对数据集DataFrame按照业务需求编码，对宽表数据按照指标进行计算
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		val session = dataframe.sparkSession
		import session.implicits._
		
		// step1. 直接进行指标计算，无需按照划分数据
		/*
			指标计算时，需要计算每个用户下单日期距今天数，两个日期相差天数，此时使用日期时间函数
				now() / current_date() / current_timestamp()
				date_sub() / date_add()
				date_format()
				date_diff()
		 */
		// 指标一：总客户数
		val customerCount: Long = dataframe.count()
		// 指标二：今日新增客户数(注册时间为今天)
		val additionCustomerCount: Long = dataframe
			.where(
				//  substring($"regdt", 0, 10) === date_sub(current_date(), 1)
				date_format($"regdt", "yyyy-MM-dd") === date_sub(current_date(), 1)
			)
			.count()
		// 指标三：留存数(超过180天未下单表示已流失，否则表示留存) 和留存率
		val preserveCustomerCount: Long = dataframe
			//.where($"last_sender_cdt" >= date_sub(current_date(), 180))
			.where(
				datediff(current_date(), $"last_sender_cdt") <= 180
			)
			.count()
		val preserveRate: Double = preserveCustomerCount / customerCount.toDouble
		// 指标四：活跃用户数(近10天内有发件的客户表示活跃用户)
		val activeCustomerCount: Long = dataframe
			.where($"last_sender_cdt" >= date_sub(current_date(), 10))
			.count()
		// 指标五：月度新用户数
		val monthOfNewCustomerCount: Long = dataframe
			.where(
				$"regDt".between(
					trunc(current_timestamp(), "month"), date_format(current_date(), "yyyy-MM-dd")
				)
			)
			.count()
		// 指标六：沉睡用户数(3个月~6个月之间的用户表示已沉睡)
		val sleepCustomerCount = dataframe
			.where(
				"last_sender_cdt >= date_sub(now(), 180) and last_sender_cdt <= date_sub(now(), 90)"
			)
			.count()
		// 指标七：流失用户数(9个月未下单表示已流失)
		val loseCustomerCount: Long = dataframe
			.where("last_sender_cdt <= date_sub(now(), 270)")
			.count()
		// 指标八：客单价、客单数、平均客单数
		val customerBillDF: DataFrame = dataframe.where("first_sender_id is not null")
		// 客单数：下单顾客数目
		val customerBillCount: Long = customerBillDF.count()
		// 客单价 = 总金额/总单数
		val customerBillAggDF: DataFrame = customerBillDF.agg(
			sum($"totalCount").as("sumCount"), // 总单数
			sum($"totalAmount").as("sumAmount") // 总金额
		)
		val billAggRow: Row = customerBillAggDF.first()
		// 客单价 = 总金额/总单数
		val customerAvgAmount: Double = billAggRow.getAs[Double]("sumAmount") / billAggRow.getAs[Long]("sumCount")
		// 平均客单数：平均每个顾客下单数目
		val avgCustomerBillCount: Double = billAggRow.getAs[Long]("sumCount") / customerBillCount.toDouble
		
		// TODO： 需要将计算所有指标结果提取出来，并且组合到Row对象中
		val dayValue: String = dataframe
			.select(
				date_format(current_date(), "yyyyMMdd").cast(StringType)
			)
			.limit(1).first().getAs[String](0)
		val aggRow: Row = Row(
			dayValue, //
			customerCount, //
			additionCustomerCount, //
			preserveRate, //
			activeCustomerCount, //
			monthOfNewCustomerCount, //
			sleepCustomerCount, //
			loseCustomerCount, //
			customerBillCount, //
			customerAvgAmount, //
			avgCustomerBillCount
		)
		
		// step2. 转换数据为DataFrame数据集
		// 第一步、将列表转换为RDD
		val rowsRDD: RDD[Row] = session.sparkContext.parallelize(Seq(aggRow)) // 将可变集合对象转换为不可变的
		// 第二步、自定义Schema信息
		val aggSchema: StructType = new StructType()
			.add("id", StringType, nullable = false) // 针对每天数据进行聚合得到一个结果，设置day为结果表中id
			.add("customerCount", LongType, nullable = true)
			.add("additionCustomerCount", LongType, nullable = true)
			.add("preserveRate", DoubleType, nullable = true)
			.add("activeCustomerCount", LongType, nullable = true)
			.add("monthOfNewCustomerCount", LongType, nullable = true)
			.add("sleepCustomerCount", LongType, nullable = true)
			.add("loseCustomerCount", LongType, nullable = true)
			.add("customerBillCount", LongType, nullable = true)
			.add("customerAvgAmount", DoubleType, nullable = true)
			.add("avgCustomerBillCount", DoubleType, nullable = true)
		
		// 第三步、调用SparkSession中createDataFrame方法，组合RowsRDD和Schema为DataFrame
		val aggDF: DataFrame = session.createDataFrame(rowsRDD, aggSchema)
		
		// step3. 返回指标计算结果
		aggDF
	}
	
	def main(args: Array[String]): Unit = {
		execute(
			this.getClass, //
			OfflineTableDefine.CUSTOMER_DETAIL, //
			OfflineTableDefine.CUSTOMER_SUMMERY, //
			isLoadFullData = true //
		)
	}
}
```

> 程序运行结果如下所示：

![1614138248067](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614138248067.png)



## 11-[掌握]-工作流调度之Azkaban 业务调度开发

> ​		主题报表开发完成以后，需要使用调度器，进行定时调度执行（其中涉及到依赖调度），此处使用Azkaban框架调度报表执行。

![1614138890643](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614138890643.png)

> 使用Azkaban调度报表执行，主要编写Shell 脚本，定时执行脚本即可。
>
> [物流项目中，所有主题报表分析，逻辑基本相似，仅仅执行程序名称不一样，所以可以编写一个Shell 脚本，传递参数，执行不同主题报表程序。]()
>
> 脚本：`scheduler.sh`

```SHELL
#!/bin/sh

cls=$1
flag=0
clsDwd=cn.itcast.logistics.offline.dwd.${cls}DWD
clsDws=cn.itcast.logistics.offline.dws.${cls}DWS
baseDir=/export/services/logistics/lib

# 获取运行主题名称, 用于组装类名称
if [[ $cls = "Customer" || $cls = "ExpressBill" || $cls = "TransportToolDot" || $cls = "TransportToolWarehouse" || $cls = "Warehouse" || $cls = "Waybill" ]]; then
        echo -e "\e[32m==== MainClass is: "$clsDwd" and "$clsDws"\e[0m"
        flag=1
else
        echo -e "\e[31mUsage : \n\tExpressBill\n\tCustomer\n\tDotTransportTool\n\tWareHouseTransportTool\n\tWareHouse\n\tWaybill\e[0m"
fi

# 当即将运行应用时, 需要设置参数
if [[ $flag = 1 ]]; then
        echo -e "\e[32m==== builder spark commands ====\e[0m"
        # 构建DWD宽表应用运行命令
        cmd1="spark-submit --packages org.apache.kudu:kudu-spark2_2.11:1.9.0-cdh6.2.1 --class ${clsDwd} --master yarn --deploy-mode cluster --driver-memory 512m --executor-cores 1 --executor-memory 512m --queue default --verbose ${baseDir}/logistics-etl.jar"
        # 构建DWS指标计算应用运行命令
        cmd2="spark-submit --packages org.apache.kudu:kudu-spark2_2.11:1.9.0-cdh6.2.1 --class ${clsDws} --master yarn --deploy-mode cluster --driver-memory 512m --executor-cores 1 --executor-memory 512m --queue default --verbose ${baseDir}/logistics-etl.jar"
        echo -e "\e[32m==== CMD1 is: $cmd1 ====\e[0m"
        echo -e "\e[32m==== CMD2 is: $cmd2 ====\e[0m"
fi

# 运行命令, 执行Spark应用
if [[ $flag = 1 && `ls -A $baseDir|wc -w` = 1 ]]; then
        echo -e "\e[32m==== start execute ${clsDwd} ====\e[0m"
        sh $cmd1
        echo -e "\e[32m==== start execute ${clsDws} ====\e[0m"
        sh $cmd2
else
        echo -e "\e[31m==== The jar package in $baseDir directory does not exist! ====\e[0m"
        echo -e "\e[31m==== Plase upload logistics-common.jar,logistics-etl.jar,logistics-generate.jar ====\e[0m"
fi

```

> 针对每个主题报表在Azkaban上创建一个Project工程，上传JOB属性文件，设置定时执行频率。
>
> [以快递单为了，编写JOB属性文件：schedulerExpressBill.job]()

```ini
#command
type=command
command=sh /export/services/logistics/bin/scheduler.sh ExpressBill
```

```
面试题一：
	Spark on YARN运行在不同DeployMode下流程图，画图，口述流程
```



## 12-[理解]-即席查询之用户标签开发

> 针对用户相关表的数据进行统计开发，给每个用户打上标签Tag，更好的洞察了解用户。

![1614139946487](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614139946487.png)

> 启动相关服务：HDFS、Hive MetaStore、Impala服务及Hue

![1614140355687](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614140355687.png)

> 需求一：用户首单时间，采用`WITH语句`编写

```SQL
WITH tmp AS(
   SELECT tcsi.ciid AS cid, MIN(tcsi.cdt) AS cdt FROM tbl_consumer_sender_info tcsi GROUP BY tcsi.ciid
)
SELECT t1.cid, t1.cdt, t2.name FROM tmp t1 LEFT JOIN tbl_customer t2 ON t2.id = t1.cid WHERE NAME IS NOT NULL;
```

![1614140657118](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614140657118.png)

> 其他标签开发，自己完成

![1614140628521](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614140628521.png)



## 13-[了解]-第7章：内容概述和课程目标

> ​		第7章，主要完成，将物流相关主要业务数据，实时ETL同步存储到Elasticsearch分布式索引中。

![1614148670495](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614148670495.png)

> 针对物流快递公司来说，相对用户（客户）来说，需要依据快递单号查询快递物流信息。
>
> [2张表数据：tbl_express_bill快递单数据和tbl_waybill 运单数据。]()

![1614148721007](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614148721007.png)

> 客户：依据提供快递单号（运单号），查询快递物流信息。

![1614148904987](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614148904987.png)

> 课程目录：主要就是掌握Spark与Elasticsearch集成使用。

![1614148946392](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614148946392.png)



## 14-[理解]-ES全文检索之快递追踪需求

> ​		快递又称速递、快件和快运，是物流的一种表现形式，与传统物流的区别是，物流的核心要素是仓储，运输和包装，而快递是一种门到门的个性化物流服务，更重视速度。

![1614149073163](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614149073163.png)

> ​		快递业作为服务产业，需要对客户负责，及时向客户响应和回馈货物的运输情况，因此快递业建立货物跟踪管理系统非常有必要。

![1614149551959](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614149551959.png)

> 本项目来说，将快递单数据和运单数据保存至Elasticsearch索引中，便于查询。

 ![1614149919021](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614149919021.png)

> ​			当将物流核心业务数据存储在Es索引中以后，不仅可以查询物流信息，而且可以使用Kibana做一些基本报表分析。
>
> [Elasticsearch 发展技术栈：ELK（三巨头） -> ElasticStack（Es、LogStash、Kibana、Beats]()

## 15-[理解]-ES全文检索之技术选项及业务流程

> ​		使用Elasticsearch存储物流核心业务数据：快递单数据和运单数据。

![1614150459049](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614150459049.png)

> ​		需要编写结构化流StructuredStreaming应用程序，实时从Kafka消费数据（物流系统，只需要expressBill和wayBill），进行ETL转换，存储到Es索引中。

![1614150530485](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614150530485.png)

> 技术栈：`Oracle -> Kafka -> StructuredStreaming -> Elasticsearch` 
>
> [核心点：Spark集成Es，Es官方提供类库，直接使用即可，需要导入相关依赖库]()

```xml
        <dependency>
            <groupId>org.elasticsearch</groupId>
            <artifactId>elasticsearch-spark-20_2.11</artifactId>
            <version>${es.version}</version>
        </dependency>
```

> 实时将物流快递单等业务数据增量同步至Elasticseach索引中，数据流程图如下所示：

![1614150809829](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614150809829.png)



## 16-[掌握]-ES全文检索之启动Es和Kibana

> ​	物流项目中，Es和Kibana都已经按照完成，在node2.itcast.cn机器安装上，具体安装参考【第4章 客快物流大数据之大数据服务器环境配置】中【3.6】和【3.7】章节即可。

![1614150911084](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614150911084.png)

> 启动Elasticsearch和Kibana，使用普通用户`es`，进行安装，所以启动时，需要切换到`es`用户
>

```ini
[root@node2 ~]# su - es
[es@node2 ~]$
[es@node2 ~]$ cd /export/services/
[es@node2 ~]$
[es@node2 services]$ cd es/
[es@node2 es]$ bin/elasticsearch -d
[es@node2 es]$ 
[es@node2 es]$ jps
7208 Jps
7193 Elasticsearch

```

![1614151129613](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614151129613.png)

> 启动Kibana，启动需要一定时间，大概2-3分钟左右

```ini
[root@node2 ~]# su - es
Last login: Wed Feb 24 15:02:59 CST 2021 on pts/0
[es@node2 ~]$ 
[es@node2 ~]$ cd /export/services/kibana
[es@node2 kibana]$ 
[es@node2 kibana]$ nohup ./bin/kibana &
```

![1614152310414](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614152310414.png)



## 17-[掌握]-ES全文检索之实时ETL存储【结构】

> ​		编写结构化流程序：`EsStreamApp`，实时从kafka消费数据，进行ETL转换操作，存储至Es索引中。

![1614152589578](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614152589578.png)

> 创建索引index，可以在Es-Head上创建，也可以在Kibana创建。
>
> [将数据存储到Es索引中时，建议用户自己创建索引，不要程序自动创建。]()

![1614152663180](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614152663180.png)

> 编写主类：EsStreamApp对象，与前面编写：KuduStreamApp，进行保存数据不一样，其他基本一样。

```scala
package cn.itcast.logistics.etl.realtime

import cn.itcast.logistics.common.{Configuration, SparkUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Elasticsearch 数据管道应用：实现Elasticsearch数据库的实时ETL操作
 */
object EsStreamApp extends BasicStreamApp {
	/**
	 * 数据的处理
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	override def process(streamDF: DataFrame, category: String): DataFrame = ???
	
	/**
	 * 数据的保存
	 *
	 * @param streamDF          保存数据集DataFrame
	 * @param tableName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	override def save(streamDF: DataFrame, tableName: String, isAutoCreateTable: Boolean): Unit = ???
	
	/**
	 * 真正提取快递单数据和运单数据，保存至Elasticsearch索引中
	 */
	def saveToEs(streamDF: DataFrame): Unit = {
		// 1. 快递单数据提取和保存
		// a. Bean -> POJO
		
		// b. 保存至Es索引
		
		
		// 2. 运单数据提取和保存
		// a. Bean -> POJO
		
		// b. 保存至Es索引
		
	}
	
	/*
	实时Elasticsearch ETL应用程序入口，数据处理逻辑步骤：
		step1. 创建SparkSession实例对象，传递SparkConf
		step2. 从Kafka数据源实时消费数据
		step3. 对JSON格式字符串数据进行转换处理
		step4. 获取消费每条数据字段信息
		step5. 将解析过滤获取的数据写入同步至Es索引
	*/
	def main(args: Array[String]): Unit = {
		// step1. 创建SparkSession实例对象，传递SparkConf
		val spark: SparkSession = SparkUtils.createSparkSession(
			SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass
		)
		import spark.implicits._ 
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		
		// step2. 从Kafka数据源实时消费数据
		val logisticsStreamDF: DataFrame = load(spark, "logistics")
		
		// step3. 对JSON格式字符串数据进行转换处理: JSON -> Bean
		val logisticsEtlStreamDF: DataFrame = process(logisticsStreamDF, "logistics")
		
		// step4. 获取消费每条数据字段信息，只需要获取快递单和运单业务数据
		saveToEs(logisticsEtlStreamDF)
		
		// step5. 启动流式应用以后，等待终止
		spark.streams.active.foreach(query => println(s"正在启动Query： ${query.name}"))
		spark.streams.awaitAnyTermination()
	}
}

```



## 18-[掌握]-ES全文检索之实时ETL存储【转换】

> ​		实现EsStreamApp对象中process方法和saveToEs方法，对数据进行转换操作。

- 1）、实现process方法：转换JSON字符串为MessageBean对象

```scala
	/**
	 * 数据的处理，将JSON字符串转换为MessageBean对象
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	override def process(streamDF: DataFrame, category: String): DataFrame = {
		// 导入隐式转换
		import streamDF.sparkSession.implicits._
		
		// 依据不同业务系统数据进行不同的ETL转换
		category match {
			case "logistics" =>
				// 第一步、转换JSON为Bean对象
				val beanStreamDS: Dataset[OggMessageBean] = streamDF
					.as[String] // 将DataFrame转换为Dataset
					.filter(json => null != json && json.trim.length > 0) // 过滤数据
					// 由于OggMessageBean是使用Java语言自定义类，所以需要自己指定编码器Encoder
					.mapPartitions { iter =>
						iter.map { json => JSON.parseObject(json, classOf[OggMessageBean]) }
					}
				// 第二步、返回转换后数据
				beanStreamDS.toDF()
			case _ => streamDF	
		}
	}
```



- 2）、saveToEs方法：提取Bean中数据字段，封装为POJO，及保存至Es索引中

```scala
	/**
	 * 真正提取快递单数据和运单数据，保存至Elasticsearch索引中
	 */
	def saveToEs(streamDF: DataFrame): Unit = {
		// 转换DataFrame为Dataset
		val oggBeanStreamDS: Dataset[OggMessageBean] = streamDF.as[OggMessageBean]
		
		// 1. 快递单数据提取和保存
		// a. Bean -> POJO
		val expressBillStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EXPRESS_BILL)
			.map(bean => DataParser.toExpressBill(bean))
			.filter(pojo => null != pojo)
			.toDF()
		// b. 保存至Es索引
		save(expressBillStreamDF, TableMapping.EXPRESS_BILL)
		
		// 2. 运单数据提取和保存
		// a. Bean -> POJO
		val waybillStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAY_BILL)
			.map(bean => DataParser.toWaybill(bean))
			.filter(pojo => null != pojo)
			.toDF()
		// b. 保存至Es索引
		save(waybillStreamDF, TableMapping.WAY_BILL)
	}
```

> 接下来重点在于，如何将DataFrame数据集（流式Stream DataFrame）保存至Es索引中。



## 19-[掌握]-ES全文检索之实时ETL存储【存储】

> ​	Elasticsearch官网提供与Spark集成类库，只要引入依赖，调用API使用即可。

> Elasticsearch文档：
>
> - 1）、配置Configuration：https://www.elastic.co/guide/en/elasticsearch/hadoop/7.6/configuration.html
>
> - 2）、集成Spark：https://www.elastic.co/guide/en/elasticsearch/hadoop/7.6/spark.html

![1614154447148](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614154447148.png)

![1614084144081](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614084144081.png)

> 实现save方法，将DataFrame保存至Es索引中。

```scala
	/**
	 * 数据的保存，将流式DataFrame数据集保存至Es索引中
	 *
	 * @param streamDF          保存数据集DataFrame
	 * @param indexName         保存索引的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	override def save(streamDF: DataFrame, indexName: String, isAutoCreateTable: Boolean): Unit = {
		streamDF
			.drop(col("opType"))  // 删除操作类型字段，针对快递单和运单不会修改和删除
			.writeStream
    		.outputMode(OutputMode.Append()) // 结构化流与Es集成时，只支持一种输出模式Append
    		.queryName(s"query-${Configuration.SPARK_ES_FORMAT}-${indexName}")
    		.format(Configuration.SPARK_ES_FORMAT)
			.option("es.nodes", Configuration.ELASTICSEARCH_HOST)
			.option("es.port", Configuration.ELASTICSEARCH_HTTP_PORT + "")
			.option("es.index.auto.create", if(isAutoCreateTable) "yes" else "no")
			.option("es.write.operation", "upsert")
			.option("es.mapping.id", "id")
    		.start(indexName)
	}
```



> EsStreamApp程序完整代码如下所示：

```scala
package cn.itcast.logistics.etl.realtime

import cn.itcast.logistics.common.{Configuration, SparkUtils, TableMapping}
import cn.itcast.logistics.common.BeanImplicits._
import cn.itcast.logistics.common.beans.parser.OggMessageBean
import cn.itcast.logistics.etl.parser.DataParser
import cn.itcast.logistics.etl.realtime.KuduStreamApp.save
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

/**
 * Elasticsearch 数据管道应用：实现Elasticsearch数据库的实时ETL操作
 */
object EsStreamApp extends BasicStreamApp {
	/**
	 * 数据的处理，将JSON字符串转换为MessageBean对象
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	override def process(streamDF: DataFrame, category: String): DataFrame = {
		// 导入隐式转换
		import streamDF.sparkSession.implicits._
		
		// 依据不同业务系统数据进行不同的ETL转换
		category match {
			case "logistics" =>
				// 第一步、转换JSON为Bean对象
				val beanStreamDS: Dataset[OggMessageBean] = streamDF
					.as[String] // 将DataFrame转换为Dataset
					.filter(json => null != json && json.trim.length > 0) // 过滤数据
					// 由于OggMessageBean是使用Java语言自定义类，所以需要自己指定编码器Encoder
					.mapPartitions { iter =>
						iter.map { json => JSON.parseObject(json, classOf[OggMessageBean]) }
					}
				// 第二步、返回转换后数据
				beanStreamDS.toDF()
			case _ => streamDF
		}
	}
	
	/**
	 * 数据的保存，将流式DataFrame数据集保存至Es索引中
	 *
	 * @param streamDF          保存数据集DataFrame
	 * @param indexName         保存索引的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	override def save(streamDF: DataFrame, indexName: String, isAutoCreateTable: Boolean): Unit = {
		streamDF
			.drop(col("opType"))  // 删除操作类型字段，针对快递单和运单不会修改和删除
			.writeStream
    		.outputMode(OutputMode.Append()) // 结构化流与Es集成时，只支持一种输出模式Append
    		.queryName(s"query-${Configuration.SPARK_ES_FORMAT}-${indexName}")
    		.format(Configuration.SPARK_ES_FORMAT)
			.option("es.nodes", Configuration.ELASTICSEARCH_HOST)
			.option("es.port", Configuration.ELASTICSEARCH_HTTP_PORT + "")
			.option("es.index.auto.create", if(isAutoCreateTable) "yes" else "no")
			.option("es.write.operation", "upsert")
			.option("es.mapping.id", "id")
    		.start(indexName)
	}
	
	/**
	 * 真正提取快递单数据和运单数据，保存至Elasticsearch索引中
	 */
	def saveToEs(streamDF: DataFrame): Unit = {
		// 转换DataFrame为Dataset
		val oggBeanStreamDS: Dataset[OggMessageBean] = streamDF.as[OggMessageBean]
		
		// 1. 快递单数据提取和保存
		// a. Bean -> POJO
		val expressBillStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EXPRESS_BILL)
			.map(bean => DataParser.toExpressBill(bean))
			.filter(pojo => null != pojo)
			.toDF()
		// b. 保存至Es索引
		save(expressBillStreamDF, TableMapping.EXPRESS_BILL)
		
		// 2. 运单数据提取和保存
		// a. Bean -> POJO
		val waybillStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAY_BILL)
			.map(bean => DataParser.toWaybill(bean))
			.filter(pojo => null != pojo)
			.toDF()
		// b. 保存至Es索引
		save(waybillStreamDF, TableMapping.WAY_BILL)
	}
	
	/*
	实时Elasticsearch ETL应用程序入口，数据处理逻辑步骤：
		step1. 创建SparkSession实例对象，传递SparkConf
		step2. 从Kafka数据源实时消费数据
		step3. 对JSON格式字符串数据进行转换处理
		step4. 获取消费每条数据字段信息
		step5. 将解析过滤获取的数据写入同步至Es索引
	*/
	def main(args: Array[String]): Unit = {
		// step1. 创建SparkSession实例对象，传递SparkConf
		val spark: SparkSession = SparkUtils.createSparkSession(
			SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass
		)
		import spark.implicits._
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		
		// step2. 从Kafka数据源实时消费数据
		val logisticsStreamDF: DataFrame = load(spark, "logistics")
		
		// step3. 对JSON格式字符串数据进行转换处理: JSON -> Bean
		val logisticsEtlStreamDF: DataFrame = process(logisticsStreamDF, "logistics")
		
		// step4. 获取消费每条数据字段信息，只需要获取快递单和运单业务数据
		saveToEs(logisticsEtlStreamDF)
		
		// step5. 启动流式应用以后，等待终止
		spark.streams.active.foreach(query => println(s"正在启动Query： ${query.name}"))
		spark.streams.awaitAnyTermination()
	}
}

```



## 20-[掌握]-ES全文检索之实时ETL存储【测试】

> ​		模拟生成快递单和运单表数据，插入Oracle数据库表中，通过OGG实时采集存储至Kafka，再由结构化流应用程序消费数据，进行ETL转换，存储至Elasticsearch索引中。

- 1）、编写模拟数据程序，仅仅生成快递单数据和运单数据（先清空，再插入）

```scala
package cn.itcast.logistics.generate;

import cn.itcast.logistics.common.utils.DBHelper;
import cn.itcast.logistics.common.utils.RDBTool;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 * 物流系统Logistics业务数据库Oracle造数程序
 */
public class MockEsDataApp {

    private static String DAT_SUFFIX = ".csv";
    private static String ENCODING = "UTF-8";
    /** Oracle **/
    private static final String ORACLE_DDL_SQL_FILE = "/oracle-db.sql";
    private static List<String> oracleTableNames = Arrays.asList("tbl_express_bill","tbl_waybill") ;
    private static String ORACLE_USER_TABLES_SQL_KEY = "ORACLE_USER_TABLES_SQL";
    private static String ORACLE_TABLE_DDL_SQL_KEY = "ORACLE_TABLE_DDL_SQL";
    private static String ORACLE_TABLE_SCHEMA_SQL_KEY = "ORACLE_TABLE_SCHEMA_SQL";
    private static Map<String, String> oracleSqlHelps = new HashMap<String, String>() {{
        put(ORACLE_USER_TABLES_SQL_KEY, "SELECT TABLE_NAME,TABLESPACE_NAME FROM user_tables WHERE TABLESPACE_NAME='TBS_LOGISTICS'");
        put(ORACLE_TABLE_DDL_SQL_KEY, "SELECT dbms_metadata.get_ddl('TABLE',?) FROM DUAL");
        put(ORACLE_TABLE_SCHEMA_SQL_KEY, "SELECT COLUMN_NAME,DATA_TYPE FROM user_tab_columns WHERE TABLE_NAME=?");
    }};
    /** PUBLIC SQL **/
    private static String CLEAN_TABLE_SQL = "TRUNCATE TABLE ?";
    // Oracle JDBC
    private static final DBHelper oracleHelper = DBHelper.builder()
            .withDialect(DBHelper.Dialect.Oracle)
            .withUrl("jdbc:oracle:thin:@//192.168.88.10:1521/ORCL")
            .withUser("itcast")
            .withPassword("itcast")
            .build();

    public static void main( String[] args) {
        boolean isClean = true;
        if (args.length == 1 && args[0].matches("(true|false)")) {
            isClean = Boolean.parseBoolean(args[0]);
        }
        Map<String, List<String>> oracleSqls = buildOracleTableDML();
        /** ==== 初始化Oracle ==== **/
        Connection connection1 = oracleHelper.getConnection();
        // 清空表
        if(isClean) {
            // 清空Oracle表
            oracleTableNames.forEach(tableName -> {
                try {
                    System.out.println("==== 开始清空Oracle的：" + tableName + " 数据 ====");
                    RDBTool.update(CLEAN_TABLE_SQL, tableName, (sql, table) -> executeUpdate(connection1, sql.replaceAll("\\?", "\"" + table + "\""), 1));
                    System.out.println("==== 完成清空Oracle的：" + tableName + " 数据 ====");
                    Thread.sleep(200 * 2);
                } catch (Exception e) {}
            });
        }
        // 插入数据到Oracle表（每2秒插入一条记录）
        oracleSqls.forEach((table,sqlList) -> {
            try {
                System.out.println("==== 开始插入数据到Oracle的：" + table + " ====");
                sqlList.forEach(sql -> RDBTool.save(sql, sqlStr -> executeUpdate(connection1, sql, 1)));
                System.out.println("==== 完成插入数据到Oracle的：" + table + " ====");
                Thread.sleep(1000 * 2);
            } catch (Exception e) {}
        });
        oracleHelper.close(connection1);
    }

    /**
     * 根据table读取csv，并从csv文件中拼接表的INSERT语句
     */
    private static Map<String, List<String>> buildOracleTableDML() {
        Map<String, List<String>> sqls = new LinkedHashMap<>();
        // 从遍历表中获取表对应的数据文件
        oracleTableNames.forEach(table -> {
            String tableDatPath = null;
            try {
                // 根据表名获取类路径下的"表名.csv"绝对路径
                tableDatPath = MockEsDataApp.class.getResource("/" + table + DAT_SUFFIX).getPath();
            } catch (Exception e) { }
            if(!StringUtils.isEmpty(tableDatPath)) {
                StringBuilder insertSQL = new StringBuilder();
                try {
                    // 读取"表名.csv"的数据
                    List<String> datas = IOUtils.readLines(new BOMInputStream(new FileInputStream(tableDatPath)), ENCODING);
                    // 取出首行的schema
                    String schemaStr = datas.get(0).replaceAll("\"","");
                    String[] fieldNames = schemaStr.split(",");
                    // 获取数据库中的schema定义
                    Map<String, String> schemaMap = getOracleTableSchema(table);
                    datas.remove(0);
                    List<String> tblSqls = new LinkedList<>();
                    datas.forEach(line->{
                        boolean chk = false;
                        insertSQL.append("INSERT INTO \"" + table + "\"(\"").append(schemaStr.replaceAll(",","\",\"")).append("\") VALUES(");
                        String[] vals = line.split(",");
                        for (int i = 0; i < vals.length; i++) {
                            String fieldName = fieldNames[i];
                            String type = schemaMap.get(fieldName);
                            String val = vals[i].trim();
                            if("NVARCHAR2".equals(type)) {insertSQL.append((StringUtils.isEmpty(val)?"NULL":"'"+val+"'")+",");}
                            else if("DATE".equals(type)) {insertSQL.append("to_date('"+val+"','yyyy-mm-dd hh24:mi:ss'),");}
                            else if("NUMBER".equals(type)) {insertSQL.append(""+(StringUtils.isEmpty(val)?"0":val)+",");}
                            else {insertSQL.append(val+",");}
                            int diff = 0;
                            if (i==vals.length-1&&fieldNames.length>vals.length) {
                                diff = fieldNames.length-vals.length;
                                for (int j = 0; j < diff; j++) {insertSQL.append("NULL,");}
                            }
                            chk = vals.length+diff == fieldNames.length;
                        }
                        insertSQL.setLength(insertSQL.length()-1);
                        insertSQL.append(")");
                        if(chk) {
                            tblSqls.add(insertSQL.toString());
                        }
                        insertSQL.setLength(0);
                    });
                    sqls.put(table, tblSqls);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        return sqls;
    }

    /**
     * 根据table和fields获取schema
     * @param table     表名
     */
    private static Map<String, String> getOracleTableSchema(String table) {
        Map<String, LinkedHashMap<String,String>> tableSchema = getOracleAllTableSchema(null);
        return tableSchema.get(table);
    }

    /**
     * 从项目的DDL文件中获取每一张表的schema信息<table,[<fieldName><fieldType>]>
     */
    private static Map<String, LinkedHashMap<String,String>> getOracleAllTableSchema(String path) {
        if(StringUtils.isEmpty(path)) {
            path = MockEsDataApp.class.getResource(ORACLE_DDL_SQL_FILE).getPath();
        }
        Map<String, LinkedHashMap<String,String>> tableSchema = new LinkedHashMap<>();
        try {
            List<String> ddlSQL = FileUtils.readLines(new File(path), ENCODING);
            String table = null;
            String curLine = null;
            Map<String, String> schema = new LinkedHashMap<>();
            for (int i=0; i<ddlSQL.size(); i++) {
                curLine = ddlSQL.get(i);
                if(StringUtils.isEmpty(curLine)) {
                    continue;
                }
                if (curLine.contains("CREATE TABLE ")) {
                    table = curLine.substring(13, curLine.lastIndexOf(" ")).replaceAll("\"","");
                    continue;
                }
                if (curLine.contains("PRIMARY KEY")) {
                    LinkedHashMap<String, String> _schema = new LinkedHashMap<>();
                    _schema.putAll(schema);
                    tableSchema.put(table, _schema);
                    table = null;
                    schema.clear();
                }
                if (!StringUtils.isEmpty(table)) {
                    int offset = curLine.indexOf("(");
                    if (offset==-1) {offset = curLine.indexOf(",");}
                    String fieldInfo = curLine.substring(0, offset);
                    if(!StringUtils.isEmpty(fieldInfo)) {
                        String[] arr = fieldInfo.replaceAll("\"","").split(" ",2);
                        String fieldName = arr[0].trim();
                        String fieldType = arr[1].trim();
                        schema.put(fieldName, fieldType);
                    }
                }
            }
        } catch (IOException e) {
        }
        return tableSchema;
    }


    /**
     * 执行增删改的SQL
     */
    private static void executeUpdate(Connection connection, String sql, int dbType) {
        Statement st = null;
        ResultSet rs = null;
        int state = 0;
        try {
            if (null==connection||connection.isClosed()) {
                if(dbType==1){
                    connection = oracleHelper.getConnection();
                }
            }
            connection.setAutoCommit(false);
            st = connection.createStatement();
            state = st.executeUpdate(sql);
            if(sql.startsWith("INSERT")) {
                if (state > 0) {
                    connection.commit();
                    System.out.println("==== SQL执行成功："+sql+" ====");
                } else {
                    System.out.println("==== SQL执行失败："+sql+" ====");
                }
            } else {
                System.out.println("==== SQL执行成功："+sql+" ====");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if(dbType==1){
                oracleHelper.close(rs, st, null);
            }
        }
    }

}

```



- 2）、启动Oracle数据库和OGG及Kafka服务

> 先启动Zookeeper和Kafka服务，再启动Oracle数据库和OGG。

![1614155333937](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614155333937.png)

> 启动Oracle数据库和OGG，先启动容器，再进入容器进行操作

```ini
[root@node1 ~]# docker start myoracle
myoracle


[root@node1 ~]# docker exec -it myoracle /bin/bash
[root@server01 oracle]# 
[root@server01 oracle]# su - oracle
Last login: Mon Aug 31 09:00:22 UTC 2020 on pts/2
-bash: warning: setlocale: LC_ALL: cannot change locale (en_US): No such file or directory
-bash: warning: setlocale: LC_ALL: cannot change locale (en_US): No such file or directory
[oracle@server01 ~]$ sqlplus "/as sysdba"

SQL*Plus: Release 11.2.0.1.0 Production on Wed Feb 24 08:31:29 2021

Copyright (c) 1982, 2009, Oracle.  All rights reserved.

Connected to an idle instance.

SQL> startup
ORACLE instance started.

Total System Global Area 1202556928 bytes
Fixed Size                  2212816 bytes
Variable Size             654314544 bytes
Database Buffers          536870912 bytes
Redo Buffers                9158656 bytes
Database mounted.
Database opened.
SQL> 


[root@node1 ~]# docker exec -it myoracle /bin/bash
[root@server01 oracle]# su - oracle
Last login: Wed Feb 24 08:31:06 UTC 2021 on pts/0
-bash: warning: setlocale: LC_ALL: cannot change locale (en_US): No such file or directory
-bash: warning: setlocale: LC_ALL: cannot change locale (en_US): No such file or directory
[oracle@server01 ~]$ cd $ORACLE_HOME/bin
[oracle@server01 bin]$ 
[oracle@server01 bin]$ ls
[oracle@server01 bin]$ lsnrctl start

LSNRCTL for Linux: Version 11.2.0.1.0 - Production on 24-FEB-2021 08:32:25

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
Start Date                24-FEB-2021 08:32:30
Uptime                    0 days 0 hr. 0 min. 2 sec
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


```

![1614155638368](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614155638368.png)



- 3）、运行应用程序

  > 先运行EsStreamApp程序，再运行MockEsDataApp程序

![1614156193520](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614156193520.png)