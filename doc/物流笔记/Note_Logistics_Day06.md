---
stypora-copy-images-to: img
typora-root-url: ./
---



# Logistics_Day06：实时增量ETL存储Kudu



![1612344442449](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1612344442449.png)





## 01-[复习]-课程内容回顾 【Docker及服务器】

> ​		每个大数据项目核心：数据流转图（数据来源Source、数据处理Transformation及数据输出Sink），可以知道项目技术架构（技术选项）和项目应用。
>

![1613782117216](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613782117216.png)

> 物流项目分为：业务服务器（数据来源）和大数据服务器（数据存储和数据分析）。
>
> - ==1、业务服务器（业务系统产生数据存储地方）==
>
>   - 针对物流项目来说，物流相关业务数据存储在RDBMS关系型数据库表中，比如Oracle和MySQL数据库
>   - 物流系统相关数据：Oracle数据库，`Oracle11g`
>   - 客户关系管理系统：MySQL数据库，`MySQL5.7`
>
>   [数据库采用Docker部署安装，数据库服务运行在Docker容器中（为什么使用Docker）]()
>
>   - 业务数据如何采集到大数据服务器（存储到Kafka 消息队列），==实时增量采集数据==
>
>     - 物流系统Logistics业务数据：使用`OGG`同步Oracle数据库表的数据
>
>     ![1613782616304](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613782616304.png)
>
>     - CRM系统业务数据：使用`Canal`同步MySQL数据库表的数据
>
>     ![1613782662489](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613782662489.png)

![1613782280868](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613782280868.png)



## 02-[复习]-课程内容回顾 【Kudu及ETL】

> ​		当将数据从业务系统存储采集到大数据存储引擎（Kafka分布式消费队列）后，需要实时消费业务数据，进行ETL转换处理，最终存储到存储引擎中（比如Kudu数据库、Elasticsearch索引和ClickHouse数据库）。
>
> [当将业务数据存储到存储引擎后，按照具体业务需要进行分析（离线报表分析和实时查询分析）。]()

![1613783556397](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613783556397.png)

> - 2、大数据服务器（基于CM6.2安装部署CDH6.2大数据框架）
>
>   - 1）、CM如何部署CDH集群，架构思想
>
> ![1613783705948](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613783705948.png)
>
>   - 2）、将业务数据实时ETL存储至Kudu数据库中
>
>     - [Kudu存储引擎，取代HDFS和HBase，属于两者之间折中产品，既能够满足批量加载分析，又能随机读写访问，由Cloudera公司开源大数据框架，与Spark和Impala无缝集成。]()
>
>  ![1613783857845](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613783857845.png)
>
>     - Kudu 存储架构，[类似HBase数据库存储架构，不依赖第三方进行协作，基于Raft协议使用一致性]()
>
>  ![1613783955389](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613783955389.png)
>
>     - Kudu Client
>       - 第一个、Java/C++/Python Client API使用，[讲解Java Client API使用【KuduClient】]()
>       - 第二个、集成Spark框架，[KuduContext、DataFrame]()
>       - 第三个、集成Impala，直接使用SQL分析Kudu表数据
>
>   - 3）、实时ETL存储，[从Kafka消费数据，进行ETL转换存储至Kudu]()
>
>     - 实时流计算引擎选择：StructuredStreaming结构化流，为什么使用？？？
>     - 构建项目开发环境，创建工程和目录，导入依赖和工具类
>     - 编写流式计算程序（StructuredStreaming编程模板）
>     - 编写公共基类`BasicStreamApp`
>
>  ![1613784240681](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613784240681.png)
>
> - 编写`KuduStreamApp`，实时ETL存储至Kudu程序 
>
> ![1613784310379](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613784310379.png)
>
> - process方法：对消费Kafka数据进行ETL转换，将JSON字符串转换为MessageBean对象
>   - OggMessageBean和CanalMessageBean
>   - 如何解析JSON：使用`FastJson`类库，MessageBean使用Java语言，编写编码器`Encoder`
> - save方法：将转换后的DataFrame数据存储到Kudu表



## 03-[了解]-第6天：课程内容提纲 

> 继续编程实现：实时增量ETL存储Kudu表，具体示意图如下所示：
>
> - 1）、load 加载数据：从Kafka Topic中实时消费业务数据，就是Topic中每条消息Message，JSON字符串
> - 2）、process 转换数据：将消费获取JSON字符串进行解析处理转换
>   - 第一、解析JSON封装为Bean对象（JavaBean实例），方便提取字段数据
>     - OggMessageBean和CanalMessageBean
>   - ==第二、提供Bean字段，封装`POJO`对象==
>     - 从MessageBean对象中获取`table、data、opType`，封装到POJO对象中
>     - POJO对象对应业务系统中每张表的字段
> - 3）、==save 保存数据：将转换后数据保存至Kudu表==
>   - 首先判断Kudu表是否存在，如果不存在，默认创建表
>   - 按照OpType将数据保存到Kudu表
>     - 如果OpType是Insert，将数据插入到Kudu表
>     - 如果OpType是Delete，依据主键删除Kudu表数据
>     - 如果OpType是Update，更新Kudu表的数据

![1613738409032](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613738409032.png)



## 04-[理解]-实时ETL开发之转换POJO【思路】

> ​		前面已经将消费Kafka Message数据解析为Bean对象，分为OggMessageBean和CanalMessageBean对象，其中包含很多字段，但是仅仅关注核心字段：数据。
>
> - 1）、OGG采集数据，提取字段：table、==op_type（操作类型）和after/before（getValue）==。
>   - 依据table表名称，封装==op_type和data==数据到POJO对象中国

![1613786737898](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613786737898.png)



> - 2）、Canal 采集数据，提取字段：table、==type和data==
>   - 依据table表名称，将==type和data==封装到POJO对象中，

![1613786933699](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613786933699.png)

> 举例说明：以Canal采集数据为例

![1613787365424](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613787365424.png)



## 05-[掌握]-实时ETL开发之OggBean转换POJO编程

> ​		按照前面分析来看，业务系统每张表数据提取封装到POJO对象中，已经将物流Logistics系统和CRM系统业务数据表对应POJO创建完成。

![1613787489030](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613787489030.png)

> 根据具体的Table名称，将数据转换成对应的POJO对象，方便后续进行保存操作。
>
> - step1、创建TableMapping对象，定义属性（都是业务系统表名称）

```scala
package cn.itcast.logistics.common

/**
 * 定义表名：根据表的名字定义属性
 */
object TableMapping {
	
	// Logistics 物流系统表名称
	val AREAS: String = "tbl_areas"
	val CHARGE_STANDARD: String = "tbl_charge_standard"
	val CODES: String = "tbl_codes"
	val COLLECT_PACKAGE: String = "tbl_collect_package"
	val COMPANY: String = "tbl_company"
	val COMPANY_DOT_MAP: String = "tbl_company_dot_map"
	val COMPANY_TRANSPORT_ROUTE_MA: String = "tbl_company_transport_route_ma"
	val COMPANY_WAREHOUSE_MAP: String = "tbl_company_warehouse_map"
	val CONSUMER_SENDER_INFO: String = "tbl_consumer_sender_info"
	val COURIER: String = "tbl_courier"
	val DELIVER_PACKAGE: String = "tbl_deliver_package"
	val DELIVER_REGION: String = "tbl_deliver_region"
	val DELIVERY_RECORD: String = "tbl_delivery_record"
	val DEPARTMENT: String = "tbl_department"
	val DOT: String = "tbl_dot"
	val DOT_TRANSPORT_TOOL: String = "tbl_dot_transport_tool"
	val DRIVER: String = "tbl_driver"
	val EMP: String = "tbl_emp"
	val EMP_INFO_MAP: String = "tbl_emp_info_map"
	val EXPRESS_BILL: String = "tbl_express_bill"
	val EXPRESS_PACKAGE: String = "tbl_express_package"
	val FIXED_AREA: String = "tbl_fixed_area"
	val GOODS_RACK: String = "tbl_goods_rack"
	val JOB: String = "tbl_job"
	val OUT_WAREHOUSE: String = "tbl_out_warehouse"
	val OUT_WAREHOUSE_DETAIL: String = "tbl_out_warehouse_detail"
	val PKG: String = "tbl_pkg"
	val POSTAL_STANDARD: String = "tbl_postal_standard"
	val PUSH_WAREHOUSE: String = "tbl_push_warehouse"
	val PUSH_WAREHOUSE_DETAIL: String = "tbl_push_warehouse_detail"
	val ROUTE: String = "tbl_route"
	val SERVICE_EVALUATION: String = "tbl_service_evaluation"
	val STORE_GRID: String = "tbl_store_grid"
	val TRANSPORT_RECORD: String = "tbl_transport_record"
	val TRANSPORT_TOOL: String = "tbl_transport_tool"
	val VEHICLE_MONITOR: String = "tbl_vehicle_monitor"
	
	val WAREHOUSE: String = "tbl_warehouse"
	val WAREHOUSE_EMP: String = "tbl_warehouse_emp"
	val WAREHOUSE_RACK_MAP: String = "tbl_warehouse_rack_map"
	val WAREHOUSE_RECEIPT: String = "tbl_warehouse_receipt"
	val WAREHOUSE_RECEIPT_DETAIL: String = "tbl_warehouse_receipt_detail"
	val WAREHOUSE_SEND_VEHICLE: String = "tbl_warehouse_send_vehicle"
	val WAREHOUSE_TRANSPORT_TOOL: String = "tbl_warehouse_transport_tool"
	val WAREHOUSE_VEHICLE_MAP: String = "tbl_warehouse_vehicle_map"
	val WAY_BILL: String = "tbl_waybill"
	val WAYBILL_LINE: String = "tbl_waybill_line"
	val WAYBILL_STATE_RECORD: String = "tbl_waybill_state_record"
	val WORK_TIME: String = "tbl_work_time"
	
	// CRM 系统业务数据表名称
	val ADDRESS: String = "tbl_address"
	val CONSUMER_ADDRESS_MAP: String = "tbl_consumer_address_map"
	val CUSTOMER: String = "tbl_customer"
}
```

> - step2、修改KuduStreamApp中process方法，添加提取数据字段并解析为POJO代码

```scala
	/**
	 * 数据的处理
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	override def process(streamDF: DataFrame, category: String): DataFrame = {
		// 导入隐式转换
		import streamDF.sparkSession.implicits._
		
		// 1. 依据不同业务系统数据进行不同的ETL转换
		val etlStreamDF: DataFrame = category match {
			// TODO: 物流系统业务数据ETL转换
			case "logistics" =>
				// 第一步、TODO： a. 转换JSON为Bean对象
				val logisticsBeanStreamDS: Dataset[OggMessageBean] = streamDF
					.as[String] // 将DataFrame转换为Dataset
					.filter(json => null != json && json.trim.length > 0) // 过滤数据
					// 由于OggMessageBean是使用Java语言自定义类，所以需要自己指定编码器Encoder
					.mapPartitions { iter =>
						iter.map { json => JSON.parseObject(json, classOf[OggMessageBean]) }
					}(Encoders.bean(classOf[OggMessageBean]))
				// 第二步、TODO：b. 提取Bean数据字段，封装POJO
				/*
					以Logistics物流系统：tbl_areas为例，从Bean对象获取数据字段值，封装POJO【AreasBean】
				 */
				val oggEtlStreamDS: Dataset[AreasBean] = logisticsBeanStreamDS
					// 过滤获取AreasBean数据
					.filter(oggBean => oggBean.getTable == TableMapping.AREAS)
					// 提取数据字段值，进行封装为POJO
					.map(oggBean => DataParser.toAreas(oggBean))
				
				// 第三步、c. 返回转换后数据
				oggEtlStreamDS.toDF()
				
			// TODO：CRM系统业务数据ETL转换
			case "crm" =>
				// a. 转换JSON为Bean对象
				implicit val canalEncoder: Encoder[CanalMessageBean] = Encoders.bean(classOf[CanalMessageBean])
				val crmBeanStreamDS: Dataset[CanalMessageBean] = streamDF
					.filter(row => ! row.isNullAt(0))
					.mapPartitions{iter =>
						iter.map{row =>
							val json = row.getAs[String](0)
							JSON.parseObject(json, classOf[CanalMessageBean])
						}
					}
				// b. 返回转换后数据
				crmBeanStreamDS.toDF()
			
			// TODO: 其他业务数据数据，直接返回即可
			case _ => streamDF
		}
		// 2. 返回转换后流数据
		etlStreamDF
	}
```



## 06-[掌握]-实时ETL开发之转换POJO【数据解析器】

> ​	编写`DataParser`类中`toAreas`方法，提取数据字段值，封装到`AreasBean`对象中。

![1613789589716](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613789589716.png)

> - 1）、首先将Map集合转换为JSON字符串：`JSON.toJSONString`
> - 2）、将JSON字符串转换为Bean对象：`JSON.parseObject`
>
> [提取数据字段并封装为JOJO对象，核心代码如下：]()

```scala
package cn.itcast.logistics.etl.parser

import java.util

import cn.itcast.logistics.common.beans.logistics._
import cn.itcast.logistics.common.beans.parser._
import com.alibaba.fastjson.JSON

/**
 * 数据解析，将每张表的字段信息转换成JavaBean对象
 */
object DataParser {
	
	/** 判断messageBean是否是OggMessageBean*/
	private def getOggMessageBean(bean: MessageBean): OggMessageBean = {
		bean match {
			case ogg: OggMessageBean => ogg
		}
	}
	
	/**
	 * 提取ogg（I、U、D）和canal（insert、update、delete）数据的optype属性，转换成统一的操作字符串
	 *
	 * @param opType 数据操作类型：insert、update、delete，任意一种
	 */
	private def getOpType(opType: String): String = {
		opType match {
			case "I" => "insert"
			case "U" => "update"
			case "D" => "delete"
			case "INSERT" => "insert"
			case "UPDATE" => "update"
			case "DELETE" => "delete"
			case _ => "insert"
		}
	}
	
	// ================== 物流Logistics系统业务数据解析 ==================
	/*
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
	*/
	def toAreas(bean: MessageBean): AreasBean = {
		var pojo: AreasBean = null
		// 转换MessageBean为OggMessageBean对象
		val oggMessageBean: OggMessageBean = getOggMessageBean(bean)
		// 提取数据字段：调用getValue
		val dataMap: util.Map[String, AnyRef] = oggMessageBean.getValue
		// 转换Map集合为JSON字符串
		val dataJson: String = JSON.toJSONString(dataMap, true)
		// 转换JSON字符串为POJO对象
		pojo = JSON.parseObject(dataJson, classOf[AreasBean])
		if(null != pojo){
			// 设置操作类型
			pojo.setOpType(getOpType(oggMessageBean.getOp_type))
		}
		// 返回POJO对象
		pojo
	}
	
}

```

> 考虑：如果提取数据字段解析POJO不成功为null时，需要顾虑数据

```scala
		// 1. 依据不同业务系统数据进行不同的ETL转换
		val etlStreamDF: DataFrame = category match {
			// TODO: 物流系统业务数据ETL转换
			case "logistics" =>
				// 第一步、TODO： a. 转换JSON为Bean对象
				val beanStreamDS: Dataset[OggMessageBean] = streamDF
					.as[String] // 将DataFrame转换为Dataset
					.filter(json => null != json && json.trim.length > 0) // 过滤数据
					// 由于OggMessageBean是使用Java语言自定义类，所以需要自己指定编码器Encoder
					.mapPartitions { iter =>
						iter.map { json => JSON.parseObject(json, classOf[OggMessageBean]) }
					}(Encoders.bean(classOf[OggMessageBean]))
				// 第二步、TODO：b. 提取Bean数据字段，封装POJO
				/*
					以Logistics物流系统：tbl_areas为例，从Bean对象获取数据字段值，封装POJO【AreasBean】
				 */
				val pojoStreamDS: Dataset[AreasBean] = beanStreamDS
					// 过滤获取AreasBean数据
					.filter(oggBean => oggBean.getTable == TableMapping.AREAS)
					// 提取数据字段值，进行封装为POJO
					.map(oggBean => DataParser.toAreas(oggBean))
					// 过滤不为null
					.filter(pojo => null != pojo)
				// 第三步、c. 返回转换后数据
				pojoStreamDS.toDF()
```



## 07-[理解]-实时ETL开发之转换POJO【隐式转换】

> ​		当对Dataset进行数据转换操作（尤其调用Dataset API）时，如果数据类型不是SparkSQL默认支持，需要自己定义编码器Encoder，否则报错，如下所示：

![1613790461500](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613790461500.png)



> [SparkSQL中Encoder编码器使用有2种方式：]()
>
> - 1）、方式一、
>
> ![1613790516383](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613790516383.png)
>
> - 2）、方式二、
>
> ![1613790530892](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613790530892.png)
>
> ==但是，上述方式单独针对某个Dataset转换进行设置，然而物流项目中，不同表数据转换都需要Encoder编码器，需要定义很多，此时可以将编码器进行封装，自定义隐式转换。==
>
> [参考SparkSQL中`import spark.implicits._`中代码，创建对象BeanImplicts，定义隐式转换函数]()

```scala
package cn.itcast.logistics.common

import cn.itcast.logistics.common.beans.crm._
import cn.itcast.logistics.common.beans.logistics._
import cn.itcast.logistics.common.beans.parser._
import org.apache.spark.sql.{Encoder, Encoders}

/**
 * 扩展自定义POJO的隐式转换实现
 *
 */
object BeanImplicits {
	
	// 定义MessageBean隐式参数Encoder值
	implicit def newOggMessageBeanEncoder: Encoder[OggMessageBean] = Encoders.bean(classOf[OggMessageBean])
	implicit def newCanalMessageBeanEncoder: Encoder[CanalMessageBean] = Encoders.bean(classOf[CanalMessageBean])
	
	// Logistics Bean
	implicit def newAreasBeanEncoder: Encoder[AreasBean] = Encoders.bean(classOf[AreasBean])
	implicit def newChargeStandardBeanEncoder: Encoder[ChargeStandardBean] = Encoders.bean(classOf[ChargeStandardBean])
	implicit def newCodesBeanEncoder: Encoder[CodesBean] = Encoders.bean(classOf[CodesBean])
	implicit def newCollectPackageBeanEncoder: Encoder[CollectPackageBean] = Encoders.bean(classOf[CollectPackageBean])
	implicit def newCompanyBeanEncoder: Encoder[CompanyBean] = Encoders.bean(classOf[CompanyBean])
	implicit def newCompanyDotMapBeanEncoder: Encoder[CompanyDotMapBean] = Encoders.bean(classOf[CompanyDotMapBean])
	implicit def newCompanyTransportRouteMaBeanEncoder: Encoder[CompanyTransportRouteMaBean] = Encoders.bean(classOf[CompanyTransportRouteMaBean])
	implicit def newCompanyWarehouseMapBeanEncoder: Encoder[CompanyWarehouseMapBean] = Encoders.bean(classOf[CompanyWarehouseMapBean])
	implicit def newConsumerSenderInfoBeanEncoder: Encoder[ConsumerSenderInfoBean] = Encoders.bean(classOf[ConsumerSenderInfoBean])
	implicit def newCourierBeanEncoder: Encoder[CourierBean] = Encoders.bean(classOf[CourierBean])
	implicit def newDeliverPackageBeanEncoder: Encoder[DeliverPackageBean] = Encoders.bean(classOf[DeliverPackageBean])
	implicit def newDeliverRegionBeanEncoder: Encoder[DeliverRegionBean] = Encoders.bean(classOf[DeliverRegionBean])
	implicit def newDeliveryRecordBeanEncoder: Encoder[DeliveryRecordBean] = Encoders.bean(classOf[DeliveryRecordBean])
	implicit def newDepartmentBeanEncoder: Encoder[DepartmentBean] = Encoders.bean(classOf[DepartmentBean])
	implicit def newDotBeanEncoder: Encoder[DotBean] = Encoders.bean(classOf[DotBean])
	implicit def newDotTransportToolBeanEncoder: Encoder[DotTransportToolBean] = Encoders.bean(classOf[DotTransportToolBean])
	implicit def newDriverBeanEncoder: Encoder[DriverBean] = Encoders.bean(classOf[DriverBean])
	implicit def newEmpBeanEncoder: Encoder[EmpBean] = Encoders.bean(classOf[EmpBean])
	implicit def newEmpInfoMapBeanEncoder: Encoder[EmpInfoMapBean] = Encoders.bean(classOf[EmpInfoMapBean])
	implicit def newExpressBillBeanEncoder: Encoder[ExpressBillBean] = Encoders.bean(classOf[ExpressBillBean])
	implicit def newExpressPackageBeanEncoder: Encoder[ExpressPackageBean] = Encoders.bean(classOf[ExpressPackageBean])
	implicit def newFixedAreaBeanEncoder: Encoder[FixedAreaBean] = Encoders.bean(classOf[FixedAreaBean])
	implicit def newGoodsRackBeanEncoder: Encoder[GoodsRackBean] = Encoders.bean(classOf[GoodsRackBean])
	implicit def newJobBeanEncoder: Encoder[JobBean] = Encoders.bean(classOf[JobBean])
	implicit def newOutWarehouseBeanEncoder: Encoder[OutWarehouseBean] = Encoders.bean(classOf[OutWarehouseBean])
	implicit def newOutWarehouseDetailBeanEncoder: Encoder[OutWarehouseDetailBean] = Encoders.bean(classOf[OutWarehouseDetailBean])
	implicit def newPkgBeanEncoder: Encoder[PkgBean] = Encoders.bean(classOf[PkgBean])
	implicit def newPostalStandardBeanEncoder: Encoder[PostalStandardBean] = Encoders.bean(classOf[PostalStandardBean])
	implicit def newPushWarehouseBeanEncoder: Encoder[PushWarehouseBean] = Encoders.bean(classOf[PushWarehouseBean])
	implicit def newPushWarehouseDetailBeanEncoder: Encoder[PushWarehouseDetailBean] = Encoders.bean(classOf[PushWarehouseDetailBean])
	implicit def newRouteBeanEncoder: Encoder[RouteBean] = Encoders.bean(classOf[RouteBean])
	implicit def newServiceEvaluationBeanEncoder: Encoder[ServiceEvaluationBean] = Encoders.bean(classOf[ServiceEvaluationBean])
	implicit def newStoreGridBeanEncoder: Encoder[StoreGridBean] = Encoders.bean(classOf[StoreGridBean])
	implicit def newTransportToolBeanEncoder: Encoder[TransportToolBean] = Encoders.bean(classOf[TransportToolBean])
	implicit def newVehicleMonitorBeanEncoder: Encoder[VehicleMonitorBean] = Encoders.bean(classOf[VehicleMonitorBean])
	implicit def newWarehouseBeanEncoder: Encoder[WarehouseBean] = Encoders.bean(classOf[WarehouseBean])
	implicit def newWarehouseEmpBeanEncoder: Encoder[WarehouseEmpBean] = Encoders.bean(classOf[WarehouseEmpBean])
	implicit def newWarehouseRackMapBeanEncoder: Encoder[WarehouseRackMapBean] = Encoders.bean(classOf[WarehouseRackMapBean])
	implicit def newWarehouseReceiptBeanEncoder: Encoder[WarehouseReceiptBean] = Encoders.bean(classOf[WarehouseReceiptBean])
	implicit def newWarehouseReceiptDetailBeanEncoder: Encoder[WarehouseReceiptDetailBean] = Encoders.bean(classOf[WarehouseReceiptDetailBean])
	implicit def newWarehouseSendVehicleBeanEncoder: Encoder[WarehouseSendVehicleBean] = Encoders.bean(classOf[WarehouseSendVehicleBean])
	implicit def newWarehouseTransportToolBeanEncoder: Encoder[WarehouseTransportToolBean] = Encoders.bean(classOf[WarehouseTransportToolBean])
	implicit def newWarehouseVehicleMapBeanEncoder: Encoder[WarehouseVehicleMapBean] = Encoders.bean(classOf[WarehouseVehicleMapBean])
	implicit def newWaybillBeanEncoder: Encoder[WaybillBean] = Encoders.bean(classOf[WaybillBean])
	implicit def newWaybillLineBeanEncoder: Encoder[WaybillLineBean] = Encoders.bean(classOf[WaybillLineBean])
	implicit def newWaybillStateRecordBeanEncoder: Encoder[WaybillStateRecordBean] = Encoders.bean(classOf[WaybillStateRecordBean])
	implicit def newWorkTimeBeanEncoder: Encoder[WorkTimeBean] = Encoders.bean(classOf[WorkTimeBean])
	implicit def newTransportRecordBeanEncoder: Encoder[TransportRecordBean] = Encoders.bean(classOf[TransportRecordBean])
	
	// CRM Bean
	implicit def newCustomerBeanEncoder: Encoder[CustomerBean] = Encoders.bean(classOf[CustomerBean])
	implicit def newAddressBeanEncoder: Encoder[AddressBean] = Encoders.bean(classOf[AddressBean])
	implicit def newConsumerAddressMapBeanEncoder: Encoder[ConsumerAddressMapBean] = Encoders.bean(classOf[ConsumerAddressMapBean])
	
}

```



## 08-[掌握]-实时ETL开发之OggBean转换POJO测试

> ​		前面已经以OggMessageBean转换为对应表的POJO代码编写完成，准备环境，进行测试。
>
> - 1）、启动Zookeeper和Kafka集群
>
> ![1613791198389](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613791198389.png)
>
> - 2）、启动容器，运行Oracle数据库和OGG服务
>
>   ![1613791228425](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613791228425.png)
>
> - 3）、运行流式计算程序：`KuduStreamApp`
>
>   [可以注释掉CRM系统数据实时ETL代码，此时仅仅针对物流系统Logistics业务数据进行测试]()
>
>   ![1613791385559](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613791385559.png)
>
> - 4）、使用DBeave远程连接Oracle数据库，对表【`tbl_areas`】表的数据进行CUD操作，查看ETL转换结果：先更新update一条数据，再删除一条数据。
>
> ![1613791479585](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613791479585.png)



## 09-[掌握]-实时ETL开发之CanalBean转换POJO

> ​		按照前面将OggMessageBean提取数据字段值，封装转换为POJO对象思路，实现对CanalMessageBean对象提取数据并封装，[此处为【crm_address】表数据为例]()，核心代码如下

```scala
			// TODO：CRM系统业务数据ETL转换
			case "crm" =>
				// 第一步、TODO： a. 转换JSON为Bean对象
				implicit val canalEncoder: Encoder[CanalMessageBean] = Encoders.bean(classOf[CanalMessageBean])
				val beanStreamDS: Dataset[CanalMessageBean] = streamDF
					.filter(row => ! row.isNullAt(0))
					.mapPartitions{iter =>
						iter.map{row =>
							val json = row.getAs[String](0)
							JSON.parseObject(json, classOf[CanalMessageBean])
						}
					}
				// 第二步、TODO： b. Bean提取数据字段，封装为POJO对象
				import cn.itcast.logistics.common.BeanImplicits._
				val pojoStreamDS: Dataset[AddressBean] = beanStreamDS
					// 过滤获取地址信息表相关数据：tbl_address
					.filter(canalBean => canalBean.getTable == TableMapping.ADDRESS)
					// 提取数据字段，转换为POJO对象
					.map(canalBean => DataParser.toAddress(canalBean))
					.filter(pojo => null != pojo) // 过滤非空数据
				
				// 第三步、c. 返回转换后数据
				pojoStreamDS.toDF()
```

> 在DataParser对象中，编写方法toAddress提取数据并封装POJO对象，核心代码如下：

```scala
	// ================== 客户关系管理CRM系统业务数据解析 ==================
	// TODO: 将CRM系统业务数据：crm_address 表业务数据转换为POJO对象
	/*
		"data": [{
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
		}]
	*/
	def toAddress(bean: MessageBean): AddressBean = {
		// i. 转换对象为CanalMessageBean实例
		val canal: CanalMessageBean = getCanalMessageBean(bean)
		// ii. 使用fastJSON将Canal采集数据中【data】字段数据转换为JSON字符串
		val jsonStr: String = JSON.toJSONString(canal.getData, SerializerFeature.PrettyFormat)
		println(jsonStr)
		// iii. 使用fastJSON解析【data】字段数据值，返回列表List
		val list: java.util.List[AddressBean] = JSON.parseArray(jsonStr, classOf[AddressBean])
		// iv. 获取值，设置操作类型OpType
		var res: AddressBean = null
		if (!CollectionUtils.isEmpty(list)) {
			res = list.get(0)
			res.setOpType(getOpType(canal.getType))
		}
		// v. 返回解析的对象
		res
	}
```

> ​		由于Canal采集MySQL数据库表的数据时，将操作数据封装在data字段中，类型为列表，列表中类型为Map集合，所以解析时为List列表，需要判断是否为null，再进行相关设置。

​		运行`KuduStreamApp`应用程序，在DBeave远程连接数据库，对表【crm_address】表中数据更新和删除，查看流式应用程序打印控制台结果数据：

![1613793972456](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613793972456.png)



![1613793989832](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613793989832.png)



## 10-[掌握]-实时ETL开发之转换POJO【重构代码】

> ​		前面已经针对OggMessageBean和CanalMessageBean对象提取数据字段并封装POJO对象测试OK，接下来需要完成针对所有业务系统表的数据转换操作。
>
> [由于不同业务系统，有不同表，进行ETL转换时（Bean转换为POJO），需要进行单独处理，并且转换后需要进行保存外部存储，所以可以将【Bean转换POJO】和【保存数据】放在一起，针对每个系统数据进行封装重构。]()

![1613794425222](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613794425222.png)

```SCALA
package cn.itcast.logistics.etl.realtime

import cn.itcast.logistics.common.beans.crm.AddressBean
import cn.itcast.logistics.common.beans.logistics.AreasBean
import cn.itcast.logistics.common.beans.parser.{CanalMessageBean, OggMessageBean}
import cn.itcast.logistics.common.{Configuration, SparkUtils, TableMapping}
import cn.itcast.logistics.etl.parser.DataParser
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession}

/**
 * Kudu数据管道应用：实现Kudu数据库的实时ETL操作
 */
object KuduStreamApp extends BasicStreamApp {
	
	/**
	 * 数据的处理，将JSON字符串转换为Bean对象
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	override def process(streamDF: DataFrame, category: String): DataFrame = {
		// 导入隐式转换
		import streamDF.sparkSession.implicits._
		
		// 1. 依据不同业务系统数据进行不同的ETL转换
		val etlStreamDF: DataFrame = category match {
			// TODO: 物流系统业务数据ETL转换
			case "logistics" =>
				// 第一步、TODO： a. 转换JSON为Bean对象
				val beanStreamDS: Dataset[OggMessageBean] = streamDF
					.as[String] // 将DataFrame转换为Dataset
					.filter(json => null != json && json.trim.length > 0) // 过滤数据
					// 由于OggMessageBean是使用Java语言自定义类，所以需要自己指定编码器Encoder
					.mapPartitions { iter =>
						iter.map { json => JSON.parseObject(json, classOf[OggMessageBean]) }
					}(Encoders.bean(classOf[OggMessageBean]))
				
				// 第三步、c. 返回转换后数据
				beanStreamDS.toDF()
				
			// TODO：CRM系统业务数据ETL转换
			case "crm" =>
				// 第一步、TODO： a. 转换JSON为Bean对象
				implicit val canalEncoder: Encoder[CanalMessageBean] = Encoders.bean(classOf[CanalMessageBean])
				val beanStreamDS: Dataset[CanalMessageBean] = streamDF
					.filter(row => ! row.isNullAt(0))
					.mapPartitions{iter =>
						iter.map{row =>
							val json = row.getAs[String](0)
							JSON.parseObject(json, classOf[CanalMessageBean])
						}
					}
				// 第三步、c. 返回转换后数据
				beanStreamDS.toDF()
			
			// TODO: 其他业务数据数据，直接返回即可
			case _ => streamDF
		}
		// 2. 返回转换后流数据
		etlStreamDF
	}
	
	/**
	 * 数据的保存
	 *
	 * @param streamDF          保存数据集DataFrame
	 * @param tableName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	override def save(streamDF: DataFrame, tableName: String, isAutoCreateTable: Boolean): Unit = {
		streamDF.writeStream
			.outputMode(OutputMode.Append())
			.queryName(s"query-${tableName}")
			.format("console")
			.option("numRows", "10")
			.option("truncate", "false")
			.start()
	}
	
	/**
	 * 物流Logistics系统业务数据ETL转换处理及保存外部存储
	 *      1)、Bean -> POJO  2)、保存save
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 */
	def etlLogistics(streamDF: DataFrame): Unit = {
		
		// 第二步、TODO：b. 提取Bean数据字段，封装POJO
		/*
			以Logistics物流系统：tbl_areas为例，从Bean对象获取数据字段值，封装POJO【AreasBean】
		 */
		import cn.itcast.logistics.common.BeanImplicits._
		val areasStreamDS: Dataset[AreasBean] = streamDF
			.as[OggMessageBean]
			// 过滤获取AreasBean数据
			.filter(oggBean => oggBean.getTable == TableMapping.AREAS)
			// 提取数据字段值，进行封装为POJO
			.map(oggBean => DataParser.toAreas(oggBean))
			// 过滤不为null
			.filter(pojo => null != pojo)
		save(areasStreamDS.toDF(), "tbl_areas")
		
		
		
	}
	
	/**
	 * 客户管理管理CRM系统业务数据ETL转换处理及保存外部存储
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 */
	def etlCrm(streamDF: DataFrame): Unit = {
		
		import cn.itcast.logistics.common.BeanImplicits._
		
		// 第二步、TODO： b. Bean提取数据字段，封装为POJO对象
		val addressStreamDS: Dataset[AddressBean] = streamDF
			.as[CanalMessageBean]
			// 过滤获取地址信息表相关数据：tbl_address
			.filter(canalBean => canalBean.getTable == TableMapping.ADDRESS)
			// 提取数据字段，转换为POJO对象
			.map(canalBean => DataParser.toAddress(canalBean))
			.filter(pojo => null != pojo) // 过滤非空数据
		save(addressStreamDS.toDF(), "tbl_address")
		
	}
	
	/** MAIN 方法入口：Spark Application应用程序入口，必须创建SparkContext对象或SpakrSession对象 */
	def main(args: Array[String]): Unit = {
		// step1. 获取SparkSession对象
		val spark: SparkSession = SparkUtils.createSparkSession(
			SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass
		)
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		import spark.implicits._
		
		// step2. 从Kafka加载数据：物流系统业务数据logistics和CRM系统业务数据crm
		val logisticsStreamDF: DataFrame = load(spark, "logistics")
		val crmStreamDF: DataFrame = load(spark, "crm")
		
		// step3. 对流式数据进行实时ETL转换操作（解析JSON，封装Bean对象）
		val logisticsEtlStreamDF = process(logisticsStreamDF, "logistics")
		val crmEtlStreamDF = process(crmStreamDF, "crm")
		
		// step4. 将转换后数据进行输出
		//save(logisticsEtlStreamDF, "tbl_logistics")
		//save(crmEtlStreamDF, "tbl_crm")
		
		/**
		 * TODO：针对不同业务系统，提供不同方法，每个方法中针对不同表进行2个操作：
		 * 1）、Bean 转换POJO对象
		 * 2）、POJO对象数据保存
		 */
		etlLogistics(logisticsEtlStreamDF)
		etlCrm(crmEtlStreamDF)
		
		// step5. 启动流式应用后，等待终止
		spark.streams.active.foreach(query => println(s"正在启动Query： ${query.name}"))
		spark.streams.awaitAnyTermination()
	}
}

```



## 11-[掌握]-实时ETL开发之Bean转换POJO【编程测试】

> ​		在不同业务系统数据转换方法中，补充代码（不同业务表数据转换为不同POJO对象）。
>
> - 物流系统Logistics数据转换：`etlLogistics`方法

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
		
		val warehouseSendVehicleStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_SEND_VEHICLE)
			.map(bean => DataParser.toWarehouseSendVehicle(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseSendVehicleStreamDF, TableMapping.WAREHOUSE_SEND_VEHICLE)
		
		val waybillLineStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAYBILL_LINE)
			.map(bean => DataParser.toWaybillLine(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(waybillLineStreamDF, TableMapping.WAYBILL_LINE)
		
		val chargeStandardStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.CHARGE_STANDARD)
			.map(bean => DataParser.toChargeStandard(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(chargeStandardStreamDF, TableMapping.CHARGE_STANDARD)
		
		val codesStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.CODES)
			.map(bean => DataParser.toCodes(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(codesStreamDF, TableMapping.CODES)
		
		val collectPackageStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COLLECT_PACKAGE)
			.map(bean => DataParser.toCollectPackage(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(collectPackageStreamDF, TableMapping.COLLECT_PACKAGE)
		
		val companyStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COMPANY)
			.map(bean => DataParser.toCompany(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(companyStreamDF, TableMapping.COMPANY)
		
		val companyDotMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COMPANY_DOT_MAP)
			.map(bean => DataParser.toCompanyDotMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(companyDotMapStreamDF, TableMapping.COMPANY_DOT_MAP)
		
		val companyTransportRouteMaStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COMPANY_TRANSPORT_ROUTE_MA)
			.map(bean => DataParser.toCompanyTransportRouteMa(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(companyTransportRouteMaStreamDF, TableMapping.COMPANY_TRANSPORT_ROUTE_MA)
		
		val companyWarehouseMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COMPANY_WAREHOUSE_MAP)
			.map(bean => DataParser.toCompanyWarehouseMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(companyWarehouseMapStreamDF, TableMapping.COMPANY_WAREHOUSE_MAP)
		
		val consumerSenderInfoStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.CONSUMER_SENDER_INFO)
			.map(bean => DataParser.toConsumerSenderInfo(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(consumerSenderInfoStreamDF, TableMapping.CONSUMER_SENDER_INFO)
		
		val courierStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COURIER)
			.map(bean => DataParser.toCourier(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(courierStreamDF, TableMapping.COURIER)
		
		val deliverPackageStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DELIVER_PACKAGE)
			.map(bean => DataParser.toDeliverPackage(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(deliverPackageStreamDF, TableMapping.DELIVER_PACKAGE)
		
		val deliverRegionStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DELIVER_REGION)
			.map(bean => DataParser.toDeliverRegion(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(deliverRegionStreamDF, TableMapping.DELIVER_REGION)
		
		val deliveryRecordStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DELIVERY_RECORD)
			.map(bean => DataParser.toDeliveryRecord(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(deliveryRecordStreamDF, TableMapping.DELIVERY_RECORD)
		
		val departmentStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DEPARTMENT)
			.map(bean => DataParser.toDepartment(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(departmentStreamDF, TableMapping.DEPARTMENT)
		
		val dotStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DOT)
			.map(bean => DataParser.toDot(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(dotStreamDF, TableMapping.DOT)
		
		val dotTransportToolStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DOT_TRANSPORT_TOOL)
			.map(bean => DataParser.toDotTransportTool(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(dotTransportToolStreamDF, TableMapping.DOT_TRANSPORT_TOOL)
		
		val driverStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DRIVER)
			.map(bean => DataParser.toDriver(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(driverStreamDF, TableMapping.DRIVER)
		
		val empStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EMP)
			.map(bean => DataParser.toEmp(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(empStreamDF, TableMapping.EMP)
		
		val empInfoMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EMP_INFO_MAP)
			.map(bean => DataParser.toEmpInfoMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(empInfoMapStreamDF, TableMapping.EMP_INFO_MAP)
		
		val expressBillStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EXPRESS_BILL)
			.map(bean => DataParser.toExpressBill(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(expressBillStreamDF, TableMapping.EXPRESS_BILL)
		
		val expressPackageStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EXPRESS_PACKAGE)
			.map(bean => DataParser.toExpressPackage(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(expressPackageStreamDF, TableMapping.EXPRESS_PACKAGE)
		
		val fixedAreaStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.FIXED_AREA)
			.map(bean => DataParser.toFixedArea(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(fixedAreaStreamDF, TableMapping.FIXED_AREA)
		
		val goodsRackStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.GOODS_RACK)
			.map(bean => DataParser.toGoodsRack(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(goodsRackStreamDF, TableMapping.GOODS_RACK)
		
		val jobStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.JOB)
			.map(bean => DataParser.toJob(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(jobStreamDF, TableMapping.JOB)
		
		val outWarehouseStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.OUT_WAREHOUSE)
			.map(bean => DataParser.toOutWarehouse(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(outWarehouseStreamDF, TableMapping.OUT_WAREHOUSE)
		
		val outWarehouseDetailStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.OUT_WAREHOUSE_DETAIL)
			.map(bean => DataParser.toOutWarehouseDetail(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(outWarehouseDetailStreamDF, TableMapping.OUT_WAREHOUSE_DETAIL)
		
		val pkgStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.PKG)
			.map(bean => DataParser.toPkg(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(pkgStreamDF, TableMapping.PKG)
		
		val postalStandardStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.POSTAL_STANDARD)
			.map(bean => DataParser.toPostalStandard(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(postalStandardStreamDF, TableMapping.POSTAL_STANDARD)
		
		val pushWarehouseStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.PUSH_WAREHOUSE)
			.map(bean => DataParser.toPushWarehouse(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(pushWarehouseStreamDF, TableMapping.PUSH_WAREHOUSE)
		
		val pushWarehouseDetailStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.PUSH_WAREHOUSE_DETAIL)
			.map(bean => DataParser.toPushWarehouseDetail(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(pushWarehouseDetailStreamDF, TableMapping.PUSH_WAREHOUSE_DETAIL)
		
		val routeStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.ROUTE)
			.map(bean => DataParser.toRoute(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(routeStreamDF, TableMapping.ROUTE)
		
		val serviceEvaluationStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.SERVICE_EVALUATION)
			.map(bean => DataParser.toServiceEvaluation(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(serviceEvaluationStreamDF, TableMapping.SERVICE_EVALUATION)
		
		val storeGridStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.STORE_GRID)
			.map(bean => DataParser.toStoreGrid(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(storeGridStreamDF, TableMapping.STORE_GRID)
		
		val transportToolStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.TRANSPORT_TOOL)
			.map(bean => DataParser.toTransportTool(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(transportToolStreamDF, TableMapping.TRANSPORT_TOOL)
		
		val vehicleMonitorStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.VEHICLE_MONITOR)
			.map(bean => DataParser.toVehicleMonitor(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(vehicleMonitorStreamDF, TableMapping.VEHICLE_MONITOR)
		
		val warehouseStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE)
			.map(bean => DataParser.toWarehouse(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseStreamDF, TableMapping.WAREHOUSE)
		
		val warehouseEmpStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_EMP)
			.map(bean => DataParser.toWarehouseEmp(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseEmpStreamDF, TableMapping.WAREHOUSE_EMP)
		
		val warehouseRackMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_RACK_MAP)
			.map(bean => DataParser.toWarehouseRackMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseRackMapStreamDF, TableMapping.WAREHOUSE_RACK_MAP)
		
		val warehouseReceiptStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_RECEIPT)
			.map(bean => DataParser.toWarehouseReceipt(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseReceiptStreamDF, TableMapping.WAREHOUSE_RECEIPT)
		
		val warehouseReceiptDetailStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_RECEIPT_DETAIL)
			.map(bean => DataParser.toWarehouseReceiptDetail(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseReceiptDetailStreamDF, TableMapping.WAREHOUSE_RECEIPT_DETAIL)
		
		val warehouseTransportToolStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_TRANSPORT_TOOL)
			.map(bean => DataParser.toWarehouseTransportTool(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseTransportToolStreamDF, TableMapping.WAREHOUSE_TRANSPORT_TOOL)
		
		val warehouseVehicleMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_VEHICLE_MAP)
			.map(bean => DataParser.toWarehouseVehicleMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseVehicleMapStreamDF, TableMapping.WAREHOUSE_VEHICLE_MAP)
		
		val waybillStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAY_BILL)
			.map(bean => DataParser.toWaybill(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(waybillStreamDF, TableMapping.WAY_BILL)
		
		val waybillStateRecordStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAYBILL_STATE_RECORD)
			.map(bean => DataParser.toWaybillStateRecord(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(waybillStateRecordStreamDF, TableMapping.WAYBILL_STATE_RECORD)
		
		val workTimeStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WORK_TIME)
			.map(bean => DataParser.toWorkTime(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(workTimeStreamDF, TableMapping.WORK_TIME)
		
		val transportRecordStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.TRANSPORT_RECORD)
			.map(bean => DataParser.toTransportRecordBean(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(transportRecordStreamDF, TableMapping.TRANSPORT_RECORD)
	}

```



> - CRM系统数据转换：`etlCrm`方法

```scala
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
		
		// Customer 表数据
		val customerStreamDS: Dataset[CustomerBean] = canalBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.CUSTOMER)
			.map(bean => DataParser.toCustomer(bean))
			.filter( pojo => null != pojo)
		save(customerStreamDS.toDF(), TableMapping.CUSTOMER)
		
		// ConsumerAddressMap 表数据
		val consumerAddressMapStreamDS: Dataset[ConsumerAddressMapBean] = canalBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.CONSUMER_ADDRESS_MAP)
			.map(bean => DataParser.toConsumerAddressMap(bean))
			.filter( pojo => null != pojo)
		save(consumerAddressMapStreamDS.toDF(), TableMapping.CONSUMER_ADDRESS_MAP)
	}
```



> 此外，需要在数据解析器DataParser中添加转换Bean对象为Pojo方法，代码如下：

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
		
		val warehouseSendVehicleStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_SEND_VEHICLE)
			.map(bean => DataParser.toWarehouseSendVehicle(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseSendVehicleStreamDF, TableMapping.WAREHOUSE_SEND_VEHICLE)
		
		val waybillLineStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAYBILL_LINE)
			.map(bean => DataParser.toWaybillLine(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(waybillLineStreamDF, TableMapping.WAYBILL_LINE)
		
		val chargeStandardStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.CHARGE_STANDARD)
			.map(bean => DataParser.toChargeStandard(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(chargeStandardStreamDF, TableMapping.CHARGE_STANDARD)
		
		val codesStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.CODES)
			.map(bean => DataParser.toCodes(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(codesStreamDF, TableMapping.CODES)
		
		val collectPackageStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COLLECT_PACKAGE)
			.map(bean => DataParser.toCollectPackage(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(collectPackageStreamDF, TableMapping.COLLECT_PACKAGE)
		
		val companyStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COMPANY)
			.map(bean => DataParser.toCompany(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(companyStreamDF, TableMapping.COMPANY)
		
		val companyDotMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COMPANY_DOT_MAP)
			.map(bean => DataParser.toCompanyDotMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(companyDotMapStreamDF, TableMapping.COMPANY_DOT_MAP)
		
		val companyTransportRouteMaStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COMPANY_TRANSPORT_ROUTE_MA)
			.map(bean => DataParser.toCompanyTransportRouteMa(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(companyTransportRouteMaStreamDF, TableMapping.COMPANY_TRANSPORT_ROUTE_MA)
		
		val companyWarehouseMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COMPANY_WAREHOUSE_MAP)
			.map(bean => DataParser.toCompanyWarehouseMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(companyWarehouseMapStreamDF, TableMapping.COMPANY_WAREHOUSE_MAP)
		
		val consumerSenderInfoStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.CONSUMER_SENDER_INFO)
			.map(bean => DataParser.toConsumerSenderInfo(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(consumerSenderInfoStreamDF, TableMapping.CONSUMER_SENDER_INFO)
		
		val courierStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.COURIER)
			.map(bean => DataParser.toCourier(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(courierStreamDF, TableMapping.COURIER)
		
		val deliverPackageStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DELIVER_PACKAGE)
			.map(bean => DataParser.toDeliverPackage(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(deliverPackageStreamDF, TableMapping.DELIVER_PACKAGE)
		
		val deliverRegionStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DELIVER_REGION)
			.map(bean => DataParser.toDeliverRegion(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(deliverRegionStreamDF, TableMapping.DELIVER_REGION)
		
		val deliveryRecordStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DELIVERY_RECORD)
			.map(bean => DataParser.toDeliveryRecord(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(deliveryRecordStreamDF, TableMapping.DELIVERY_RECORD)
		
		val departmentStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DEPARTMENT)
			.map(bean => DataParser.toDepartment(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(departmentStreamDF, TableMapping.DEPARTMENT)
		
		val dotStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DOT)
			.map(bean => DataParser.toDot(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(dotStreamDF, TableMapping.DOT)
		
		val dotTransportToolStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DOT_TRANSPORT_TOOL)
			.map(bean => DataParser.toDotTransportTool(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(dotTransportToolStreamDF, TableMapping.DOT_TRANSPORT_TOOL)
		
		val driverStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.DRIVER)
			.map(bean => DataParser.toDriver(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(driverStreamDF, TableMapping.DRIVER)
		
		val empStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EMP)
			.map(bean => DataParser.toEmp(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(empStreamDF, TableMapping.EMP)
		
		val empInfoMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EMP_INFO_MAP)
			.map(bean => DataParser.toEmpInfoMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(empInfoMapStreamDF, TableMapping.EMP_INFO_MAP)
		
		val expressBillStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EXPRESS_BILL)
			.map(bean => DataParser.toExpressBill(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(expressBillStreamDF, TableMapping.EXPRESS_BILL)
		
		val expressPackageStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EXPRESS_PACKAGE)
			.map(bean => DataParser.toExpressPackage(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(expressPackageStreamDF, TableMapping.EXPRESS_PACKAGE)
		
		val fixedAreaStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.FIXED_AREA)
			.map(bean => DataParser.toFixedArea(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(fixedAreaStreamDF, TableMapping.FIXED_AREA)
		
		val goodsRackStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.GOODS_RACK)
			.map(bean => DataParser.toGoodsRack(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(goodsRackStreamDF, TableMapping.GOODS_RACK)
		
		val jobStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.JOB)
			.map(bean => DataParser.toJob(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(jobStreamDF, TableMapping.JOB)
		
		val outWarehouseStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.OUT_WAREHOUSE)
			.map(bean => DataParser.toOutWarehouse(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(outWarehouseStreamDF, TableMapping.OUT_WAREHOUSE)
		
		val outWarehouseDetailStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.OUT_WAREHOUSE_DETAIL)
			.map(bean => DataParser.toOutWarehouseDetail(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(outWarehouseDetailStreamDF, TableMapping.OUT_WAREHOUSE_DETAIL)
		
		val pkgStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.PKG)
			.map(bean => DataParser.toPkg(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(pkgStreamDF, TableMapping.PKG)
		
		val postalStandardStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.POSTAL_STANDARD)
			.map(bean => DataParser.toPostalStandard(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(postalStandardStreamDF, TableMapping.POSTAL_STANDARD)
		
		val pushWarehouseStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.PUSH_WAREHOUSE)
			.map(bean => DataParser.toPushWarehouse(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(pushWarehouseStreamDF, TableMapping.PUSH_WAREHOUSE)
		
		val pushWarehouseDetailStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.PUSH_WAREHOUSE_DETAIL)
			.map(bean => DataParser.toPushWarehouseDetail(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(pushWarehouseDetailStreamDF, TableMapping.PUSH_WAREHOUSE_DETAIL)
		
		val routeStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.ROUTE)
			.map(bean => DataParser.toRoute(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(routeStreamDF, TableMapping.ROUTE)
		
		val serviceEvaluationStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.SERVICE_EVALUATION)
			.map(bean => DataParser.toServiceEvaluation(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(serviceEvaluationStreamDF, TableMapping.SERVICE_EVALUATION)
		
		val storeGridStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.STORE_GRID)
			.map(bean => DataParser.toStoreGrid(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(storeGridStreamDF, TableMapping.STORE_GRID)
		
		val transportToolStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.TRANSPORT_TOOL)
			.map(bean => DataParser.toTransportTool(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(transportToolStreamDF, TableMapping.TRANSPORT_TOOL)
		
		val vehicleMonitorStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.VEHICLE_MONITOR)
			.map(bean => DataParser.toVehicleMonitor(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(vehicleMonitorStreamDF, TableMapping.VEHICLE_MONITOR)
		
		val warehouseStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE)
			.map(bean => DataParser.toWarehouse(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseStreamDF, TableMapping.WAREHOUSE)
		
		val warehouseEmpStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_EMP)
			.map(bean => DataParser.toWarehouseEmp(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseEmpStreamDF, TableMapping.WAREHOUSE_EMP)
		
		val warehouseRackMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_RACK_MAP)
			.map(bean => DataParser.toWarehouseRackMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseRackMapStreamDF, TableMapping.WAREHOUSE_RACK_MAP)
		
		val warehouseReceiptStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_RECEIPT)
			.map(bean => DataParser.toWarehouseReceipt(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseReceiptStreamDF, TableMapping.WAREHOUSE_RECEIPT)
		
		val warehouseReceiptDetailStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_RECEIPT_DETAIL)
			.map(bean => DataParser.toWarehouseReceiptDetail(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseReceiptDetailStreamDF, TableMapping.WAREHOUSE_RECEIPT_DETAIL)
		
		val warehouseTransportToolStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_TRANSPORT_TOOL)
			.map(bean => DataParser.toWarehouseTransportTool(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseTransportToolStreamDF, TableMapping.WAREHOUSE_TRANSPORT_TOOL)
		
		val warehouseVehicleMapStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_VEHICLE_MAP)
			.map(bean => DataParser.toWarehouseVehicleMap(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(warehouseVehicleMapStreamDF, TableMapping.WAREHOUSE_VEHICLE_MAP)
		
		val waybillStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAY_BILL)
			.map(bean => DataParser.toWaybill(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(waybillStreamDF, TableMapping.WAY_BILL)
		
		val waybillStateRecordStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAYBILL_STATE_RECORD)
			.map(bean => DataParser.toWaybillStateRecord(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(waybillStateRecordStreamDF, TableMapping.WAYBILL_STATE_RECORD)
		
		val workTimeStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WORK_TIME)
			.map(bean => DataParser.toWorkTime(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(workTimeStreamDF, TableMapping.WORK_TIME)
		
		val transportRecordStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.TRANSPORT_RECORD)
			.map(bean => DataParser.toTransportRecordBean(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(transportRecordStreamDF, TableMapping.TRANSPORT_RECORD)
	}

```

> 运行流式计算程序：KuduStreamApp，模拟更新和删除数据，查看是否消费及转换OK。



## 12-[掌握]-实时ETL开发之保存Kudu表【save方法】

> ​		前面已经开发完成，数据实时ETL转换（JSON -> Bean -> POJO），接下来将转换后的数据保存到Kudu表中，需要在`save`方法中实现代码。

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
		// step1. 判断是否需要创建表（当表不存在时）
		if(isAutoCreateTable){
			// TODO: 判断Kudu中表是否存在，如果不存在，创建表
			KuduTools.createKuduTable(tableName)
		}
		
		// step2. 保存数据至Kudu表
		streamDF.writeStream
    		.outputMode(OutputMode.Append())
    		.queryName(s"query-kudu-${tableName}")
    		.format(Configuration.SPARK_KUDU_FORMAT)
    		.option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
    		.option("kudu.table", tableName)
    		.option("kudu.operation", "upsert")
    		.option("kudu.socketReadTimeoutMs", "10000")
    		.start()
	}
```

> 将流式StreamDataFrame保存至Kudu之前，先判断表是否存在，如果允许创建创建表。

```scala
package cn.itcast.logistics.common

object KuduTools {
	
	def createKuduTable(tableName: String) = ???
	
}

```



## 13-[理解]-实时ETL开发之保存Kudu表【KuduTools】

> 实现表是否存在的判断逻辑，如果表不存在则在kudu数据库中创建表：

![1613804987222](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613804987222.png)

> 具体代码实现如下，其中细节如下：
>
> - 1）、KuduContext构建，需要2个参数：KuduMasterRPCAddress和SparkContext
>
>   - 可以从DataFrame中获取SparkSession实例，再次获取SparkContext对象
>
> - 2）、Kudu表创建时，`Schema`约束要求主键字段不能为空，所以不能直接使用`DataFrame.schema`
>
> - 3）、分区策略时，设置字段必须是主键或者主键字段
>
>   - Scala集合对象转换为Java集合对象
>
>   ```scala
>   import scala.collection.JavaConverters._
>   ```
>
> - 4）、直接调用KuduContext中方法创建表即可，先判断表不存在，再创建

```scala
package cn.itcast.logistics.common

import java.util

import org.apache.kudu.client
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Kudu操作的工具类：创建表及其他操作
 */
object KuduTools {
	
	/**
	 * 创建Kudu表，创建Kudu表的约束？
	 * 1. Kudu表中必须要有一个主键列，更新数据和删除数据都需要根据主键更新，应该使用哪个列作为主键？id
	 * 2. Kudu表的字段有哪些？oracle和mysql的表有哪些字段，Kudu表就有哪些字段
	 * 3. kudu表的名字是什么？使用数据中传递的表明作为Kudu的表名
	 */
	def createKuduTable(tableName: String, streamDF: DataFrame,
	                    keys: Seq[String] = Seq("id")): Unit = {
		/**
		 * 实现步骤：
		 * step1. 获取Kudu的上下文对象：KuduContext
		 * step2. 判断Kudu中是否存在这张表，如果不存在则创建
		 * step3. 生成Kudu表的结构信息
		 * step4. 设置表的分区策略和副本数目
		 * step5. 创建表
		 */
		// step1. 构建KuduContext实例对象
		val kuduContext = new KuduContext(Configuration.KUDU_RPC_ADDRESS, streamDF.sparkSession.sparkContext)
		
		// step2. 判断Kudu中是否存在这张表，如果不存在则创建
		if(kuduContext.tableExists(tableName)){
			println(s"Kudu中表【${tableName}】已经存在，无需创建")
			return
		}
		
		/*
		  def createTable(
		      tableName: String,
		      schema: StructType,
		      keys: Seq[String],
		      options: CreateTableOptions
		  ): KuduTable
		 */
		// step3. 生成Kudu表的结构信息, 不能直接使用DataFrame中schema设置，需要设置主键列不为null
		val schema: StructType = StructType(
			streamDF.schema.fields.map{field =>
				StructField(
					field.name, //
					field.dataType, //
					nullable = if(keys.contains(field.name)) false else true
				)
			}
		)
		
		// step4. 设置Kudu表分区策略和副本数目
		val options: CreateTableOptions = new CreateTableOptions()
		options.setNumReplicas(1)
		// 将Scala集合对象与Java集合对象相互转换
		import scala.collection.JavaConverters._
		options.addHashPartitions(keys.asJava, 3)
		
		// step5. 传递参数，创建表
		val kuduTable = kuduContext.createTable(tableName, schema, keys, options)
		println(s"Kudu Table ID: ${kuduTable.getTableId}")
	}
	
}

```



## 14-[掌握]-实时ETL开发之保存Kudu表【CRM数据测试】

> ​		前面已经将保存数据save方法实现，并且可以自动创建Kudu表，以CRM系统数据进行测试。
>
> [首先需要启动Kudu服务：KuduMaster和Kudu TabletServer，使用CM启动]()

![1613807771120](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613807771120.png)

> 使用KuduPlus可视化工具连接Kudu，截图如下：

![1613807937039](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613807937039.png)

- 插入数据


```SQL
-- 插入数据INSERT
INSERT INTO `crm_address` VALUES ('10001', '葛秋红', null, '17*******47', '恒大影城南侧小金庄', '130903', null, '2020-02-02 18:51:39', '2020-02-02 18:51:39', null);
```

![1613808005449](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613808005449.png)



- 更新数据，Kudu表数据如下：

![1613808063305](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613808063305.png)



- 删除数据，Kudu表数据如下：

![1613808113941](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613808113941.png)



> 测试发现，当删除`Delete`数据，没有真正的删除数据，而是插入`Insert`数据。
>
> [为什么会是这样呢？？？？问题出现在哪里呢？？？？？]()

![1613808271066](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613808271066.png)

> 由于设置属性【`kudu.operation`】值为upsert（插入或更新），不会删除Kudu表数据。此外，发现Kudu表中多字段【opType】多余的，表示此条数据操作类型：Insert、Update还是Delete。
>
> [`opType`字段值，确定`kudu.operation`属性的值：]()
>
> - 1）、当opType为`insert`或`update`时，kudu.operation值为`upsert`
> - 2）、当opType为delete时，kudu.operation值为`delete`



## 15-[掌握]-实时ETL开发之保存Kudu表【opType优化】

> ​		上述直接保存数据至Kudu表示，没有对OpType字段信息进行处理，应该依据OpType字段的值，确定如何将数据保存至Kudu表，如果是INSERT和UPDATE，将数据插入更新Kudu表；如果是DELETE，获取主键，删除Kudu表的数据。

![1613808690066](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613808690066.png)

> 具体实现代码如下所示：
>
> - step1、在Kudu中创建表时，删除opType字段信息

```scala
		// step1. 判断是否需要创建表（当表不存在时）
		if(isAutoCreateTable){
			// TODO: 判断Kudu中表是否存在，如果不存在，创建表
			KuduTools.createKuduTable(odsTableName, streamDF.drop("opType"))
		}
```

> - step2、按照opType将StreamDataFrame分为2个部分，分别保存到Kudu表

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
		val odsTableName: String = s"ods_${tableName}"
		
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



## 16-[掌握]-实时ETL开发之保存Kudu表【实时数据测试】

> ​	接下来，模拟产生数据测试应用程序，仅仅针对CRM系统业务数据测试：
>
> - 1）、手动插入数据、更新数据、删除数据

插入数据或者更新数据，查看Kudu表数据：

![1613809590812](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613809590812.png)

删除数据：id=420，平安数据

![1613809640099](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613809640099.png)



> - 2）、运行数据生成器程序【`MockCrmDataApp`】，进行插入数据
>
> [MockCrmDataApp程序先清空数据，再插入数据]()

![1613809844918](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613809844918.png)

当流式计算程序运行以后，在启动数据模拟生成器

![1613809919488](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1613809919488.png)

> ​			分别运行CRM系统和Logistics物流系统数据模拟生成器，实时产生数据，ETL存在值Kudu表，便于后续离线报表开发和即席查询使用。













