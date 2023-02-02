package cn.itcast.logistics.etl.realtime

import cn.itcast.logistics.common.beans.crm.{AddressBean, ConsumerAddressMapBean, CustomerBean}
import cn.itcast.logistics.common.beans.logistics.AreasBean
import cn.itcast.logistics.common.beans.parser.{CanalMessageBean, OggMessageBean}
import cn.itcast.logistics.common.{Configuration, KuduTools, SparkUtils, TableMapping}
import cn.itcast.logistics.etl.parser.DataParser
import com.alibaba.fastjson.JSON
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql._

/**
 * Kudu数据管道应用：实现Kudu数据库的实时ETL操作
 */
object KuduStreamApp extends BasicStreamApp {
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
		
		val etlStreamDF: DataFrame = category match {
			// TODO： 物流业务系统数据ETL处理
			case "logistics" => {
				// 第一步、对获取OGG同步物流数据，转换封装MessageBean对象
				val oggBeanStreamDS: Dataset[OggMessageBean] = streamDF
					// 将DataFrame转换为Dataset，由于DataFrame中只有一个字段value，类型是String
					.as[String]
					// 对Dataset中每个分区数据进行操作，每个分区数据封装在迭代器中
					.mapPartitions{iter =>
						iter
							.filter(jsonStr => null != jsonStr && jsonStr.trim.length > 0 ) // 过滤数据
							// 解析JSON格式数据, 使用FastJson类库
							.map(jsonStr => JSON.parseObject(jsonStr, classOf[OggMessageBean]) )
					}(Encoders.bean(classOf[OggMessageBean]))
				// 第三步、返回转换后的数据
				oggBeanStreamDS.toDF()
			}
			// TODO： CRM业务系统数据ETL处理
			case "crm" => {
				// 第一步、对获取Canal同步CRM数据，转换封装MessageBean对象
				implicit val crmEncoder: Encoder[CanalMessageBean] = Encoders.bean(classOf[CanalMessageBean])
				val canalBeanStreamDS: Dataset[CanalMessageBean] = streamDF
					.filter(row => ! row.isNullAt(0))
					.mapPartitions{iter =>
						iter.map{row =>
							// 获取JSON值
							val jsonStr: String = row.getAs[String](0)
							// 解析JSON数据为JavaBean对象
							JSON.parseObject(jsonStr, classOf[CanalMessageBean])
						}
					}
				// 第三步、返回转换后的数据
				canalBeanStreamDS.toDF()
			}
			// 其他业务数据ETL处理
			case _ => streamDF
		}
		
		// 返回ETL后数据集
		etlStreamDF
	}
	
	/**
	 * 数据的处理，将Kafka获取Message解析封装值Bean对象，并提取数据封装至POJO对象
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	def processToPojo(streamDF: DataFrame, category: String): DataFrame = {
		// 导入隐式转换
		import streamDF.sparkSession.implicits._
		
		val etlStreamDF: DataFrame = category match {
			// TODO： 物流业务系统数据ETL处理
			case "logistics" => {
				// 第一步、对获取OGG同步物流数据，转换封装MessageBean对象
				val oggBeanStreamDS: Dataset[OggMessageBean] = streamDF
					// 将DataFrame转换为Dataset，由于DataFrame中只有一个字段value，类型是String
					.as[String]
					// 对Dataset中每个分区数据进行操作，每个分区数据封装在迭代器中
					.mapPartitions{iter =>
						iter
							.filter(jsonStr => null != jsonStr && jsonStr.trim.length > 0 ) // 过滤数据
							// 解析JSON格式数据, 使用FastJson类库
							.map(jsonStr => JSON.parseObject(jsonStr, classOf[OggMessageBean]) )
					}(Encoders.bean(classOf[OggMessageBean]))
				// 第二步、解析数据字段（before或after -> value）值为POJO对象
				import cn.itcast.logistics.common.BeanImplicits._
				val pojoStreamDS: Dataset[AreasBean] = oggBeanStreamDS
					// 过滤获取Area表数据
					.filter(oggBean => oggBean.getTable == TableMapping.AREAS)
					// 解析数据为POJO对象
					.map(oggBean => DataParser.toAreas(oggBean))
					.filter( pojo => null != pojo) // 过滤掉解析为null数据
				
				// 第三步、返回转换后的数据
				pojoStreamDS.toDF()
			}
			// TODO： CRM业务系统数据ETL处理
			case "crm" => {
				// 第一步、对获取Canal同步CRM数据，转换封装MessageBean对象
				implicit val crmEncoder: Encoder[CanalMessageBean] = Encoders.bean(classOf[CanalMessageBean])
				val canalBeanStreamDS: Dataset[CanalMessageBean] = streamDF
					.filter(row => ! row.isNullAt(0))
					.mapPartitions{iter =>
						iter.map{row =>
							// 获取JSON值
							val jsonStr: String = row.getAs[String](0)
							// 解析JSON数据为JavaBean对象
							JSON.parseObject(jsonStr, classOf[CanalMessageBean])
						}
					}
				// 第二步、解析数据字段（data）值为POJO对象
				import cn.itcast.logistics.common.BeanImplicits._
				val pojoStreamDS: Dataset[AddressBean] = canalBeanStreamDS
					// 过滤获取Address表数据
					.filter(canalBean => canalBean.getTable == TableMapping.ADDRESS)
					// 解析数据为POJO对象
					.map(canalBean => DataParser.toAddress(canalBean))
					.filter( pojo => null != pojo) // 过滤掉解析为null数据
				
				// 第三步、返回转换后的数据
				pojoStreamDS.toDF()
			}
			// 其他业务数据ETL处理
			case _ => streamDF
		}
		
		// 返回ETL后数据集
		etlStreamDF
	}
	
	/**
	 * 数据的处理，将Kafka获取Message解析封装值Bean对象
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 * @param category 业务数据类型，比如物流系统业务数据，CRM系统业务数据等
	 * @return 流式数据集StreamingDataFrame
	 */
	def processToBean(streamDF: DataFrame, category: String): DataFrame = {
		// 导入隐式转换
		import streamDF.sparkSession.implicits._
		// 对Kafka消费消息进行ETL转换
		val etlStreamDF: DataFrame = category match {
			// TODO： 物流业务系统数据ETL处理
			case "logistics" => {
				// 对获取OGG同步物流数据，转换封装MessageBean对象
				val oggBeanStreamDS: Dataset[OggMessageBean] = streamDF
					// 将DataFrame转换为Dataset，由于DataFrame中只有一个字段value，类型是String
					.as[String]
					// 对Dataset中每个分区数据进行操作，每个分区数据封装在迭代器中
					.mapPartitions{iter =>
						iter
							.filter(jsonStr => null != jsonStr && jsonStr.trim.length > 0 ) // 过滤数据
							// 解析JSON格式数据, 使用FastJson类库
							.map(jsonStr => JSON.parseObject(jsonStr, classOf[OggMessageBean]) )
					}(Encoders.bean(classOf[OggMessageBean]))
				// 返回转换后的数据
				oggBeanStreamDS.toDF()
			}
			// TODO： CRM业务系统数据ETL处理
			case "crm" => {
				// 对获取Canal同步CRM数据，转换封装MessageBean对象
				implicit val crmEncoder: Encoder[CanalMessageBean] = Encoders.bean(classOf[CanalMessageBean])
				val canalBeanStreamDS: Dataset[CanalMessageBean] = streamDF
					.filter(row => ! row.isNullAt(0))
					.mapPartitions{iter =>
						iter.map{row =>
							// 获取JSON值
							val jsonStr: String = row.getAs[String](0)
							// 解析JSON数据为JavaBean对象
							JSON.parseObject(jsonStr, classOf[CanalMessageBean])
						}
					}
				// 返回转换后的数据
				canalBeanStreamDS.toDF()
			}
			// 其他业务数据ETL处理
			case _ => streamDF
		}
		// 返回ETL后数据集
		etlStreamDF
	}
	
	/**
	 * 数据的保存，将数据保存至Kudu表中，如果表不存在，需要创建表
	 *
	 * @param streamDF       保存数据集DataFrame，解析Kafka数据为POJO对象，其中包含opType操作类型
	 * @param tableName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	override def save(streamDF: DataFrame, tableName: String,
	                  isAutoCreateTable: Boolean  = true): Unit = {
		
		// 第一、当Kudu中表不存在时，创建表
		if(isAutoCreateTable){
			KuduTools.createKuduTable(tableName, streamDF.drop(col("opType")))
		}
		
		// TODO: 第二、保存数据至Kudu表中，考虑OpType每条数据操作类型
		/*
			1. 当Op_Type为Insert和Update时，属于插入更新操作： Upsert
			2. 当Op_Type为Delete时，属于删除操作，依据Kudu表的主键删除数据: Delete
			
			回顾Spark与Kudu集成
				方式一：KuduContext 进行DDL（创建表，修改表等）和DML（CRUD等）操作
				方式二：SparkSQL实现外部数据源，SparkSession操作，加载数据和保存数据
		 */
		// step1. 构建Kudu的上下文对象：KuduContext
		val kuduContext: KuduContext = new KuduContext(
			Configuration.KUDU_RPC_ADDRESS, streamDF.sparkSession.sparkContext
		)
		
		// TODO: step2. 依据opType划分数据，将数据分为2部分：Upsert操作和Delete操作
		// Upsert 操作
		streamDF
			// 过滤获取插入和更新数据
			.filter(
				upper(col("opType")) === "INSERT" || upper(col("opType")) === "UPDATE"
			)
			// 删除opType列数据
			.drop(col("opType"))
			.writeStream
			.outputMode(OutputMode.Append()) // 追加输出模式
			.queryName(s"query-${tableName}-upsert")
			.foreachBatch((batchDF: DataFrame, _: Long) => {
				if(! batchDF.isEmpty) kuduContext.upsertRows(batchDF, s"${tableName}")
			})
			.start()
		
		// Delete操作，通过过滤获取删除数据
		streamDF
			.filter(upper(col("opType")) === "DELETE")
			.select(col("id"))
			.writeStream
			.outputMode(OutputMode.Append())
			.queryName(s"query-${tableName}-delete")
			.foreachBatch((batchDF: DataFrame, _: Long) => {
				if(! batchDF.isEmpty) kuduContext.deleteRows(batchDF, tableName)
			})
			.start()
	}
	
	/**
	 * 数据的保存，将数据保存至Kudu表中，如果表不存在，需要创建表
	 *
	 * @param streamDF       保存数据集DataFrame，解析Kafka数据为POJO对象，其中包含opType操作类型
	 * @param tableName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	def saveToKudu(streamDF: DataFrame, tableName: String,
	                  isAutoCreateTable: Boolean  = true): Unit = {
		val odsTableName: String = s"ods_${tableName}"
		
		// 第一、当Kudu中表不存在时，创建表
		if(isAutoCreateTable){
			KuduTools.createKuduTable(odsTableName, streamDF)
		}
		
		// 第二、保存数据至Kudu表中
		streamDF
			.writeStream
			.format(Configuration.SPARK_KUDU_FORMAT)
			.outputMode(OutputMode.Append())
			.option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
			.option("kudu.table", odsTableName)
			.queryName(s"query-${odsTableName}")
			.start()
	}
	
	/**
	 * 数据的保存，打印控制台，用于测试
	 *
	 * @param streamDF       保存数据集DataFrame
	 * @param tableName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	def saveToConsole(streamDF: DataFrame, tableName: String,
	                  isAutoCreateTable: Boolean  = true): Unit = {
		streamDF.writeStream
			.outputMode(OutputMode.Append())
			.queryName(s"query-${tableName}")
			.format("console")
			.option("numRows", "20").option("truncate", "false")
			.start()
	}
	
	/**
	 * 物流Logistics系统业务数据ETL转换处理及保存外部存储
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 */
	def etlLogistics(streamDF: DataFrame): Unit = {
		// 导入隐式转换
		import cn.itcast.logistics.common.BeanImplicits._
		
		// 转换DataFrame为Dataset
		val oggBeanStreamDS: Dataset[OggMessageBean] = streamDF.as[OggMessageBean]
		
		// Area 表数据
		val areaStreamDS: Dataset[AreasBean] = oggBeanStreamDS
			.filter(oggBean => oggBean.getTable == TableMapping.AREAS)
			.map(oggBean => DataParser.toAreas(oggBean))
			.filter( pojo => null != pojo)
		save(areaStreamDS.toDF(), TableMapping.AREAS)
		/*
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
		*/
	}
	
	/**
	 * 客户管理管理CRM系统业务数据ETL转换处理及保存外部存储
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 */
	def etlCrm(streamDF: DataFrame): Unit = {
		// 导入隐式转换
		import cn.itcast.logistics.common.BeanImplicits._
		
		// 转换DataFrame为Dataset
		val canalBeanStreamDS: Dataset[CanalMessageBean] = streamDF.as[CanalMessageBean]
		
		// Address 表数据
		val addressStreamDS: Dataset[AddressBean] = canalBeanStreamDS
			.filter(canalBean => canalBean.getTable == TableMapping.ADDRESS)
			.map(canalBean => DataParser.toAddress(canalBean))
			.filter( pojo => null != pojo)
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
	
	/*
	实时Kudu ETL应用程序入口，数据处理逻辑步骤：
		step1. 创建SparkSession实例对象，传递SparkConf
		step2. 从Kafka数据源实时消费数据
		step3. 对JSON格式字符串数据进行转换处理
		step4. 获取消费每条数据字段信息
		step5. 将解析过滤获取的数据写入同步至Kudu表
	 */
	def main(args: Array[String]): Unit = {
		// 1. 创建SparkSession实例，设置配置信息
		val spark: SparkSession = {
			// 获取SparkConf对象
			val sparkConf: SparkConf = SparkUtils.autoSettingEnv(SparkUtils.sparkConf())
			// 传递SparkConf对象，构建SparkSession实例
			SparkUtils.createSparkSession(sparkConf, this.getClass)
		}  // 导入隐式转换
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		
		// 2. 从数据源Kafka加载数据
		// 获取物流相关数据
		val logisticsStreamDF: DataFrame = load(spark, Configuration.KAFKA_LOGISTICS_TOPIC)
		// 获取CRM相关数据
		val crmStreamDF: DataFrame = load(spark, Configuration.KAFKA_CRM_TOPIC)
		
		// 3. 数据处理，调用process方法
		val logisticsEtlStreamDF: DataFrame = process(logisticsStreamDF, "logistics")
		val crmEtlStreamDF: DataFrame = process(crmStreamDF, "crm")
		
		/*
			// 4. 数据输出，调用save方法
			save(logisticsEtlStreamDF, "logistics")
			save(crmEtlStreamDF, "crm")
		*/
		
		/*
		    TODO：每个业务系统中有多张表，每张表数据需要单独转换为POJO对象，再输出到存储引擎
		    所以将【3. 数据处理】中转换为POJO和【4. 数据输出】关联在一起
		    TODO: 一个业务系统封装一个方法处理所有表数据，先进行ETL转换处理，再保存至外部存储引擎
		 */
		etlLogistics(logisticsEtlStreamDF)
		etlCrm(crmEtlStreamDF)
		
		// 5. 等待应用终止
		spark.streams.active.foreach(query => println(s"准备启动查询Query： ${query.name}"))
		spark.streams.awaitAnyTermination()
	}
}
