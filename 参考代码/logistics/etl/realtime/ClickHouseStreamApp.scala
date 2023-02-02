package cn.itcast.logistics.etl.realtime

import cn.itcast.logistics.common.beans.crm.{AddressBean, ConsumerAddressMapBean, CustomerBean}
import cn.itcast.logistics.common.beans.logistics.AreasBean
import cn.itcast.logistics.common.beans.parser.{CanalMessageBean, OggMessageBean}
import cn.itcast.logistics.common.{Configuration, SparkUtils, TableMapping}
import cn.itcast.logistics.etl.parser.DataParser
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger

/**
 * ClickHouse 数据管道应用：实现ClickHouse数据库的实时ETL操作
 */
object ClickHouseStreamApp extends BasicStreamApp {
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
		
		category match {
			// TODO： 物流业务系统数据ETL处理
			case "logistics" =>
				// 第一步、对获取OGG同步物流数据，转换封装MessageBean对象
				val oggBeanStreamDS: Dataset[OggMessageBean] = streamDF
					// 将DataFrame转换为Dataset，由于DataFrame中只有一个字段value，类型是String
					.as[String]
					// 对Dataset中每个分区数据进行操作，每个分区数据封装在迭代器中
					.mapPartitions { iter =>
						iter
							.filter(jsonStr => null != jsonStr && jsonStr.trim.length > 0) // 过滤数据
							// 解析JSON格式数据, 使用FastJson类库
							.map(jsonStr => JSON.parseObject(jsonStr, classOf[OggMessageBean]))
					}(Encoders.bean(classOf[OggMessageBean]))
				// 第二步、返回转换后的数据
				oggBeanStreamDS.toDF()
			
			// TODO： CRM业务系统数据ETL处理
			case "crm" =>
				// 第一步、对获取Canal同步CRM数据，转换封装MessageBean对象
				implicit val crmEncoder: Encoder[CanalMessageBean] = Encoders.bean(classOf[CanalMessageBean])
				val canalBeanStreamDS: Dataset[CanalMessageBean] = streamDF
					.filter(row => !row.isNullAt(0))
					.mapPartitions { iter =>
						iter.map { row =>
							// 获取JSON值
							val jsonStr: String = row.getAs[String](0)
							// 解析JSON数据为JavaBean对象
							JSON.parseObject(jsonStr, classOf[CanalMessageBean])
						}
					}
				// 第二步、返回转换后的数据
				canalBeanStreamDS.toDF()
			// TODO：其他业务数据ETL处理
			case _ => streamDF
		}
	}
	
	/**
	 * 数据的保存
	 *
	 * @param streamDF          保存数据集DataFrame
	 * @param tableName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	override def save(streamDF: DataFrame, tableName: String, isAutoCreateTable: Boolean): Unit = {
		streamDF
			.writeStream
			.queryName(s"query-${tableName}")
			.trigger(Trigger.ProcessingTime("10 seconds"))
			.format(Configuration.SPARK_CLICK_HOUSE_FORMAT) // 指定数据源
			.option("clickhouse.driver", Configuration.CLICK_HOUSE_DRIVER)
			.option("clickhouse.url", Configuration.CLICK_HOUSE_URL)
			.option("clickhouse.user", Configuration.CLICK_HOUSE_USER)
			.option("clickhouse.password", Configuration.CLICK_HOUSE_PASSWORD)
			.option("clickhouse.table", tableName)
			.option("clickhouse.auto.create", isAutoCreateTable)
			.option("clickhouse.primary.key", "id")
			.option("clickhouse.operate.field", "opType")
			.start() // 启动流式应用
	}
	
	/**
	 * 物流Logistics系统业务数据ETL转换处理及保存外部存储ClickHouse表
	 */
	def saveClickHouseLogistics(streamDF: DataFrame): Unit = {
		// 导入隐式转换
		import cn.itcast.logistics.common.BeanImplicits._
		
		// 转换DataFrame为Dataset
		val oggBeanStreamDS: Dataset[OggMessageBean] = streamDF.as[OggMessageBean]
		
		val expressBillStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.EXPRESS_BILL)
			.map(bean => DataParser.toExpressBill(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(expressBillStreamDF, TableMapping.EXPRESS_BILL)
		
		val waybillStreamDF = oggBeanStreamDS
			.filter(bean => bean.getTable == TableMapping.WAY_BILL)
			.map(bean => DataParser.toWaybill(bean))
			.filter( pojo => null != pojo)
			.toDF()
		save(waybillStreamDF, TableMapping.WAY_BILL)
		
		val areaStreamDS: Dataset[AreasBean] = oggBeanStreamDS
			.filter(oggBean => oggBean.getTable == TableMapping.AREAS)
			.map(oggBean => DataParser.toAreas(oggBean))
			.filter( pojo => null != pojo)
		save(areaStreamDS.toDF(), TableMapping.AREAS)
		
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
	
	/**
	 * 客户管理管理CRM系统业务数据ETL转换处理及保存外部存储ClickHouse表
	 */
	def saveClickHouseCrm(streamDF: DataFrame): Unit = {
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
		实时ClickHouse ETL应用程序入口，数据处理逻辑步骤：
			step1. 创建SparkSession实例对象，传递SparkConf
			step2. 从Kafka数据源实时消费数据
			step3. 对JSON格式字符串数据进行转换处理
			step4. 获取消费每条数据字段信息
			step5. 将解析过滤获取的数据写入同步至Es索引
	 */
	def main(args: Array[String]): Unit = {
		// 1. 创建SparkSession实例，设置配置信息
		val spark: SparkSession = {
			// 获取SparkConf对象
			val sparkConf: SparkConf = SparkUtils
				.autoSettingEnv(SparkUtils.sparkConf())
				// 设置Kryo序列化
				.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			// 传递SparkConf对象，构建SparkSession实例
			SparkUtils.createSparkSession(sparkConf, this.getClass)
		} // 导入隐式转换
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		
		// 2. 从数据源Kafka加载数据
		// 获取物流相关数据
		val logisticsStreamDF: DataFrame = load(spark, Configuration.KAFKA_LOGISTICS_TOPIC)
		// 获取CRM相关数据
		val crmStreamDF: DataFrame = load(spark, Configuration.KAFKA_CRM_TOPIC)
		
		// 3. 数据处理，调用process方法
		val logisticsBeanStreamDF: DataFrame = process(logisticsStreamDF, "logistics")
		val crmBeanStreamDF: DataFrame = process(crmStreamDF, "crm")
		
		// 4. 数据输出，调用save方法
		saveClickHouseLogistics(logisticsBeanStreamDF)
		saveClickHouseCrm(crmBeanStreamDF)
		
		// 5. 等待应用终止
		spark.streams.active.foreach(query => println(s"准备启动查询Query： ${query.name}"))
		spark.streams.awaitAnyTermination()
	}
}
