package cn.itcast.logistics.etl.realtime.kudu

import cn.itcast.logistics.common.beans.crm._
import cn.itcast.logistics.common.beans.logistics._
import cn.itcast.logistics.common.beans.parser.{CanalMessageBean, OggMessageBean}
import cn.itcast.logistics.common.{Configuration, KuduTools, SparkUtils, TableMapping}
import cn.itcast.logistics.etl.parser.DataParser
import cn.itcast.logistics.etl.realtime.BasicStreamApp
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql._

/**
 * Kudu数据管道应用：实现Kudu数据库的实时ETL操作
 */
object KuduStreamApp_05 extends BasicStreamApp {
	
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
	override def save(streamDF: DataFrame, tableName: String,
	                  isAutoCreateTable: Boolean = true): Unit = {
		// 将实时ETL数据存储在Kudu表中，属于数仓架构ODS层
		val odsTableName: String = s"ods_${tableName}"
		
		// step1. 判断是否需要创建表（当表不存在时）
		if(isAutoCreateTable){
			// TODO: 判断Kudu中表是否存在，如果不存在，创建表
			KuduTools.createKuduTable(odsTableName, streamDF)
		}
		
		// step2. 保存数据至Kudu表
		streamDF.writeStream
    		.outputMode(OutputMode.Append())
    		.queryName(s"query-kudu-${odsTableName}")
    		.format(Configuration.SPARK_KUDU_FORMAT)
    		.option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
    		.option("kudu.table", odsTableName)
    		.option("kudu.operation", "upsert")
    		.option("kudu.socketReadTimeoutMs", "10000")
    		.start()
	}
	
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
	
	/** MAIN 方法入口：Spark Application应用程序入口，必须创建SparkContext对象或SpakrSession对象 */
	def main(args: Array[String]): Unit = {
		// step1. 获取SparkSession对象
		val spark: SparkSession = SparkUtils.createSparkSession(
			SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass
		)
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		
		// step2. 从Kafka加载数据：物流系统业务数据logistics和CRM系统业务数据crm
		val logisticsStreamDF: DataFrame = load(spark, "logistics")
		val crmStreamDF: DataFrame = load(spark, "crm")
		
		// step3. 对流式数据进行实时ETL转换操作（解析JSON，封装Bean对象）
		//val logisticsEtlStreamDF = process(logisticsStreamDF, "logistics")
		val crmEtlStreamDF = process(crmStreamDF, "crm")
		
		// step4. 将转换后数据进行输出
		//save(logisticsEtlStreamDF, "tbl_logistics")
		//save(crmEtlStreamDF, "tbl_crm")
		
		/**
		 * TODO：针对不同业务系统，提供不同方法，每个方法中针对不同表进行2个操作：
		 * 1）、Bean 转换POJO对象
		 * 2）、POJO对象数据保存
		 */
		//etlLogistics(logisticsEtlStreamDF)
		etlCrm(crmEtlStreamDF)
		
		// step5. 启动流式应用后，等待终止
		spark.streams.active.foreach(query => println(s"正在启动Query： ${query.name}"))
		spark.streams.awaitAnyTermination()
	}
}
