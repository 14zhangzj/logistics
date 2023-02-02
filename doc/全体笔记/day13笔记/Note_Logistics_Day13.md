---
stypora-copy-images-to: img
typora-root-url: ./
---



# Logistics_Day13：数据服务接口开发



![1614521554768](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614521554768.png)

网址：https://smart.jdwl.com/jh_demo.html



## 01-[复习]-上次课程内容回顾 

> ​		主要实现自定义外部数据源：按照SparkSQL提供DataSource API V2实现ClickHouse数据源，可以批量从ClickHouse数据库加载load和保存数据，以及流式数据保存。
>
> [在SparkSQL中，从2.3版本开始，提供DataSource API V2（使用Java语言开发接口）版本，继承结构示意图如下所示：]()

![1610932507875](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610932507875.png)

> - 1）、批量加载：`load`，继承（实现）`DataSourceV2`和`ReadSupport`，实现其中方法，对于读取数据来说，最重要类：`DataSourceReader`。
> - 2）、批量保存：`save`，继承（实现）`DataSourceV2`和`WriteSupport`，实现其中方法，对于写入数据来说，最重要类：`DataSourceWriter`。
> - 3）、流式保存：`start`，继承（实现）`DataSourceV2`和`StreamWriteSupport`，实现其中方法，对于流式写入数据来说，最重要类：`StreamWriter`，此Writer继承`DataSourceWriter`，底层写入数据时一样的。

> ​			在物流项目中，需要编写实时应用程序，此处指的就是结构化流应用程序，实时将业务数据进行ETL转换后，存储到ClickHouse表中，需要流式数据保存，
>

![1610932356437](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610932356437.png)

> 注意：针对物流项目来说，在流式应用程序运行之前，需要批量将历史数据导入到ClickHouse表中。

![1614646475467](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614646475467.png)



## 02-[了解]-第13天：课程内容提纲 

> ​		主要讲解三个方法内容：实时ETL存储ClickHouse表、实时大屏系统和物流项目回顾。
>
> - 1）、==实时ETL存储ClickHouse表==
>   - 编写结构化流应用程式，实时从Kafka消费数据，进行ETL转换，最终存储到ClickHouse表中
>   - 类似实时ETL存储Kudu表或Elasticsearch索引
>   - 整个流式应用程序示意图如下所示：

![1614518137804](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614518137804.png)

> - 2）、实时大屏系统
>   - 针对物流项目来说，对实时性要求不是很高，使用SparkStreaming或者StructuredStreaming完全可以满足实时性需求。
>   - 前端应用系统直接调用数据接口，从ClickHouse表查询分析数据，将结构以JSON格式返回给前端展示
>   - 基于SpringCloud开发数据服务接口和使用NodeJS实现大屏前端
> - 3）、物流项目回顾
>   - 整体性回顾物流项目核心业务模块、技术点和技术面试题



## 03-[掌握]-ClickHouse 分析之实时ETL开发【结构】

> ​		接下来，编写流式应用程序（结构化流程程序），实时从Kafka消费数据，进行ETL转换，最终存储到ClickHouse表中，程序流程图如下所示：
>
> [类似前面讲解实时ETL存储至Kudu表，仅仅转换后数据存储终端Sink不同而已，此处为ClickHouse数据库]()

![1610934873712](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610934873712.png)

> 注意：整个项目中，采集多个业务系统相关业务数据，统一存储到Kafka Topic中，[此处仅仅以2个业务系统为例：物流系统Logistics和CRM系统，实时采集数据到Topic。]()
>
> ==不同业务数据采集到Kafka时，存储topic不一样的，方便数据管理，不同业务系统业务数据量不一样的，所以在Kafka中创建Topic对应的分区partition数目不一样。==

![1614647779598](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614647779598.png)



## 04-[掌握]-ClickHouse 分析之实时ETL开发【存储】

> ​		接下来，完成流式程序方法方式：
>
> - 1）、数据转换：`process`，将JSON字符串转换为MessageBean对象。

```scala
	/**
	 * 数据的处理，消费Kafka数据进行转换：JSON -> Bean对象
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
			// TODO: 物流系统业务数据ETL转换
			case "logistics" =>
				// 第一步、TODO： a. 转换JSON为Bean对象
				val beanStreamDS: Dataset[OggMessageBean] = streamDF
					.as[String] // 将DataFrame转换为Dataset
					.filter(json => null != json && json.trim.length > 0) // 过滤数据
					// 由于OggMessageBean是使用Java语言自定义类，所以需要自己指定编码器Encoder
					.mapPartitions { iter =>
						iter.map { json => JSON.parseObject(json, classOf[OggMessageBean]) }
					}
				// 第二步、c. 返回转换后数据
				beanStreamDS.toDF()
			
			// TODO：CRM系统业务数据ETL转换
			case "crm" =>
				// 第一步、TODO： a. 转换JSON为Bean对象
				val beanStreamDS: Dataset[CanalMessageBean] = streamDF
					.filter(row => ! row.isNullAt(0))
					.mapPartitions{iter =>
						iter.map{row =>
							val json = row.getAs[String](0)
							JSON.parseObject(json.trim, classOf[CanalMessageBean])
						}
					}
				// 第二步、c. 返回转换后数据
				beanStreamDS.toDF()
			
			// TODO: 其他业务数据数据，直接返回即可
			case _ => streamDF
		}
	}
```



> - 2）、数据保存：`save`，数据保存至ClickHouse表

```scala
	/**
	 * 数据的保存，将数据保存至ClickHouse表中
	 *
	 * @param streamDF          保存数据集DataFrame
	 * @param tableName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认创建表
	 */
	override def save(streamDF: DataFrame, tableName: String, isAutoCreateTable: Boolean): Unit = {
		streamDF
			.writeStream
			.queryName(s"query-${Configuration.SPARK_CLICK_HOUSE_FORMAT}-${tableName}")
			// 设置触发器，时间间隔
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
```



> - 3）、业务系统数据保存：`saveClickhouseXx`，从MessageBean中提取数据字段的值并保存
>
>   - 物流系统业务数据：`saveClickHouseLogistics`
>
>   ```scala
>   	/**
>   	 * 针对物流系统业务数据，转换后的Bean数据，提取数据字段，保存至ClickHouse表
>   	 */
>   	def saveClickHouseLogistics(streamDF: DataFrame): Unit = {
>   		
>   		// 转换DataFrame为Dataset
>   		val oggBeanStreamDS: Dataset[OggMessageBean] = streamDF.as[OggMessageBean]
>   		
>   		// 第二步、TODO：b. 提取Bean数据字段，封装POJO
>   		/*
>   			以Logistics物流系统：tbl_areas为例，从Bean对象获取数据字段值，封装POJO【AreasBean】
>   		 */
>   		val areasStreamDS: Dataset[AreasBean] = oggBeanStreamDS
>   			// 过滤获取AreasBean数据
>   			.filter(oggBean => oggBean.getTable == TableMapping.AREAS)
>   			// 提取数据字段值，进行封装为POJO
>   			.map(oggBean => DataParser.toAreas(oggBean))
>   			// 过滤不为null
>   			.filter(pojo => null != pojo)
>   		save(areasStreamDS.toDF(), TableMapping.AREAS)
>   		
>   		val warehouseSendVehicleStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_SEND_VEHICLE)
>   			.map(bean => DataParser.toWarehouseSendVehicle(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(warehouseSendVehicleStreamDF, TableMapping.WAREHOUSE_SEND_VEHICLE)
>   		
>   		val waybillLineStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.WAYBILL_LINE)
>   			.map(bean => DataParser.toWaybillLine(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(waybillLineStreamDF, TableMapping.WAYBILL_LINE)
>   		
>   		val chargeStandardStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.CHARGE_STANDARD)
>   			.map(bean => DataParser.toChargeStandard(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(chargeStandardStreamDF, TableMapping.CHARGE_STANDARD)
>   		
>   		val codesStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.CODES)
>   			.map(bean => DataParser.toCodes(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(codesStreamDF, TableMapping.CODES)
>   		
>   		val collectPackageStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.COLLECT_PACKAGE)
>   			.map(bean => DataParser.toCollectPackage(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(collectPackageStreamDF, TableMapping.COLLECT_PACKAGE)
>   		
>   		val companyStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.COMPANY)
>   			.map(bean => DataParser.toCompany(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(companyStreamDF, TableMapping.COMPANY)
>   		
>   		val companyDotMapStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.COMPANY_DOT_MAP)
>   			.map(bean => DataParser.toCompanyDotMap(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(companyDotMapStreamDF, TableMapping.COMPANY_DOT_MAP)
>   		
>   		val companyTransportRouteMaStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.COMPANY_TRANSPORT_ROUTE_MA)
>   			.map(bean => DataParser.toCompanyTransportRouteMa(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(companyTransportRouteMaStreamDF, TableMapping.COMPANY_TRANSPORT_ROUTE_MA)
>   		
>   		val companyWarehouseMapStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.COMPANY_WAREHOUSE_MAP)
>   			.map(bean => DataParser.toCompanyWarehouseMap(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(companyWarehouseMapStreamDF, TableMapping.COMPANY_WAREHOUSE_MAP)
>   		
>   		val consumerSenderInfoStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.CONSUMER_SENDER_INFO)
>   			.map(bean => DataParser.toConsumerSenderInfo(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(consumerSenderInfoStreamDF, TableMapping.CONSUMER_SENDER_INFO)
>   		
>   		val courierStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.COURIER)
>   			.map(bean => DataParser.toCourier(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(courierStreamDF, TableMapping.COURIER)
>   		
>   		val deliverPackageStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.DELIVER_PACKAGE)
>   			.map(bean => DataParser.toDeliverPackage(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(deliverPackageStreamDF, TableMapping.DELIVER_PACKAGE)
>   		
>   		val deliverRegionStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.DELIVER_REGION)
>   			.map(bean => DataParser.toDeliverRegion(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(deliverRegionStreamDF, TableMapping.DELIVER_REGION)
>   		
>   		val deliveryRecordStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.DELIVERY_RECORD)
>   			.map(bean => DataParser.toDeliveryRecord(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(deliveryRecordStreamDF, TableMapping.DELIVERY_RECORD)
>   		
>   		val departmentStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.DEPARTMENT)
>   			.map(bean => DataParser.toDepartment(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(departmentStreamDF, TableMapping.DEPARTMENT)
>   		
>   		val dotStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.DOT)
>   			.map(bean => DataParser.toDot(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(dotStreamDF, TableMapping.DOT)
>   		
>   		val dotTransportToolStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.DOT_TRANSPORT_TOOL)
>   			.map(bean => DataParser.toDotTransportTool(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(dotTransportToolStreamDF, TableMapping.DOT_TRANSPORT_TOOL)
>   		
>   		val driverStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.DRIVER)
>   			.map(bean => DataParser.toDriver(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(driverStreamDF, TableMapping.DRIVER)
>   		
>   		val empStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.EMP)
>   			.map(bean => DataParser.toEmp(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(empStreamDF, TableMapping.EMP)
>   		
>   		val empInfoMapStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.EMP_INFO_MAP)
>   			.map(bean => DataParser.toEmpInfoMap(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(empInfoMapStreamDF, TableMapping.EMP_INFO_MAP)
>   		
>   		val expressBillStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.EXPRESS_BILL)
>   			.map(bean => DataParser.toExpressBill(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(expressBillStreamDF, TableMapping.EXPRESS_BILL)
>   		
>   		val expressPackageStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.EXPRESS_PACKAGE)
>   			.map(bean => DataParser.toExpressPackage(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(expressPackageStreamDF, TableMapping.EXPRESS_PACKAGE)
>   		
>   		val fixedAreaStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.FIXED_AREA)
>   			.map(bean => DataParser.toFixedArea(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(fixedAreaStreamDF, TableMapping.FIXED_AREA)
>   		
>   		val goodsRackStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.GOODS_RACK)
>   			.map(bean => DataParser.toGoodsRack(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(goodsRackStreamDF, TableMapping.GOODS_RACK)
>   		
>   		val jobStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.JOB)
>   			.map(bean => DataParser.toJob(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(jobStreamDF, TableMapping.JOB)
>   		
>   		val outWarehouseStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.OUT_WAREHOUSE)
>   			.map(bean => DataParser.toOutWarehouse(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(outWarehouseStreamDF, TableMapping.OUT_WAREHOUSE)
>   		
>   		val outWarehouseDetailStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.OUT_WAREHOUSE_DETAIL)
>   			.map(bean => DataParser.toOutWarehouseDetail(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(outWarehouseDetailStreamDF, TableMapping.OUT_WAREHOUSE_DETAIL)
>   		
>   		val pkgStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.PKG)
>   			.map(bean => DataParser.toPkg(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(pkgStreamDF, TableMapping.PKG)
>   		
>   		val postalStandardStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.POSTAL_STANDARD)
>   			.map(bean => DataParser.toPostalStandard(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(postalStandardStreamDF, TableMapping.POSTAL_STANDARD)
>   		
>   		val pushWarehouseStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.PUSH_WAREHOUSE)
>   			.map(bean => DataParser.toPushWarehouse(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(pushWarehouseStreamDF, TableMapping.PUSH_WAREHOUSE)
>   		
>   		val pushWarehouseDetailStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.PUSH_WAREHOUSE_DETAIL)
>   			.map(bean => DataParser.toPushWarehouseDetail(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(pushWarehouseDetailStreamDF, TableMapping.PUSH_WAREHOUSE_DETAIL)
>   		
>   		val routeStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.ROUTE)
>   			.map(bean => DataParser.toRoute(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(routeStreamDF, TableMapping.ROUTE)
>   		
>   		val serviceEvaluationStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.SERVICE_EVALUATION)
>   			.map(bean => DataParser.toServiceEvaluation(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(serviceEvaluationStreamDF, TableMapping.SERVICE_EVALUATION)
>   		
>   		val storeGridStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.STORE_GRID)
>   			.map(bean => DataParser.toStoreGrid(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(storeGridStreamDF, TableMapping.STORE_GRID)
>   		
>   		val transportToolStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.TRANSPORT_TOOL)
>   			.map(bean => DataParser.toTransportTool(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(transportToolStreamDF, TableMapping.TRANSPORT_TOOL)
>   		
>   		val vehicleMonitorStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.VEHICLE_MONITOR)
>   			.map(bean => DataParser.toVehicleMonitor(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(vehicleMonitorStreamDF, TableMapping.VEHICLE_MONITOR)
>   		
>   		val warehouseStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.WAREHOUSE)
>   			.map(bean => DataParser.toWarehouse(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(warehouseStreamDF, TableMapping.WAREHOUSE)
>   		
>   		val warehouseEmpStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_EMP)
>   			.map(bean => DataParser.toWarehouseEmp(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(warehouseEmpStreamDF, TableMapping.WAREHOUSE_EMP)
>   		
>   		val warehouseRackMapStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_RACK_MAP)
>   			.map(bean => DataParser.toWarehouseRackMap(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(warehouseRackMapStreamDF, TableMapping.WAREHOUSE_RACK_MAP)
>   		
>   		val warehouseReceiptStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_RECEIPT)
>   			.map(bean => DataParser.toWarehouseReceipt(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(warehouseReceiptStreamDF, TableMapping.WAREHOUSE_RECEIPT)
>   		
>   		val warehouseReceiptDetailStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_RECEIPT_DETAIL)
>   			.map(bean => DataParser.toWarehouseReceiptDetail(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(warehouseReceiptDetailStreamDF, TableMapping.WAREHOUSE_RECEIPT_DETAIL)
>   		
>   		val warehouseTransportToolStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_TRANSPORT_TOOL)
>   			.map(bean => DataParser.toWarehouseTransportTool(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(warehouseTransportToolStreamDF, TableMapping.WAREHOUSE_TRANSPORT_TOOL)
>   		
>   		val warehouseVehicleMapStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.WAREHOUSE_VEHICLE_MAP)
>   			.map(bean => DataParser.toWarehouseVehicleMap(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(warehouseVehicleMapStreamDF, TableMapping.WAREHOUSE_VEHICLE_MAP)
>   		
>   		val waybillStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.WAY_BILL)
>   			.map(bean => DataParser.toWaybill(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(waybillStreamDF, TableMapping.WAY_BILL)
>   		
>   		val waybillStateRecordStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.WAYBILL_STATE_RECORD)
>   			.map(bean => DataParser.toWaybillStateRecord(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(waybillStateRecordStreamDF, TableMapping.WAYBILL_STATE_RECORD)
>   		
>   		val workTimeStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.WORK_TIME)
>   			.map(bean => DataParser.toWorkTime(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(workTimeStreamDF, TableMapping.WORK_TIME)
>   		
>   		val transportRecordStreamDF = oggBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.TRANSPORT_RECORD)
>   			.map(bean => DataParser.toTransportRecordBean(bean))
>   			.filter( pojo => null != pojo)
>   			.toDF()
>   		save(transportRecordStreamDF, TableMapping.TRANSPORT_RECORD)
>   	}
>   ```
>
>   
>
>   - CRM系统业务数据：`saveClickHouseCrm`
>
>   ```scala
>   	/**
>   	 * 针对CRM系统业务数据，转换后的Bean数据，提取数据字段，保存至ClickHouse表
>   	 */
>   	def saveClickHouseCrm(streamDF: DataFrame): Unit = {
>   		// 转换DataFrame为Dataset
>   		val canalBeanStreamDS: Dataset[CanalMessageBean] = streamDF.as[CanalMessageBean]
>   		
>   		// 第二步、TODO： b. Bean提取数据字段，封装为POJO对象
>   		val addressStreamDS: Dataset[AddressBean] = canalBeanStreamDS
>   			// 过滤获取地址信息表相关数据：tbl_address
>   			.filter(canalBean => canalBean.getTable == TableMapping.ADDRESS)
>   			// 提取数据字段，转换为POJO对象
>   			.map(canalBean => DataParser.toAddress(canalBean))
>   			.filter(pojo => null != pojo) // 过滤非空数据
>   		save(addressStreamDS.toDF(), TableMapping.ADDRESS)
>   		
>   		// Customer 表数据
>   		val customerStreamDS: Dataset[CustomerBean] = canalBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.CUSTOMER)
>   			.map(bean => DataParser.toCustomer(bean))
>   			.filter( pojo => null != pojo)
>   		save(customerStreamDS.toDF(), TableMapping.CUSTOMER)
>   		
>   		// ConsumerAddressMap 表数据
>   		val consumerAddressMapStreamDS: Dataset[ConsumerAddressMapBean] = canalBeanStreamDS
>   			.filter(bean => bean.getTable == TableMapping.CONSUMER_ADDRESS_MAP)
>   			.map(bean => DataParser.toConsumerAddressMap(bean))
>   			.filter( pojo => null != pojo)
>   		save(consumerAddressMapStreamDS.toDF(), TableMapping.CONSUMER_ADDRESS_MAP)
>   	}
>   ```
>
>   ​	当上述方法实现完成以后，可以启动MySQL数据库和Oracle数据库，及Canal及OGG模拟产生数据，实时消费，将业务数据保存至ClickHouse表中。



## 05-[掌握]-ClickHouse 分析之实时ETL开发【测试】

> ​	运行应用程序和数据模拟生成器，整体测试程序，步骤如下：
>
> - 1）、启动数据库和采集框架，采用Docker容器部署
>
>   - step1、启动CM，界面启动Zk服务和Kafka服务
>
>   ```ini
>   [root@node2 ~]# jps
>   2170 Kafka
>   2172 QuorumPeerMain
>   ```
>
>   
>
>   - step2、MySQL数据库容器
>
>   ```ini
>   [root@node1 ~]# docker start mysql
>   ```
>
>   
>
>   - step3、CanalServer容器
>
>   ```ini
>   [root@node1 ~]# docker start canal-server
>   
>   [root@node1 ~]# docker exec -it  canal-server /bin/bash
>   [root@28888fad98c9 admin]# cd canal-server/bin
>   [root@28888fad98c9 bin]# ./restart.sh 
>   [root@28888fad98c9 bin]# jps
>   252 Jps
>   221 CanalLauncher
>   ```
>
>   
>
>   - step4、Oracle数据库容器
>
>     ```ini
>     [root@node1 ~]# docker start myoracle
>     ```
>
>     
>
>     - 启动Oracle数据库
>    
>     ```ini
>     # 数据库服务
>     [root@node1 ~]# docker exec -it myoracle /bin/bash
>     [root@server01 oracle]# su - oracle
>     [oracle@server01 ~]$ sqlplus "/as sysdba"
>     SQL> startup   
>         
>     # 监听服务
>     [root@node1 ~]# docker exec -it myoracle /bin/bash
>     [root@server01 oracle]# su - oracle
>     [oracle@server01 ~]$ cd $ORACLE_HOME/bin
>     [oracle@server01 bin]$ lsnrctl start
>     ```
>
>     
>
>     - 启动OGG服务，分为源端和目标端
>    
>     ```ini
>     # 源端服务
>     [root@node1 ~]# docker exec -it myoracle /bin/bash
>     [root@server01 oracle]# su - oracle
>     [oracle@server01 ~]$ cd $OGG_SRC_HOME/
>     [oracle@server01 src]$ ./ggsci 
>     GGSCI (server01) 1> start mgr 
>     GGSCI (server01) 3> start EXTKAFKA
>     GGSCI (server01) 4> start PUKAFKA
>     GGSCI (server01) 5> info all
>         
>     Program     Status      Group       Lag at Chkpt  Time Since Chkpt
>         
>     MANAGER     RUNNING                                           
>     EXTRACT     RUNNING     EXTKAFKA    34:47:04      00:00:01    
>     EXTRACT     RUNNING     PUKAFKA     00:00:00      00:00:07 
>     
>     
>     # 目标端服务
>     [root@node1 ~]# docker exec -it myoracle /bin/bash
>     [root@server01 oracle]# su - oracle
>     [oracle@server01 ~]$ cd $OGG_TGR_HOME/
>     [oracle@server01 tgr]$ ./ggsci 
>     GGSCI (server01) 1> start mgr
>     GGSCI (server01) 4> start REKAFKA
>     GGSCI (server01) 6> info all
>         
>     Program     Status      Group       Lag at Chkpt  Time Since Chkpt
>         
>     MANAGER     RUNNING                                           
>     REPLICAT    RUNNING     REKAFKA     00:00:00      34:48:25   
>     ```
>
>     
>
> - 2）、启动流式应用程序
>
>   - 直接运行`ClickHouseStreamApp`程序即可，确保检查目录被删除。
>
> - 3）、启动数据模拟生成器程序
>
>   - 先启动CRM系统数据模拟生成器程序：`MockCrmDataApp`
>   - 再启动Logistics系统数据模拟生成器程序：`MockLogisticsDataApp`

**注意**，自定义外部数据源ClickHouse，没有让类继承Serializable接口，所以Spark流式应用程序，需要设置序列化Kryo方式，否则程序异常报错，序列化异常。

![1614651817137](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614651817137.png)



## 面试题-[掌握]-结构化流知识点核心概念

> 面试题：==结构化流中如何保证应用程序容灾恢复或端到端精确性一次语义的？？？==

[为了实现这个目标，Structured Streaming设计source、sink和execution engine来追踪计算处理的进度，这样就可以在任何一个步骤出现失败时自动重试。]()

> - 1、每个Streaming source都被设计成支持offset，进而可以让Spark来追踪读取的位置；
> - 2、Spark基于checkpoint和wal来持久化保存每个trigger interval内处理的offset的范围；

![1614652123578](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614652123578.png)

> 3、sink被设计成可以支持在多次计算处理时保持幂等性，就是说，用同样的一批数据，无论多
> 少次去更新sink，都会保持一致和相同的状态。
>
> [综合利用基于offset的source，基于checkpoint和wal的execution engine，以及基于幂等性的sink，可以支持完整的一次且仅一次的语义。]()



> ==结构化流编程模型==：Structured Streaming将流式数据当成一个不断增长的table，然后使用和批处理同一套API，都是基于DataSet/DataFrame的。

![1614652281439](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614652281439.png)

> 在这个模型中，主要存在下面几个组成部分：

![1614652310684](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614652310684.png)

![1614652321871](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614652321871.png)

> ​		Structured Streaming最核心的思想就是将实时到达的数据看作是一个不断追加的unbound table无界表，到达流的每个数据项就像是表中的一个新行被附加到无边界的表中，用静态结构化数据的批处理查询方式进行流计算。



## 06-[掌握]-实时ETL开发之UDF函数使用

> ​		在整个物流项目中，无论是实时ETL操作，还是离线报表分析，都没有使用UDF函数处理数据。接下来修改实时ETL程序：`ClickHouseStreamApp`，自定义UDF函数，实现数据转换操作。

```ini
回顾在SparkSQL中，自定义UDF函数方式：
1）、方式一：在SQL中使用
	spark.udf.register(
		() => {}   // 匿名函数
	)
	
2）、方式二：在DSL中使用
	import org.apache.spark.sql.functions.udf
	val xx_udf = udf(
		() => {}   // 匿名函数
	)
```

> ​		修改程序`ClickHouseStreamApp`，自定义UDF函数，实现==将JSON字符串转换为MessageBean对象==，在DSL中使用。

```scala
		// TODO: 自定义UDF函数，实现JSON字符串转换为MessageBean对象
		import org.apache.spark.sql.functions.udf
		val json_to_bean: UserDefinedFunction = udf(
			(json: String) => {
				JSON.parseObject(json, classOf[OggMessageBean])
			}
		)
```

> 对应数据ETL转换代码：

![1614654092216](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614654092216.png)



## 07-[掌握]-实时大屏系统之大屏指标开发

> ​		将物流系统和CRM系统业务数据实时同步到`ClickHouse`数据库表中以后，需要开发三个应用：
>
> - 1）、==实时大屏展示==，每隔一定时间从ClickHouse表中查数据，在前端扎实
>   - 固定指标实时查询分析
> - 2）、==数据服务接口==，提供给其他用户导出相关业务数据，进行使用
>   - 接口就是：Url连接地址，需要传递参数，进行访问请求，返回JSON格式数据
> - 3）、==OLAP实时分析==
>   - 依据不确定需求，实时查询分析，得到结果，比如临时会议所需要报表数据、临时统计分析。

> ​		物流实时大屏展示的数据，==从ClickHouse·表中读取，底层调用的是`JDBC Client API`，通过服务接口提供给前端页面展示，每隔10秒刷新页面从ClickHouse表中查询数据==。

![1614654600161](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614654600161.png)

> 实时大屏截图，各个指标对应的SQL语句如下所示：

![1614654630759](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614654630759.png)

> 实时大屏上指标如下所示：

![1614654689075](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614654689075.png)



## 08-[掌握]-实时大屏系统之数据服务接口

> ​		服务接口：==给用户提供URL地址信息，通过传递参数，请求服务端，获取数据，通常以JSON格式返回==。

> 高德地图给开发者用户提供数据服务接口，比如依据IP地址，查询归属地（省份、城市等信息）
>
> - 高德网站：https://developer.amap.com/api/webservice/guide/api/ipconfig （注册）
> - IP定位，提供URL地址

![1614655355783](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614655355783.png)

> ```ini
> url：
> 	https://restapi.amap.com/v3/ip?key=e34eb745a49cd9907f15d96418c4f9c0&ip=116.30.197.230
> parameters:
> 	key: 
> 	ip: 
> ```
>
> 在浏览器上输入URL地址，回车请求，获取信息数据

![1614655590393](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614655590393.png)



> GET 请求参数信息：

![1614655400930](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614655400930.png)



> ​		开发api接口访问后端存储介质，可视化页面通过http访问开发的后端api接口，从而将数据通过图表展示出来或者以JSON格式返回数据。
>
> [在物流项目中，开发数据服务接口，分别查询Es索引Index数据和ClickHouse表数据，对外提供服务]()
>
> - 1）、物流业务数据存储Elasticsearh索引Index中时，开发数据服务接口，流程示意图：

![1614655699352](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614655699352.png)

> - 2）、物流业务数据存储ClickHouse表中时，开发数据服务接口，流程示意图：

![1614655737259](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614655737259.png)



## 09-[理解]-实时大屏系统之SpringCloud 服务

> ​		数据服务接口开发：使用SpringCloud进行开发，目前使用比较广泛框架。

```
从浅入深精通SpringCloud微服务架构
	https://www.bilibili.com/video/BV1eE41187Ug
```

![1614656838554](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614656838554.png)

> 网址：https://spring.io/projects/spring-cloud

![1614656936174](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614656936174.png)

> 提供开发完成数据接口，解压【logistics-web-parent.zip】包，IDEA导入Maven工程，如下图所示：

![1614657021077](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614657021077.png)

> SpringCloud由5个部分组成，开发时，对应5个Maven Module模块，启动5个应用（都是SpringBoot开发）

![1614657094374](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614657094374.png)

> 启动应用时，从左到右服务进行启动：
>
> [注意，在启动应用之前，需要将Elasticsearch和ClickHouse启动，连接服务，后续查询数据]()

![1614657142231](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614657142231.png)

> 启动应用，可以发现，连接ES和ClickHouse

![1614657447115](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614657447115.png)

> 当所有应用启动完成以后，在浏览器中输入Eureka服务地址：http://localhost:8001



> 访问API服务中的接口：http://localhost:8085/itcast-logistics/v1/realtime/api

![1614657642838](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614657642838.png)



## 10-[理解]-实时大屏系统之前端服务启动

> ​		数据服务接口应用已经运行，接下来启动前端页面，调用服务接口，从ClickHouse表查询数据，前端展示

> ​		查看【`logistics-service-provider`】模块中，查看dao层代码：`LogisticsRealtimeDao`，可以发现，实时大屏上指标都是通过SQL到ClickHouse表查询。

![1614657853174](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614657853174.png)

> 启动前端服务，在Windows系统启动，基于NodeJS开发
>
> - 1）、安装NodeJs：`node-v12.18.2-x64.msi`
> - 2）、解压前端项目：`project-kekuai-bigdata-kanban-nuxt-ts.rar`，进入目录，启动Shell命令行

![1614658054576](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614658054576.png)

> - 3）、启动前端项目

![1614658081546](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614658081546.png)

```ini
PS D:\NodeLinux\project-kekuai-bigdata-kanban-nuxt-ts> yarn build
yarn run v1.22.10
$ nuxt-ts build
i Production build                                                                                                          12:08:46
i Bundling only for client side                                                                                             12:08:46
i Target: static                                                                                                            12:08:46
√ Builder initialized                                                                                                       12:08:46
√ Nuxt files generated                                                                                                      12:08:48

 WARN  Browserslist: caniuse-lite is outdated. Please run:                                                                  12:08:52
npx browserslist@latest --update-db

i Starting type checking service...                                                                         nuxt:typescript 12:09:51
i Using 1 worker with 2048MB memory limit                                                                   nuxt:typescript 12:09:51

√ Client
  Compiled successfully in 1.63m


Hash: aa28e6f28487445b3a96
Version: webpack 4.44.0
Time: 97963ms
Built at: 2021/03/02 下午12:11:29
                             Asset      Size  Chunks                                Chunk Names
    ../server/client.manifest.json  52.9 KiB          [emitted]
                          LICENSES     1 KiB          [emitted]
                    app.68697f9.js   933 KiB       1  [emitted] [immutable]  [big]  app
            commons/app.d099092.js   196 KiB       2  [emitted] [immutable]         commons/app
commons/home.Home~index.5a7677f.js   135 KiB       0  [emitted] [immutable]         commons/home.Home~index
          elementui.app.066a957.js   856 KiB       3  [emitted] [immutable]  [big]  elementui.app
 fonts/DIGITALDREAMFAT.a918474.ttf  25.6 KiB          [emitted]
  fonts/element-icons.535877f.woff  27.5 KiB          [emitted]
   fonts/element-icons.732389d.ttf  54.6 KiB          [emitted]
                img/2x.871be4b.png  14.9 KiB          [emitted]
                img/bg.16b955a.png  4.89 MiB          [emitted]              [big]
                img/wg.4bf50aa.png   293 KiB          [emitted]              [big]
            pages/index.ae6a4ae.js  1.75 KiB       4  [emitted] [immutable]         pages/index
                runtime.bc69744.js  2.36 KiB       5  [emitted] [immutable]         runtime
 + 1 hidden asset
Entrypoint app [big] = runtime.bc69744.js elementui.app.066a957.js commons/app.d099092.js app.68697f9.js

WARNING in asset size limit: The following asset(s) exceed the recommended size limit (244 KiB).
This can impact web performance.
Assets:
  img/wg.4bf50aa.png (293 KiB)
  img/bg.16b955a.png (4.89 MiB)
  app.68697f9.js (933 KiB)
  elementui.app.066a957.js (856 KiB)

WARNING in entrypoint size limit: The following entrypoint(s) combined asset size exceeds the recommended limit (1000 KiB). This can impact web performance.
Entrypoints:
  app (1.94 MiB)
      runtime.bc69744.js
      elementui.app.066a957.js
      commons/app.d099092.js
      app.68697f9.js

i Generating output directory: dist/                                                                                        12:11:29
i Generating pages                                                                                                          12:11:30
√ Generated route "/"                                                                                                       12:11:30
√ Generated route "/home/Home"                                                                                              12:11:30
√ Client-side fallback created: 200.html                                                                                    12:11:30
Done in 189.57s.
PS D:\NodeLinux\project-kekuai-bigdata-kanban-nuxt-ts>
PS D:\NodeLinux\project-kekuai-bigdata-kanban-nuxt-ts> yarn start
yarn run v1.22.10
$ nuxt-ts start

   ╭─────────────────────────────────────────╮
   │                                         │
   │   Nuxt.js @ v2.13.3                     │
   │                                         │
   │   ▸ Environment: production             │
   │   ▸ Rendering:   client-side            │
   │   ▸ Target:      server                 │
   │                                         │
   │   Memory usage: 66.4 MB (RSS: 125 MB)   │
   │                                         │
   │   Listening: http://localhost:3000/     │
   │                                         │
   ╰─────────────────────────────────────────╯


```



> - 4）、访问Web页面：http://localhost:3000/

![1614658367932](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614658367932.png)





## 11-[复习]-物流项目回顾之项目整体概述

> 整个物流项目分为几章内容：每章内容有什么呢？？

![1614667103972](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614667103972.png)

> 划分9章内容为4个部分：
>
> - 第一部分、项目概述及Docker容器使用：2天
>   - 第1章和第2章
> - 第二部分、数据实时采集与ETL：4天
>   - 第3章至第5章
>   - 数据实时采集，分别使用OGG和Canal采集Oracle数据库和MySQL数据库表的数据到Kafka（重点）
>   - 实时存储引擎Kudu，基本架构，存储引擎诞生背景，Java Client API使用和集成Spark使用
>   - 数据实时ETL，存储至Kudu表（重点）
> - 第三部分、离线报表分析和实时即席查询：3天
>   - 第6章内容
>   - 离线报表SparkSQL：5个主题报表分析，按照离线数仓分层管理数据（ODS层、DWD层和DWS层）
>   - 即席查询Impala：实时分析引擎，基于内存分析数据，与Kudu集成就是一对CP
>   - 离线报表分析，需要任务调度引擎：Azkaban调度框架，定时调度和依赖调度
>   - Impala集成Hue，提供可视化界面，编写SQL，实时查询分析
>   - 物流信息查询，实时将快递单数据和运单数据同步至Es索引中：第7章
> - 第四部分、OLAP分析查询，使用ClickHouse数据库：4天
>   - 第8章和第9章内容
>   - ClickHouse数据库 入门使用
>   - ClickHouse 存储引擎及JDBC Client API使用（Java语言查询数据和整合Spark，进行DML和DDL操作）
>   - 自定义外部数据源，实现从ClickHouse数据库中批量加载数据、批量保存数据和流式保存数据
>   - 实时ETL存储ClickHouse表中
>   - 实时大屏展示：服务接口开发（功能含义）及大屏前端展示（NodeJS开发）
>
> [对整个物流项目来说，记住如下数据流向图：数据从哪里，到哪里去]()
>
> - 数据源：来源物流快递公司业务系统数据库（RDBMS），以MySQL数据库和Oracle数据库为例
>   - 数据采集，如何将业务数据同步采集到大数据存储引擎（Es、Kudu和ClickHouse）
>   - 实时采集，使用OGG和Canal
> - 数据存储：整个大数据项目来说，数据存储在哪里？？？
>   - 将业务数据存储到三个不同存储引擎：Kudu、Es和ClickHouse
> - 数据分析：离线报表分析（传统分析）、即席查询（业务不明确）和物流信息检索（Es服务接口），以及实时大屏展示（ClickHouse 查询统计）和数据服务接口（给外部系统数据）

![1610951797312](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610951797312.png)

> 物流项目来说，搭建开发测试环境（学习环境），将服务器分为：业务服务器和大数据服务器。
>
> - 1）、业务服务器：采用Docker容器部署安装数据库和采集框架
> - 2）、大数据服务器：单机版，基于CM6.2.1安装部署CDH 6.2.1框架

![1610951929699](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610951929699.png)

> 整个项目技术点，如下所示：

![1614668233263](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614668233263.png)



## 12-[复习]-物流项目回顾之业务模块【数据源】

> ​			此项目，数据都是存储在业务系统的数据库中，采用实时采集业务数据，将数据存储到Kafka Topic中
>
> [1个业务系统数据采集到1个topic中，不同业务系统topic分区数目不一样的]()

![1610952249225](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610952249225.png)

> 不同业务系统使用数据库不一样，以物流系统：Oracle数据库和CRM系统：MySQL数据库为例：
>
> - 1）、Oracle数据库存储业务数据时，采用OGG采集，OGG中间件，实时采集数据架构图如下所示：
>   - 源端SRC：Extract进程、LocalTrail、Pump发送数据文件到目标端
>   - 目标端TRT：Collector进程、RemoteTrail、Replicat进程读取数据，发送给Kafka

![1610952322599](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610952322599.png)

> - 2）、MySQL数据库存储业务数据时，采用Canal采集，属于阿里开源框架，新版本1.1.0才支持将数据发送到消息队列，比如Kafka Topic
>
>   [CanalServer原理：伪装MySQL数据库小弟Slave，通过pump协议发送请求，获取binlog日志，解析日志数据，以JSON格式发送给Kafka Topic]()
>
>   ==必须掌握==，尤其在国内，新版本提供集群高可用HA和WebUI界面管理服务。

![1610952370334](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610952370334.png)

> ==扩展：==国外使用MaxWell框架，类似Canal，实时采集MySQL数据库表的数据，http://maxwells-daemon.io/

![1614669104986](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614669104986.png)





## 13-[复习]-物流项目回顾之业务模块【实时ETL存储】

> ​		当业务数据实时采集到Kafka 消息队列以后，需要开发流式计算程序，实时ETL转换，存储到存储引擎：
>
> - 1）、流式计算程序：StructuredStreaming，结构化流程序
>
>   [为什么选择StructuredStreaming？？？？三点原因]()
>
> - 2）、存储引擎：Kudu数据库（类似HBase数据库）、Es索引（物流信息）和ClickHouse（OLAP分析）

![1610952615641](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610952615641.png)

> 实时ETL流程：==对于流式计算程序，程序代码为三个部分==
>
> - 1）、数据源Source：从Kafka消费数据，封装方法：`load`
> - 2）、数据转换Transformation：对消费数据进行转换ETL操作，封装方法：`process`
> - 3）、数据终端Sink：将转换后数据存储，封装方法：`save`

![1610934873712](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610934873712.png)

> 在这个ETL过程中，分为两个步骤完成：
>
> - 1）、将Kafka获取Message数据JSON字符串，解析为OGG和Canal数据格式对应Bean对象
>
>   ==JSON -> MessageBean，使用FastJSON库，Bean对象使用Java语言==
>
> - 2）、从Bean对象提取沪操作的数据，解析封装到POJO对象
>
>   ==Bean对象 -> POJO对象（数据库中每张表对应POJO==



## 14-[复习]-物流项目回顾之业务模块【离线报表及即席查询】

> ​		物流项目中，业务数据主要存储引擎之一就是Kudu 数据库，进行离线报表分析和即席查询。
>
> - 1）、离线报表分析：编写SparkSQL程序，加载Kudu表数据，分析以后，存储到Kudu表
>   - 按照数据仓库分层管理数据：ODS层、DWD层（数据明细层）和DWS层（数据服务层）
>   - 依据主题开发报表分析，讲解5个主题报表开发（快递单、运单、仓库、车辆和客户）
>   - 每个主题报表开发步骤：
>     - 第1步、开发DWD层，将事实表数据与相关维度表数据进行关联（leftJoin），拉宽操作，存储到Kudu表中（DWD层）
>     - 第2步、开发DWS层，读取DWD层宽表数据，按照指标进行计算统计，将结果保存至Kudu表（DWS层）
>   - 各个主题报表开发完成以后，需要每日调度执行，使用Azkaban调度

![1610954248445](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610954248445.png)



> - 2）、即席查询
>   - 针对物流公司来说，很多时候，有一些临时需求报表分析，依据条件快速查询分析数据
>   - 基于内存分析引擎框架：Impala，与Kudu集成，性能非常好

![1610952833463](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610952833463.png)

> ​		即席查询，往往就是编写SQL语句，要么是开发人员自己书写，要么就是程序依据条件拼凑SQL语句执行，所以将Hue与Impala集成，提供编写SQL语句，方便进行即席查询分析。
>

![1610953087109](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610953087109.png)



## 15-[复习]-物流项目回顾之业务模块【快递物流查询】

> ​		针对物流快递来说，需要给用户提供接口，实时依据`快递单号或者运单号`，查询快递物流信息，选择使用框架：Elasticsearch 索引Index。

![1610953223680](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610953223680.png)

> 实时将业务数据同步到Es索引时，仅仅需要物流系统中：`运单数据和快递单数据`即可。
>
> - 1）、结构化流与Es集成，官方提供库，直接使用即可
> - 2）、业务数据存储Es索引以后，还可以进行一些复杂查询，此次使用Kibana简易报表和图表展示
> - 3）、Es索引中，存储的都是最近业务数据，不存储历史业务数据
>   - ==比如，快递单表和运单表存储最近3个月数据，或者1个月数据。==



## 16-[复习]-物流项目回顾之业务模块【OLAP 分析】

> 将物流业务数据存储至`ClickHouse`分析数据库中，一次写入（批量写入）多次查询。
>
> - 1）、需要从海量数据进行OLAP分析，实时查询，
>   - [近几年ClickHouse分析数据库使用最为广泛，存储数据量很大，列式存数据，支持分布式集群存储]()
> - 2）、物流系统，往往与第三方合作，需要提供服务接口给第三方，导出向数据
>   - [当第三方需要数据时（业务数据），快速查询出来，导出给用户]()
> - 3）、实时大屏展示
>   - [每隔10秒，从数据库查询结果数据，展示大屏中]()

![1610953644714](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610953644714.png)



> 扩展：近几年比较火框架`Doris`，目前已经Apache顶级项目，http://doris.apache.org/master/zh-CN/

![1614671875608](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1614671875608.png)



## 17-[复习]-物流项目回顾之核心业务技术点

> - 1）、数据实时ETL，编写流式应用程序，从Kafka消费数据，进行ETL转换（DSL），存储不同外部数据库
>   - 编写公共接口：`BaseStreamApp`，三个方法（`load、process和save`）
>   - 掌握：实时ETL至Kudu程序【`KuduStreamApp`】，第5天和第6天

![1610954594812](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610954594812.png)

> - 2）、离线报表分析，编写SparkSQL程序，针对每个主题报表开发
>
>   - 数仓架构分层管理数据，每个主题报表开发2个程序：DWD层拉宽和DWS层计算
>
>   - DWS层指标计算，全量加载数据还是增量加载数据，都是按照每天统计，最终转换为DataFrame
>
>     客户主题报表分析，指标统计，最好深入理解，背下来

![1610954667325](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610954667325.png)

> - 3）、自定义SparkSQL外部数据源，实现从ClickHouse数据库批量加载和保存及流式数据保存。
>   - 实现类继承图，有所了解，会使用定义好API。
>   - 基础较好的同学，整个实现过程，最好理解掌握，核心步骤关键点，简历写上

![1610954707140](https://assents-out.oss-cn-shenzhen.aliyuncs.com/img/1610954707140.png)



## 18-[复习]-物流项目回顾之技术面试题

> 由于物流项目基于Spark框架实时采集数据、离线报表分析及Impala即席查询和ClickHouse OALP分析

![1610956101241](/../03_%E7%AC%94%E8%AE%B0/img/1610956101241.png)

> - 1、数据采集如何完成
>   - [OGG 不要涉及，Oracle DBA完成]()
>   - Canal数据采集，一定知道高可用HA集群模式
> - 2、数据量大小
>   - Kafka topic 数据存储生命周期（多久）
>   - Kafka Topic 个数及分区数和副本
>   - Kafka 集群规模及机器配置
> - 3、实时增量ETL程序开发，为什么选择使用StructuredStreaming？？
>   - [重点知识点]()
> - 4、消费Kafka数据几种方式及区别，如何保存偏移量？
>   - SparkStreaming Checkpoint或自己管理
>   - StructuredStreaming 使用Checkpoint管理
> - 5、为什么使用Kudu存储，不使用HBase？？
>   - 两者区别？？
>   - Kudu中数据读写流程
>   - Kudu如何存储数据，每个表分区策略？？？
>   - Kudu使用注意事项，Kudu集群对时间同步极其严格
> - 6、DataFrame与Dataset、RDD区别
>   - RDD 特性有哪些？？你是如何理解RDD的？？？
>   - 为什么Spark计算比较快，与MapReduce相比较优势是什么？？、
>   - SparkSQL中优化有哪些？？？使用常见函数有哪些？？？
> - 7、Impala分析引擎
>   - Impala架构，实现目的，目前架构如何
>   - Hue与Impala集成
> - 8、离线数仓
>   - 数仓分层如何划分呢？？？为什么要划分？？为什么要如此设计？？？
>   - 雪花模型和星型模型区别是什么？？？？
> - 9、ClickHouse 为什么选择，有哪些优势？？
> - 10、SparkSQL外部数据源实现（难点）
> - 11、大数据平台集群规模、服务配置、项目人员安排、项目开发周期等等
> - 12、业务线：你完成什么，你做了什么，你遇到什么问题，你是如何解决的？？？？



```
1、项目描述：三四句话（三句话最好）

2、项目职责：你完成什么，你做了什么，你遇到什么问题，你是如何解决的？？？？
	规划范化书写，条理性
	[数据是什么、实现什么功能、怎么实现..........................]
```











