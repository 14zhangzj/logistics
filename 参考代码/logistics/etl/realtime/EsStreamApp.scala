package cn.itcast.logistics.etl.realtime
import cn.itcast.logistics.common.BeanImplicits._
import cn.itcast.logistics.common.beans.parser.OggMessageBean
import cn.itcast.logistics.common.{Configuration, SparkUtils, TableMapping}
import cn.itcast.logistics.etl.parser.DataParser
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

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
	override def process(streamDF: DataFrame, category: String): DataFrame = {
		// 导入隐式转换
		import streamDF.sparkSession.implicits._
		
		// 判断类型转换数据：将Message（JSON字符串）转换为Bean对象（OggMessageBean实例）
		category match {
			// TODO： 物流业务系统数据ETL处理
			case "logistics" => {
				// 1. 对获取OGG同步物流数据，转换封装MessageBean对象
				val oggBeanStreamDS: Dataset[OggMessageBean] = streamDF
					// 将DataFrame转换为Dataset，由于DataFrame中只有一个字段value，类型是String
					.as[String]
					// 对Dataset中每个分区数据进行操作，每个分区数据封装在迭代器中
					.mapPartitions { iter =>
						iter
							.filter(jsonStr => null != jsonStr && jsonStr.trim.length > 0) // 过滤数据
							// 解析JSON格式数据, 使用FastJson类库
							.map(jsonStr => JSON.parseObject(jsonStr, classOf[OggMessageBean]))
					}
				// 2. 返回转换后的数据
				oggBeanStreamDS.toDF()
			}
			// 其他业务数据ETL处理
			case _ => streamDF
		}
	}
	
	/**
	 * 数据的保存至Elasticsearch索引中，如果索引不存在，设置自动创建（默认情况）
	 *
	 * @param streamDF          保存数据集DataFrame
	 * @param indexName         保存表的名称
	 * @param isAutoCreateTable 是否自动创建索引，默认创建
	 */
	override def save(streamDF: DataFrame, indexName: String, isAutoCreateTable: Boolean = true): Unit = {
		streamDF
			.drop(col("opType"))
			.writeStream
			.outputMode(OutputMode.Append())
			.queryName(s"query-${indexName}")
			.format(Configuration.SPARK_ES_FORMAT)
			.option("es.nodes", Configuration.ELASTICSEARCH_HOST)
			.option("es.port", Configuration.ELASTICSEARCH_HTTP_PORT + "")
			.option("es.index.auto.create", isAutoCreateTable)
			.option("es.write.operation", "upsert")
			.option("es.mapping.id", "id")
			.start(indexName) // 参数含义：Elasticsearch中索引名称
	}
	
	/**
	 * 物流Logistics系统业务数据ETL转换处理及保存外部存储
	 *
	 * @param streamDF 流式数据集StreamingDataFrame
	 */
	def saveToEs(streamDF: DataFrame): Unit = {
		
		// 转换DataFrame为Dataset
		val streamDS: Dataset[OggMessageBean] = streamDF.as[OggMessageBean]
		
		// 快递单
		val expressBillDF: DataFrame = streamDS
			.filter(bean => bean.getTable == TableMapping.EXPRESS_BILL)
			.map(bean => DataParser.toExpressBill(bean))
			.toDF()
		//save(expressBillDF, TableMapping.EXPRESS_BILL)
		save(expressBillDF,"index_express_bill")
		
		// 运单
		val waybillDF: DataFrame = streamDS
			.filter(bean => bean.getTable == TableMapping.WAY_BILL)
			.map(bean => DataParser.toWaybill(bean))
			.toDF()
		//save(waybillDF, TableMapping.WAY_BILL)
		save(waybillDF, "index_way_bill")
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
		
		// 3. 数据处理，调用process方法
		val logisticsEtlStreamDF: DataFrame = process(logisticsStreamDF, "logistics")
		
		// 4. 数据输出，调用save方法
		saveToEs(logisticsEtlStreamDF)
		
		// 5. 等待应用终止
		spark.streams.active.foreach(query => println(s"准备启动查询Query： ${query.name}"))
		spark.streams.awaitAnyTermination()
	}
}
