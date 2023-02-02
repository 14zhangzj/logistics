package cn.itcast.logistics.etl.realtime.es

import cn.itcast.logistics.common.BeanImplicits._
import cn.itcast.logistics.common.beans.parser.OggMessageBean
import cn.itcast.logistics.common.{Configuration, SparkUtils, TableMapping}
import cn.itcast.logistics.etl.parser.DataParser
import cn.itcast.logistics.etl.realtime.BasicStreamApp
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * Elasticsearch 数据管道应用：实现Elasticsearch数据库的实时ETL操作
 */
object EsStreamApp_01 extends BasicStreamApp {
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
