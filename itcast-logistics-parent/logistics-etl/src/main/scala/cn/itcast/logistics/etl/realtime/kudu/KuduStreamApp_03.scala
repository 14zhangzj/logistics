package cn.itcast.logistics.etl.realtime.kudu

import cn.itcast.logistics.common.beans.crm.AddressBean
import cn.itcast.logistics.common.beans.logistics.AreasBean
import cn.itcast.logistics.common.beans.parser.{CanalMessageBean, OggMessageBean}
import cn.itcast.logistics.common.{Configuration, SparkUtils, TableMapping}
import cn.itcast.logistics.etl.parser.DataParser
import cn.itcast.logistics.etl.realtime.BasicStreamApp
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql._

/**
 * Kudu数据管道应用：实现Kudu数据库的实时ETL操作
 */
object KuduStreamApp_03 extends BasicStreamApp {
	
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
				import cn.itcast.logistics.common.BeanImplicits._
				val pojoStreamDS: Dataset[AreasBean] = beanStreamDS
					// 过滤获取AreasBean数据
					.filter(oggBean => oggBean.getTable == TableMapping.AREAS)
					// 提取数据字段值，进行封装为POJO
					.map(oggBean => DataParser.toAreas(oggBean))
					// 过滤不为null
					.filter(pojo => null != pojo)
				// 第三步、c. 返回转换后数据
				pojoStreamDS.toDF()
				
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
		
		// step3. 对流式数据进行实时ETL转换操作
		val logisticsEtlStreamDF = process(logisticsStreamDF, "logistics")
		val crmEtlStreamDF = process(crmStreamDF, "crm")
		
		// step4. 将转换后数据进行输出
		save(logisticsEtlStreamDF, "tbl_logistics")
		save(crmEtlStreamDF, "tbl_crm")
		
		// step5. 启动流式应用后，等待终止
		spark.streams.active.foreach(query => println(s"正在启动Query： ${query.name}"))
		spark.streams.awaitAnyTermination()
	}
}
