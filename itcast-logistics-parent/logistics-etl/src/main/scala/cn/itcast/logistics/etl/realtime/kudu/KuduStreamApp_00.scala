package cn.itcast.logistics.etl.realtime.kudu

import cn.itcast.logistics.common.{Configuration, SparkUtils}
import cn.itcast.logistics.etl.realtime.BasicStreamApp
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Kudu数据管道应用：实现Kudu数据库的实时ETL操作
 */
object KuduStreamApp_00 extends BasicStreamApp {
	
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
