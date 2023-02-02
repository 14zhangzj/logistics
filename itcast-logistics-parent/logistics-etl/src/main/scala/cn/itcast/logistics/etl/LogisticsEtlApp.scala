package cn.itcast.logistics.etl

import cn.itcast.logistics.common.Configuration
import org.apache.commons.lang3.SystemUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 测试程序：实时从Kafka消费数据（物流系统和CRM系统业务数据），将数据打印在控制台，没有任何逻辑
 */
object LogisticsEtlApp {
	
	def main(args: Array[String]): Unit = {
		// step1. 构建SparkSession实例对象，设置相关属性
		// 1. 初始化设置Spark Application配置
		val sparkConf: SparkConf = new SparkConf()
			.set("spark.sql.session.timeZone", "Asia/Shanghai")
			.set("spark.sql.files.maxPartitionBytes", "134217728")
			.set("spark.sql.files.openCostInBytes", "134217728")
			.set("spark.sql.autoBroadcastJoinThreshold", "67108864")
		
		// 2. 判断Spark Application运行模式进行设置
		if (SystemUtils.IS_OS_WINDOWS || SystemUtils.IS_OS_MAC) {
			//本地环境LOCAL_HADOOP_HOME
			System.setProperty("hadoop.home.dir", Configuration.LOCAL_HADOOP_HOME)
			//设置运行环境和checkpoint路径
			sparkConf
				.set("spark.master", "local[*]")
				.set("spark.sql.shuffle.partitions", "4")
				.set("spark.sql.streaming.checkpointLocation", Configuration.SPARK_APP_WIN_CHECKPOINT_DIR)
		} else {
			//生产环境，提交应用：spark-submit --deploy-mode cluster --conf spark.sql.shuffle.partitions=10 xx.jar ...
			sparkConf
				.set("spark.master", "yarn")
				.set("spark.sql.streaming.checkpointLocation", Configuration.SPARK_APP_DFS_CHECKPOINT_DIR)
		}
		// 3. 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.config(sparkConf)
			.getOrCreate()
		import spark.implicits._
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		
		// step2. 从Kafka数据源加载业务数据（CRM系统：crm和物流系统：logistics）
		// step3. 实时数据ETL转换操作
		// step4. 将数据打印到控制台Console
		// 4. 初始化消费物流Topic数据参数
		val logisticsStreamDF: DataFrame = spark
			.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", Configuration.KAFKA_ADDRESS)
			.option("subscribe", Configuration.KAFKA_LOGISTICS_TOPIC)
			.load()
		// 5. 消费物流Topic数据，打印控制台
		val logisticsQuery = logisticsStreamDF
			.selectExpr("CAST(value AS STRING)")
			.writeStream
			.outputMode(OutputMode.Append())
			.queryName("query-logistics")
			.format("console")
			.option("numRows", "10")
			.option("truncate", "false")
			.start()
		
		// 6. 初始化消费CRM Topic数据参数
		val crmStreamDF: DataFrame = spark
			.readStream
			.format("kafka")
			.option("kafka.bootstrap.servers", Configuration.KAFKA_ADDRESS)
			.option("subscribe", Configuration.KAFKA_CRM_TOPIC)
			.load()
		// 7. 消费CRM Topic数据，打印控制台
		val crmQuery = crmStreamDF
			.selectExpr("CAST(value AS STRING)")
			.writeStream
			.outputMode(OutputMode.Append())
			.queryName("query-crm")
			.format("console")
			.option("numRows", "10")
			.option("truncate", "false")
			.start()

		// step5. 启动流式应用，需要等待终止
		// 8. 启动流式应用，等待终止
		spark.streams.active.foreach(query => println(s"正在启动Query： ${query.name}"))
		spark.streams.awaitAnyTermination()
	}
	
}
