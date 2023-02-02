package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common._
import cn.itcast.logistics.offline.BasicOfflineApp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
 * 快递单主题开发：
 *      加载Kudu中快递单宽表：dwd_tbl_express_bill_detail 数据，按照业务进行指标统计
 */
object ExpressBillDWS extends BasicOfflineApp{
	/**
	 * 数据处理，对宽表进行指标计算
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		// 获取SparkSession实例对象并导入隐式转换
		val spark: SparkSession = dataframe.sparkSession
		import spark.implicits._
		
		/**
		    考虑加载宽表数据是否是全量数据，如果是，需要按照每天day划分
		 */
		// 定义一个列表，存储每天计算指标，类型Row
		val rowList: ListBuffer[Row] = new  ListBuffer[Row]()
		dataframe.select($"day").distinct().collect().foreach{dayRow =>
			// 获取日期day值
			val dayValue: String = dayRow.getString(0) // 20210118,20210119,20210120
			
			// 获取每天宽表数据
			val expressBillDetailDF: DataFrame = dataframe.filter($"day" === dayValue)
			// 由于后续使用多次，可以进行缓存
			expressBillDetailDF.persist(StorageLevel.MEMORY_AND_DISK)
			
			// 指标计算
			// 指标一、总快递单数
			val totalDF: DataFrame = expressBillDetailDF.agg(count($"id").as("total"))
			
			// 指标二、各类客户快递单数：最大、最小和平均
			val aggTypeTotalDF: DataFrame = expressBillDetailDF.groupBy($"type").count()
			val typeTotalDF: DataFrame = aggTypeTotalDF.agg(
				max($"count").as("maxTypeTotal"), //
				min($"count").as("minTypeTotal"), //
				round(avg($"count"), 0).as("avgTypeTotal")//
			)
			
			// 指标三、各网点快递单数：最大、最小和平均
			val aggDotTotalDF: DataFrame = expressBillDetailDF.groupBy($"dot_id").count()
			val dotTotalDF: DataFrame = aggDotTotalDF.agg(
				max($"count").as("maxDotTotal"), //
				min($"count").as("minDotTotal"), //
				round(avg($"count"), 0).as("avgDotTotal")//
			)
			
			// 指标四、各渠道快递单数：最大、最小和平均
			val aggChannelTotalDF: DataFrame = expressBillDetailDF.groupBy($"order_channel_id").count()
			val channelTotalDF: DataFrame = aggChannelTotalDF.agg(
				max($"count").as("maxChannelTotal"), //
				min($"count").as("minChannelTotal"), //
				round(avg($"count"), 0).as("avgChannelTotal")//
			)
			
			// 指标五、各终端快递单数：最大、最小和平均
			val aggTerminalTotalDF: DataFrame = expressBillDetailDF.groupBy($"order_terminal_type").count()
			val terminalTotalDF: DataFrame = aggTerminalTotalDF.agg(
				max($"count").as("maxTerminalTotal"), //
				min($"count").as("minTerminalTotal"), //
				round(avg($"count"), 0).as("avgTerminalTotal")//
			)
			
			// 数据不再使用，释放资源
			expressBillDetailDF.unpersist()
			
			// 将每天统计各个指标封装到Row对象中
			/*
				在SparkSQL中Row创建方式？？？
				-1. Row(v1, v2, v3, ...)
				-2. Row.fromSeq(Seq(v1, v2, v3, ...))
			 */
			val aggRow: Row = Row.fromSeq(
				Seq(dayValue) ++
				totalDF.first().toSeq ++
					typeTotalDF.first().toSeq ++
					dotTotalDF.first().toSeq ++
					channelTotalDF.first().toSeq ++
					terminalTotalDF.first().toSeq
			)
			// 加入列表
			rowList += aggRow
		}
		
		// TODO: 将ListBuffer转换为DataFrame，首先转换列表为RDD，然后自定义Schame，最后转换为DataFrame
		// step1. 采用并行化方式转换ListBuffer为RDD
		val rowRDD: RDD[Row] = spark.sparkContext.parallelize(rowList)
		// step2. 自定义Schema信息，必须与Row字段类型匹配
		val schema: StructType = new StructType()
    		.add("id", StringType, nullable = false)
			.add("total", LongType, nullable = true)
			.add("maxTypeTotal", LongType, nullable = true)
			.add("minTypeTotal", LongType, nullable = true)
			.add("avgTypeTotal", DoubleType, nullable = true)
			.add("maxDotTotal", LongType, nullable = true)
			.add("minDotTotal", LongType, nullable = true)
			.add("avgDotTotal", DoubleType, nullable = true)
			.add("maxChannelTotal", LongType, nullable = true)
			.add("minChannelTotal", LongType, nullable = true)
			.add("avgChannelTotal", DoubleType, nullable = true)
			.add("maxTerminalTotal", LongType, nullable = true)
			.add("minTerminalTotal", LongType, nullable = true)
			.add("avgTerminalTotal", DoubleType, nullable = true)
		// step3. 转换RDD[Row]为DataFrame
		val aggDF: DataFrame = spark.createDataFrame(rowRDD, schema)
		
		// 返回计算指标
		aggDF
	}
	
	// SparkSQL 程序入口
	def main(args: Array[String]): Unit = {
		// step1. 创建SparkSession对象，传递SparkConf对象
		val spark: SparkSession = SparkUtils.createSparkSession(
			SparkUtils.autoSettingEnv(SparkUtils.sparkConf()), this.getClass
		)
		import spark.implicits._
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
		// step2. 加载Kudu中的宽表数据
		val expressBillDetailDF: DataFrame = load(
			spark, OfflineTableDefine.EXPRESS_BILL_DETAIL, isLoadFullData = Configuration.IS_FIRST_RUNNABLE
		)
		expressBillDetailDF.show(10, truncate = false)
		// step3.  按照业务进行指标计算
		val expressBillSummaryDF: DataFrame = process(expressBillDetailDF)
		expressBillSummaryDF.show(10, truncate = false)
		// step4. 将拉宽后的数据再次写回到Kudu数据库中
		save(expressBillSummaryDF, OfflineTableDefine.EXPRESS_BILL_SUMMARY)
		// 应用结束，关闭资源
		spark.stop()
	}
}
