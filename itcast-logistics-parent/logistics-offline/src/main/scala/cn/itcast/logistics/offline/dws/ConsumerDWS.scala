package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common.OfflineTableDefine
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}

/**
 * 客户主题指标开发：
 *      从Kudu表加载客户详情宽表数据，按照具体业务进行统计分析，最后结果存储Kudu中DWS层。
 */
object ConsumerDWS extends AbstractOfflineApp{
	/**
	 * 对数据集DataFrame按照业务需求编码，对宽表数据按照指标进行计算
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		val session = dataframe.sparkSession
		import session.implicits._
		
		// step1. 直接进行指标计算，无需按照划分数据
		/*
			指标计算时，需要计算每个用户下单日期距今天数，两个日期相差天数，此时使用日期时间函数
				now() / current_date() / current_timestamp()
				date_sub() / date_add()
				date_format()
				date_diff()
		 */
		// 指标一：总客户数
		val customerCount: Long = dataframe.count()
		// 指标二：今日新增客户数(注册时间为今天)
		val additionCustomerCount: Long = dataframe
			.where(
				//  substring($"regdt", 0, 10) === date_sub(current_date(), 1)
				date_format($"regdt", "yyyy-MM-dd") === date_sub(current_date(), 1)
			)
			.count()
		// 指标三：留存数(超过180天未下单表示已流失，否则表示留存) 和留存率
		val preserveCustomerCount: Long = dataframe
			//.where($"last_sender_cdt" >= date_sub(current_date(), 180))
			.where(
				datediff(current_date(), $"last_sender_cdt") <= 180
			)
			.count()
		val preserveRate: Double = preserveCustomerCount / customerCount.toDouble
		// 指标四：活跃用户数(近10天内有发件的客户表示活跃用户)
		val activeCustomerCount: Long = dataframe
			.where($"last_sender_cdt" >= date_sub(current_date(), 10))
			.count()
		// 指标五：月度新用户数
		val monthOfNewCustomerCount: Long = dataframe
			.where(
				$"regDt".between(
					trunc(current_timestamp(), "month"), date_format(current_date(), "yyyy-MM-dd")
				)
			)
			.count()
		// 指标六：沉睡用户数(3个月~6个月之间的用户表示已沉睡)
		val sleepCustomerCount = dataframe
			.where(
				"last_sender_cdt >= date_sub(now(), 180) and last_sender_cdt <= date_sub(now(), 90)"
			)
			.count()
		// 指标七：流失用户数(9个月未下单表示已流失)
		val loseCustomerCount: Long = dataframe
			.where("last_sender_cdt <= date_sub(now(), 270)")
			.count()
		// 指标八：客单价、客单数、平均客单数
		val customerBillDF: DataFrame = dataframe.where("first_sender_id is not null")
		// 客单数：下单顾客数目
		val customerBillCount: Long = customerBillDF.count()
		// 客单价 = 总金额/总单数
		val customerBillAggDF: DataFrame = customerBillDF.agg(
			sum($"totalCount").as("sumCount"), // 总单数
			sum($"totalAmount").as("sumAmount") // 总金额
		)
		val billAggRow: Row = customerBillAggDF.first()
		// 客单价 = 总金额/总单数
		val customerAvgAmount: Double = billAggRow.getAs[Double]("sumAmount") / billAggRow.getAs[Long]("sumCount")
		// 平均客单数：平均每个顾客下单数目
		val avgCustomerBillCount: Double = billAggRow.getAs[Long]("sumCount") / customerBillCount.toDouble
		
		// TODO： 需要将计算所有指标结果提取出来，并且组合到Row对象中
		val dayValue: String = dataframe
			.select(
				date_format(current_date(), "yyyyMMdd").cast(StringType)
			)
			.limit(1).first().getAs[String](0)
		val aggRow: Row = Row(
			dayValue, //
			customerCount, //
			additionCustomerCount, //
			preserveRate, //
			activeCustomerCount, //
			monthOfNewCustomerCount, //
			sleepCustomerCount, //
			loseCustomerCount, //
			customerBillCount, //
			customerAvgAmount, //
			avgCustomerBillCount
		)
		
		// step2. 转换数据为DataFrame数据集
		// 第一步、将列表转换为RDD
		val rowsRDD: RDD[Row] = session.sparkContext.parallelize(Seq(aggRow)) // 将可变集合对象转换为不可变的
		// 第二步、自定义Schema信息
		val aggSchema: StructType = new StructType()
			.add("id", StringType, nullable = false) // 针对每天数据进行聚合得到一个结果，设置day为结果表中id
			.add("customerCount", LongType, nullable = true)
			.add("additionCustomerCount", LongType, nullable = true)
			.add("preserveRate", DoubleType, nullable = true)
			.add("activeCustomerCount", LongType, nullable = true)
			.add("monthOfNewCustomerCount", LongType, nullable = true)
			.add("sleepCustomerCount", LongType, nullable = true)
			.add("loseCustomerCount", LongType, nullable = true)
			.add("customerBillCount", LongType, nullable = true)
			.add("customerAvgAmount", DoubleType, nullable = true)
			.add("avgCustomerBillCount", DoubleType, nullable = true)
		
		// 第三步、调用SparkSession中createDataFrame方法，组合RowsRDD和Schema为DataFrame
		val aggDF: DataFrame = session.createDataFrame(rowsRDD, aggSchema)
		
		// step3. 返回指标计算结果
		aggDF
	}
	
	def main(args: Array[String]): Unit = {
		execute(
			this.getClass, //
			OfflineTableDefine.CUSTOMER_DETAIL, //
			OfflineTableDefine.CUSTOMER_SUMMERY, //
			isLoadFullData = true //
		)
	}
}
