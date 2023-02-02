package cn.itcast.logistics.offline.dws

import cn.itcast.logistics.common.{Configuration, OfflineTableDefine}
import cn.itcast.logistics.offline.AbstractOfflineApp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
 * 运单主题指标开发：
 * 从Kudu表加载宽表数据，按照业务指标进行统计分析：基于不同维度分组聚合，类似快递单指表指标。
 */
object WayBillDWS extends AbstractOfflineApp {
	/**
	 * 对数据集DataFrame按照业务需求编码，由子类复杂实现
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	override def process(dataframe: DataFrame): DataFrame = {
		// 此时dataframe为 宽表数据，其中有字段day
		
		// 获取SparkSession实例对象
		val session = dataframe.sparkSession
		import session.implicits._
		
		/*
			无论是全量数据还是增量数据，都是按照day进行划分指标计算，将每天指标计算封装到Row中，存储到列表ListBuffer中
		 */
		// step1. 按天day划分业务数据，计算指标，封装为Row对象
		val rowList: ListBuffer[Row] = new ListBuffer[Row]()
		dataframe.select($"day").distinct().collect().foreach{dayRow =>
			// 获取day值
			val dayValue: String = dayRow.getString(0)
			
			// 过滤获取每天运单表数据
			val wayBillDetailDF: DataFrame = dataframe.filter($"day".equalTo(dayValue))
			wayBillDetailDF.persist(StorageLevel.MEMORY_AND_DISK)
			
			// 计算指标
			// 指标一、总运单数
			val totalDF: DataFrame = wayBillDetailDF.agg(count($"id").as("total"))
			
			// 指标二、各区域运单数：最大、最小和平均
			val areaTotalDF: DataFrame = wayBillDetailDF.groupBy($"area_id").count()
			val areaAggTotalDF: DataFrame = areaTotalDF.agg(
				max($"count").as("maxAreaTotal"), //
				min($"count").as("minAreaTotal"), //
				round(avg($"count"), 0).as("avgAreaTotal") //
			)
			
			// 指标三、各分公司运单数：最大、最小和平均
			val companyTotalDF: DataFrame = wayBillDetailDF.groupBy($"sw_company_name").count()
			val companyAggTotalDF: DataFrame = companyTotalDF.agg(
				max($"count").as("maxCompanyTotal"), //
				min($"count").as("minCompanyTotal"), //
				round(avg($"count"), 0).as("avgCompanyTotal") //
			)
			
			// 指标四、各网点运单数：最大、最小和平均
			val dotTotalDF: DataFrame = wayBillDetailDF.groupBy($"dot_id").count()
			val dotAggTotalDF: DataFrame = dotTotalDF.agg(
				max($"count").as("maxDotTotal"), //
				min($"count").as("minDotTotal"), //
				round(avg($"count"), 0).as("avgDotTotal") //
			)
			
			// 指标五、各线路运单数：最大、最小和平均
			val routeTotalDF: DataFrame = wayBillDetailDF.groupBy($"route_id").count()
			val routeAggTotalDF: DataFrame = routeTotalDF.agg(
				max($"count").as("maxRouteTotal"), //
				min($"count").as("minRouteTotal"), //
				round(avg($"count"), 0).as("avgRouteTotal") //
			)
			
			// 指标六、各运输工具运单数：最大、最小和平均
			val ttTotalDF: DataFrame = wayBillDetailDF.groupBy($"tt_id").count()
			val ttAggTotalDF: DataFrame = ttTotalDF.agg(
				max($"count").as("maxTtTotal"), //
				min($"count").as("minTtTotal"), //
				round(avg($"count"), 0).as("avgTtTotal") //
			)
			
			// 指标七、各客户类型运单数：最大、最小和平均
			val typeTotalDF: DataFrame = wayBillDetailDF.groupBy($"ctype").count()
			val typeAggTotalDF: DataFrame = typeTotalDF.agg(
				max($"count").as("maxTypeTotal"), //
				min($"count").as("minTypeTotal"), //
				round(avg($"count"), 0).as("avgTypeTotal") //
			)
			
			// 数据不再使用，释放缓存
			wayBillDetailDF.unpersist()
			
			// 封装指标到Row对象
			val aggRow = Row.fromSeq(
				dayRow.toSeq ++
					totalDF.first().toSeq ++
					areaAggTotalDF.first().toSeq ++
					companyAggTotalDF.first().toSeq ++
					dotAggTotalDF.first().toSeq ++
					routeAggTotalDF.first().toSeq ++
					ttAggTotalDF.first().toSeq ++
					typeAggTotalDF.first().toSeq
			)
			// 加入到列表中
			rowList += aggRow
		}
		
		// step2. 将计算指标，存储到DataFrame中
		// a. RDD[Row]
		val rowRDD: RDD[Row] = session.sparkContext.parallelize(rowList.toList)
		// b. 自定义Schema
		val schema: StructType = new StructType()
			.add("id", StringType, nullable = false) // 针对每天数据进行聚合得到一个结果，设置day为结果表中id
			.add("total", LongType, nullable = true)
			.add("maxAreaTotal", LongType, nullable = true)
			.add("minAreaTotal", LongType, nullable = true)
			.add("avgAreaTotal", DoubleType, nullable = true)
			.add("maxCompanyTotal", LongType, nullable = true)
			.add("minCompanyTotal", LongType, nullable = true)
			.add("avgCompanyTotal", DoubleType, nullable = true)
			.add("maxDotTotal", LongType, nullable = true)
			.add("minDotTotal", LongType, nullable = true)
			.add("avgDotTotal", DoubleType, nullable = true)
			.add("maxRouteTotal", LongType, nullable = true)
			.add("minRouteTotal", LongType, nullable = true)
			.add("avgRouteTotal", DoubleType, nullable = true)
			.add("maxToolTotal", LongType, nullable = true)
			.add("minToolTotal", LongType, nullable = true)
			.add("avgToolTotal", DoubleType, nullable = true)
			.add("maxCtypeTotal", LongType, nullable = true)
			.add("minCtypeTotal", LongType, nullable = true)
			.add("avgCtypeTotal", DoubleType, nullable = true)
		// c. 构建DataFrame
		val aggDF: DataFrame = session.createDataFrame(rowRDD, schema)
		
		// step3. 返回计算指标数据集
		aggDF
	}
	
	def main(args: Array[String]): Unit = {
		// 调用模板方法，传递参数
		execute(
			this.getClass, //
			OfflineTableDefine.WAY_BILL_DETAIL, //
			OfflineTableDefine.WAY_BILL_SUMMARY, //
			isLoadFullData = Configuration.IS_FIRST_RUNNABLE //
		)
	}
}
