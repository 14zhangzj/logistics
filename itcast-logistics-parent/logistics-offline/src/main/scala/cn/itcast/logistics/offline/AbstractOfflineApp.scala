package cn.itcast.logistics.offline

import cn.itcast.logistics.common.{Configuration, KuduTools, SparkUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, current_date, date_sub, substring}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

trait AbstractOfflineApp {
	
	// 定义变量，SparkSession实例对象
	var spark: SparkSession = _
	
	/**
	 * 初始化SparkSession实例
	 */
	def init(clazz: Class[_]): Unit = {
		// 获取SparkConf对象
		var sparkConf: SparkConf = SparkUtils.sparkConf()
		// 设置运行模型，依据运行环境
		sparkConf = SparkUtils.autoSettingEnv(sparkConf)
		// 实例化SparkSession对象
		spark = SparkSession.builder().config(sparkConf).getOrCreate()
		spark.sparkContext.setLogLevel(Configuration.LOG_OFF)
	}
	
	/**
	 * 从Kudu数据源加载数据，指定表的名称
	 *
	 * @param tableName 表的名称
	 * @param isLoadFullData 是否加载全量数据，默认值为false
	 * @return 分布式数据集DataFrame
	 */
	def loadKuduSource(tableName: String, isLoadFullData: Boolean = false): DataFrame = {
		// 首先，需要判断是否全量加载数据，如果不是增量加载（加载昨日数据）
		if(isLoadFullData){
			// 全量加载数据
			spark.read
				.format(Configuration.SPARK_KUDU_FORMAT)
				.option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
				.option("kudu.table", tableName)
				.option("kudu.socketReadTimeoutMs", "10000")
				.load()
		}else{
			// 增量加载数据，加载昨日数据
			spark.read
				.format(Configuration.SPARK_KUDU_FORMAT)
				.option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
				.option("kudu.table", tableName)
				.option("kudu.socketReadTimeoutMs", "10000")
				.load()
				.filter(
					// cdt: 2002-06-17 04:38:00 , String -> 2002-06-17
					date_sub(current_date(), 1).cast(StringType) === substring(col("cdt"), 0, 10)
				)
		}
	}
	
	/**
	 * 对数据集DataFrame按照业务需求编码，由子类复杂实现
	 *
	 * @param dataframe 数据集，表示加载事实表的数据
	 * @return 处理以后数据集
	 */
	def process(dataframe: DataFrame): DataFrame
	
	/**
	 * 将数据集DataFrame保存至Kudu表中，指定表的名
	 *
	 * @param dataframe 要保存的数据集
	 * @param tableName 保存表的名称
	 */
	def saveKuduSink(dataframe: DataFrame, tableName: String,
	                 isAutoCreateTable: Boolean = true, keys: Seq[String] = Seq("id")): Unit = {
		// 首先，判断是否允许创建表，如果允许，表不存在时再创建
		if(isAutoCreateTable){
			KuduTools.createKuduTable(tableName, dataframe, keys) // TODO: 主键keys默认为id列名称
		}
		
		// 保存数据
		dataframe.write
			.mode(SaveMode.Append)
			.format(Configuration.SPARK_KUDU_FORMAT)
			.option("kudu.master", Configuration.KUDU_RPC_ADDRESS)
			.option("kudu.table", tableName)
			.option("kudu.operation", "upsert")
			.option("kudu.socketReadTimeoutMs", "10000")
			.save()
	}
	
	/**
	 * 程序结束，关闭资源，SparkSession关闭
	 */
	def close(): Unit = {
		if(null != spark) spark.close()
	}
	
	/**
	 * 模板方法，确定基本方法执行顺序
	 */
	def execute(clazz: Class[_],
	            srcTable: String, dstTable: String,
	            isLoadFullData: Boolean = false,
	            isAutoCreateTable: Boolean = true, keys: Seq[String] = Seq("id")): Unit = {
		// step1. 实例化SparkSession对象
		init(clazz)
		
		try{
			// step2. 从Kudu表加载数据
			val kuduDF: DataFrame = loadKuduSource(srcTable, isLoadFullData)
			kuduDF.show(10, truncate = false)
			
			// step3. 处理数据
			val resultDF: DataFrame = process(kuduDF)
			resultDF.show(10, truncate = false)
			
			// step4. 保存数据到Kudu表
			saveKuduSink(resultDF, dstTable, isAutoCreateTable, keys)
		}catch {
			case e: Exception => e.printStackTrace()
		}finally {
			// step5. 关闭资源
			close()
		}
	}
}
