package cn.itcast.logistics.offline

import cn.itcast.logistics.common.{Configuration, KuduTools}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * 根据不同的主题开发，定义抽象方法
	 *- 1. 数据读取load：从Kudu数据库的ODS层读取数据，事实表和维度表
	 *- 2. 数据处理process：要么是拉链关联宽表，要么是依据业务指标分析得到结果表
	 *- 3. 数据保存save：将宽表或结果表存储Kudu数据库的DWD层或者DWS层
 */
trait BasicOfflineApp {
	
	/**
	 * 读取Kudu表的数据，依据指定Kudu表名称
	 *
	 * @param spark SparkSession实例对象
	 * @param tableName 表的名
	 * @param isLoadFullData 是否加载全量数据，默认值为false
	 */
	def load(spark: SparkSession, tableName: String, isLoadFullData: Boolean = false): DataFrame = {
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
	 * 数据处理，要么对事实表进行拉链操作，要么对宽表进行指标计算
	 */
	def process(dataframe: DataFrame): DataFrame
	
	/**
	 * 数据存储: DWD及DWS层的数据都是需要写入到kudu数据库中，写入逻辑相同
	 *
	 * @param dataframe 数据集，主题指标结果数据
	 * @param tableName Kudu表的名称
	 * @param isAutoCreateTable 是否自动创建表，默认为true，当表不存在时创建表
	 */
	def save(dataframe: DataFrame, tableName: String, isAutoCreateTable: Boolean = true): Unit = {
		
		// 首先，判断是否允许创建表，如果允许，表不存在时再创建
		if(isAutoCreateTable){
			KuduTools.createKuduTable(tableName, dataframe) // TODO: 主键keys默认为id列名称
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
	
}
