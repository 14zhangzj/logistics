package cn.itcast.kudu.data

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * Kudu集成Spark，使用KuduContext对Kudu表中的数据进行操作
 */
object KuduSparkDataDemo {
	
	// 插入数据
	def insertData(spark: SparkSession, kuduContext: KuduContext, tableName: String): Unit = {
		// 模拟数据
		val usersDF: DataFrame = spark.createDataFrame(
			Seq(
				(1001, "zhangsan", 23, "男"),
				(1002, "lisi", 22, "男"),
				(1003, "xiaohong", 24, "女"),
				(1004, "zhaoliu2", 33, "男")
			)
		).toDF("id", "name", "age", "gender")
		// 插入数据
		/*
		  def insertRows(
		      data: DataFrame,
		      tableName: String,
		      writeOptions: KuduWriteOptions = new KuduWriteOptions
		  ): Unit
		 */
		kuduContext.insertRows(usersDF, tableName)
	}
	
	// 查看Kudu表的数据
	def selectData(spark: SparkSession, kuduContext: KuduContext, tableName: String): Unit = {
		/*
		  def kuduRDD(
		      sc: SparkContext,
		      tableName: String,
		      columnProjection: Seq[String] = Nil,
		      options: KuduReadOptions = KuduReadOptions()
		  ): RDD[Row]
		 */
		val kuduRDD: RDD[Row] = kuduContext.kuduRDD(spark.sparkContext, tableName, Seq("id", "name", "age", "gender"))
		kuduRDD.foreach{row =>
			println(
				"id = " + row.getInt(0) + ", name = " +
					row.getString(1) + ", age = " + row.getInt(2) +  ", gender = " + row.getString(3)
			)
		}
	}
	
	// 更新数据
	def updateData(kuduContext: KuduContext, tableName: String): Unit = ???
	
	// 当主键存在时，更新数据；不存在时，插入数据
	def upsertData(kuduContext: KuduContext, tableName: String): Unit  = ???
	
	// 删除数据，只能依据主键删除
	def deleteData(kuduContext: KuduContext, tableName: String): Unit = ???
	
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象，设置相关属性
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[3]")
			// 设置Shuffle分区数目
			.config("spark.sql.shuffle.partitions", "3")
			.getOrCreate()
		import spark.implicits._
		
		// 2. 构建KuduContext实例对象
		val kuduContext: KuduContext = new KuduContext("node2.itcast.cn:7051", spark.sparkContext)
		val tableName: String = "kudu_itcast_users"
		
		// 3. 对Kudu表的数据进行CRUD操作
		// 插入数据insert
		//insertData(spark, kuduContext, tableName)
		// 查询数据
		selectData(spark, kuduContext, tableName)
		// 更新数据update
		//updateData(kuduContext, tableName)
		// 插入更新数据upsert
		//upsertData(kuduContext, tableName)
		// 删除数据delete
		//deleteData(kuduContext, tableName)
		
		// 程序结束，关闭资源
		spark.stop()
		
	}
	
}
