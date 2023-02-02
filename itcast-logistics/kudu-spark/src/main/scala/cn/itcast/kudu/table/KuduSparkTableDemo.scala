package cn.itcast.kudu.table

import java.util

import org.apache.kudu.client.{CreateTableOptions, DeleteTableResponse, KuduTable}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ByteType, IntegerType, StringType, StructType}

/**
 * Kudu集成Spark，创建表和删除表：kudu_itcast_users
 */
object KuduSparkTableDemo {
	
	/**
	 * 使用KuduContext创建表
	 */
	def createKuduTable(context: KuduContext, tableName: String): Unit = {
		// a. 构建表的Schema信息
		val schema: StructType = new StructType()
    		.add("id", IntegerType, nullable = false)
    		.add("name", StringType, nullable = true)
    		.add("age", IntegerType, nullable = true)
    		.add("gender", StringType, nullable = true)
		// b. 指定表的主键
		val keys: Seq[String] = Seq("id")
		// c. 设置表的分区和副本数
		val options: CreateTableOptions = new CreateTableOptions()
		options.addHashPartitions(util.Arrays.asList("id"), 3)
		options.setNumReplicas(1)
		/*
		  def createTable(
		      tableName: String,
		      schema: StructType,
		      keys: Seq[String],
		      options: CreateTableOptions
		  ): KuduTable
		 */
		val kuduTable: KuduTable = context.createTable(tableName, schema, keys, options)
		println("Table ID: " + kuduTable.getTableId)
	}
	
	/**
	 * 使用KuduContext删除表
	 */
	def dropKuduTable(context: KuduContext, tableName: String): Unit = {
		// 判断表是否存在，存在的话，进行删除
		if(context.tableExists(tableName)){
			val response: DeleteTableResponse = context.deleteTable(tableName)
			println(response.getElapsedMillis)
		}
	}
	
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
		
		// 3. 创建表或删除表
		//val tableName: String = "kudu_itcast_users"
		val tableName: String = "kudu_users"
		createKuduTable(kuduContext, tableName)
		// dropKuduTable(kuduContext, tableName)
		
		// 程序结束，关闭资源
		spark.stop()
	}
	
}
