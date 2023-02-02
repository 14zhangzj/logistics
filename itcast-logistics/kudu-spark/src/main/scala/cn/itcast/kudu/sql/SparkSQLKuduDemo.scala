package cn.itcast.kudu.sql

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * 使用SparkSession读取Kudu数据，封装到DataFrame/Dataset集合中
 */
object SparkSQLKuduDemo {
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象，设置相关属性
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[3]")
			// 设置Shuffle分区数目
			.config("spark.sql.shuffle.partitions", "3")
			.getOrCreate()
		import spark.implicits._
		
		// 2. 加载Kudu表的数据
		val kuduDF: DataFrame = spark.read
			.format("kudu")
			.option("kudu.master", "node2.itcast.cn:7051")
			.option("kudu.table", "kudu_itcast_users")
			.load()
		/*
			root
			 |-- id: integer (nullable = false)
			 |-- name: string (nullable = true)
			 |-- age: integer (nullable = true)
			 |-- gender: string (nullable = true)
		 */
		//kuduDF.printSchema()
		/*
			+----+--------+---+------+
			|id  |name    |age|gender|
			+----+--------+---+------+
			|1001|zhangsan|23 |男    | -> F
			|1002|lisi    |22 |男    |
			|1004|zhaoliu2|33 |男    |
			|1003|xiaohong|24 |女    | -> M
			+----+--------+---+------+
		 */
		// kuduDF.show(10, truncate = false)
		
		// 3. 数据转换，自定义UDF函数实现：转换gender值
		val gender_to_udf: UserDefinedFunction = udf(
			(gender: String) => {
				// 模式匹配
				gender match {
					case "男" => "F"
					case "女" => "M"
					case _ => "未知"
				}
			}
		)
		
		val usersDF: DataFrame = kuduDF.select(
			$"id", $"name", //
			// 每个人年龄 加1
			$"age".plus(1).as("age"), //
			// 转换gender性别
			gender_to_udf($"gender").as("gender")
		)
		usersDF.persist(StorageLevel.MEMORY_AND_DISK)
		
		usersDF.printSchema()
		usersDF.show(10, truncate = false)
		
		// 4. 保存数据至Kudu表中
		usersDF.write
			.mode(SaveMode.Append)
			.format("kudu")
			.option("kudu.master", "node2.itcast.cn:7051")
			.option("kudu.table", "kudu_users")
			.option("kudu.operation", "upsert")
			.save()
		
		// 5. 程序结束，关闭资源
		spark.stop()
	}
	
}
