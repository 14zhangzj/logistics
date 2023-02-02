package cn.itcast.clickhouse

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * SparkSQL加载JSON格式文件数据，依据Schema信息在ClickHouse中创建表，并进行数据CUD操作
 */
object ClickHouseSparkDemo {
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[2]")
    		.config("spark.sql.shuffle.partitions", "2")
    		.getOrCreate()
		import spark.implicits._
		import org.apache.spark.sql.functions._
		
		// 2. 加载JSON数据：交易订单数据
		val ordersDF: DataFrame = spark.read.json("datas/order.json")
		/*
			root
			 |-- areaName: string (nullable = true)
			 |-- category: string (nullable = true)
			 |-- id: long (nullable = true)
			 |-- money: string (nullable = true)
			 |-- timestamp: string (nullable = true)
		 */
		//ordersDF.printSchema()
		/*
			+--------+--------+---+-----+--------------------+
			|areaName|category|id |money|timestamp           |
			+--------+--------+---+-----+--------------------+
			|北京    |平板电脑|1  |1450 |2019-05-08T01:03.00Z|
			|北京    |手机    |2  |1450 |2019-05-08T01:01.00Z|
			|北京    |手机    |3  |8412 |2019-05-08T01:03.00Z|
			|上海    |电脑    |4  |1513 |2019-05-08T05:01.00Z|
			+--------+--------+---+-----+--------------------+
		 */
		// ordersDF.show(10, truncate = false)
		
		// 3. 依据DataFrame数据集，在ClickHouse数据库中创建表和删除表
		// 构建表的DDL语句
		val createDdl: String = ClickHouseUtils.createTableDdl("db_ck", "tbl_orders", ordersDF.schema)
		//println(s"Create DDL: \n${createDdl}")
		// 执行DDL语句，创建表
		//ClickHouseUtils.executeUpdate(createDdl)
		
		// 构建删除表的DDL语句
		val dropDdl: String = ClickHouseUtils.dropTableDdl("db_ck", "tbl_orders")
		//println(s"Drop DDL: \n${dropDdl}")
		// 执行DDL语句，删除表
		//ClickHouseUtils.executeUpdate(dropDdl)
		
		// 4. 保存DataFrame数据集到ClickHouse表中
		//ClickHouseUtils.insertData(ordersDF, "db_ck", "tbl_orders")
		
		// 5. 更新数据到ClickHouse表中
		val updateDF: DataFrame = Seq(
			(3, 9999, "2021-02-27T01:03.00Z"),
			(4, 9999, "2021-02-27T01:03.00Z")
		).toDF("id", "money", "timestamp")
		// ClickHouseUtils.updateData(updateDF, "db_ck", "tbl_orders")
		
		// 6. 删除ClickHouse表中数据
		val deleteDF: DataFrame = Seq(Tuple1(1), Tuple1(2), Tuple1(3)).toDF("id")
		ClickHouseUtils.deleteData(deleteDF, "db_ck", "tbl_orders")
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
	
}
