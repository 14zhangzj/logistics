package cn.itcas.logistics.test.clickhouse

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 测试自定义数据源ClickHouse：
 *      1. 使用批的方式加载ClickHouse表中的数据
 *      2. 将数据集批量保存至ClickHouse表中
 */
object SparkSQLClickHouseTest {
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象，设置相关配置信息
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[2]")
			.config("spark.sql.shuffle.partitions", "2")
			// 设置Kryo序列化方式
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.getOrCreate()
		import spark.implicits._
		
		// 2. 从ClickHouse加载数据，封装至DataFrame中
		val clickHouseDF: DataFrame = spark.read
			.format("clickhouse")
			.option("clickhouse.driver", "ru.yandex.clickhouse.ClickHouseDriver")
			.option("clickhouse.url", "jdbc:clickhouse://node2.itcast.cn:8123/")
			.option("clickhouse.user", "root")
			.option("clickhouse.password", "123456")
			.option("clickhouse.table", "default.tbl_order")
			.load()
		//clickHouseDF.printSchema()
		//clickHouseDF.show(10, truncate = false)
		
		// 3. 数据分析处理：按照category类别分组统计
		val aggDF: DataFrame = clickHouseDF.groupBy($"category").count()
		//aggDF.printSchema()
		//aggDF.show(10, truncate = false)
		
		// 4. 保存分析结果数据至ClickHouse表中
		aggDF
			// 添加字段，表示数据属于insert、update还是delete
			.withColumn("opType", lit("insert"))
			.write
			.mode(SaveMode.Append)
			.format("clickhouse")
			.option("clickhouse.driver", "ru.yandex.clickhouse.ClickHouseDriver")
			.option("clickhouse.url", "jdbc:clickhouse://node2.itcast.cn:8123/")
			.option("clickhouse.user", "root")
			.option("clickhouse.password", "123456")
			.option("clickhouse.table", "default.tbl_order_agg")
			.option("clickhouse.auto.create", "true")  // 表不存在，创建表
			.option("clickhouse.primary.key", "category") // 指定主键字段名称
			.option("clickhouse.operate.field", "opType") // 指定数据操作类型的字段
			.save()
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
