package cn.itcas.logistics.test.clickhouse

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 从TCP Socket读取数据，进行词频统计，将最终结果存储至ClickHouse表中
 */
object StructuredStreamingClickHouseTest {
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[3]")
			.config("spark.sql.shuffle.partitions", "3")
    		.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.getOrCreate()
		import spark.implicits._
		
		// 2. 从TCP Socket消费数据
		val inputStreamDF: DataFrame = spark.readStream
			.format("socket")
			.option("host", "node2.itcast.cn")
			.option("port", "9998")
			.load()
		/*
			value: String
		 */
		inputStreamDF.printSchema()
		
		// 3. 词频统计
		val resultStreamDF: DataFrame = inputStreamDF
			.as[String] // 将DataFrame转换为Dataset，方便使用
			.filter(line => null != line && line.trim.length > 0)
			// 分割单词, 每个单词列名称：value -> String
			.flatMap(line => line.trim.split("\\s+"))
			// 按照单词进行分组 -> SELECT value, count().as("count") FROM words GROUP BY value ;
			.groupBy($"value").count()
			// 修改字段名称
			.select($"value".as("word"), $"count".as("total"))
		
		// 4. 保存流式数据到ClickHouse表中
		val query: StreamingQuery = resultStreamDF
			.withColumn("opType", lit("insert"))
			.writeStream
			.outputMode(OutputMode.Update())
			.queryName("query-clickhouse-tbl_word_count")
			.format("clickhouse")
			.option("clickhouse.driver", "ru.yandex.clickhouse.ClickHouseDriver")
			.option("clickhouse.url", "jdbc:clickhouse://node2.itcast.cn:8123/")
			.option("clickhouse.user", "root")
			.option("clickhouse.password", "123456")
			.option("clickhouse.table", "default.tbl_word_count")
			.option("clickhouse.auto.create", "true")
			.option("clickhouse.primary.key", "word")
			.option("clickhouse.operate.field", "opType")
			.option("checkpointLocation", "datas/ckpt-1001/")
			.start()
		
		// TODO: step5. 启动流式应用后，等待终止
		query.awaitTermination()
		query.stop()
	}
	
}
