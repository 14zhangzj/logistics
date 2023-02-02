package cn.itcast.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/*
	结构化流程序：词频统计
        从TCP Socket消费数据，实时词频统计，将结果保存至MySQL表中
 */
object StreamingWordCount {
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
    		.appName(this.getClass.getSimpleName.stripSuffix("$"))
    		.master("local[3]")
    		.config("spark.sql.shuffle.partitions", "3")
    		.getOrCreate()
		import spark.implicits._
		
		// 2. 从TCP Socket消费数据
		val inputStreamDF: DataFrame = spark.readStream
			.format("socket")
			.option("host", "node1.itcast.cn")
			.option("port", "9999")
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
		
		// 4. 流式数据输出
		val query: StreamingQuery = resultStreamDF
			.coalesce(1) // 降低分区数目，结果数据，数据量很少
			.writeStream
			.outputMode(OutputMode.Update())
			.queryName("query-wordcount")
			// 针对每批次结果进行输出
			.foreachBatch((batchDF: DataFrame, batchId: Long) => {
				println(s"=================Batch： ${batchId}=================")
				// 判断当前批次是否有数据，有的话，再进行输出
				if (!batchDF.isEmpty) {
					// 自己编写JDBC代码，实现数据保存操作，主键不存在进行插入，存在进行更新
					batchDF.foreachPartition(iter => saveToMySQL(iter))
				}
			})
			.option("checkpointLocation", "datas/ckpt-1001")
			.start()
		
		// 流式应用启动以后，等待
		query.awaitTermination()
		query.stop()
	}
	
	/**
	 * 将DataFrame中每个分区数据：Iterator[Row]，报保存到MySQL表中
	 */
	def saveToMySQL(iter: Iterator[Row]): Unit = {
		// step1. 加载驱动类
		Class.forName("com.mysql.jdbc.Driver")
		
		// 定义变量
		var conn: Connection = null
		var pstmt: PreparedStatement = null
		try{
			// step2. 获取连接对象
			conn = DriverManager.getConnection("jdbc:mysql://node1.itcast.cn:3306", "root", "123456")
			// 设置数据库事务为手动提交
			val autoCommit: Boolean = conn.getAutoCommit
			conn.setAutoCommit(false)
			// step3. 构建PreparedStatement对象
			pstmt = conn.prepareStatement("INSERT INTO db_spark.tbl_wordcount(word, total) VALUES(?, ?) ON DUPLICATE KEY UPDATE total=?")
			// step4. 设置值
			iter.foreach{row =>
				// 获取单词和词频
				val word = row.getAs[String]("word")
				val total = row.getAs[Long]("total")
				
				pstmt.setString(1, word)
				pstmt.setLong(2, total)
				pstmt.setLong(3, total)
				
				// 加入批次
				pstmt.addBatch()
			}
			// step5. 执行批量插入
			pstmt.executeBatch()
			
			// 手动提交事务
			conn.commit()
			conn.setAutoCommit(autoCommit)
		}catch {
			case e: Exception => e.printStackTrace()
		}finally {
			// step6. 关闭连接
			if(null != pstmt) pstmt.close()
			if(null != conn) conn.close()
		}
	}
	
}
