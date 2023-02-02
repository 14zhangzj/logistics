package cn.itcast.logistics.common

import java.util

import org.apache.kudu.client
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Kudu操作的工具类：创建表及其他操作
 */
object KuduTools {
	
	/**
	 * 创建Kudu表，创建Kudu表的约束？
	 * 1. Kudu表中必须要有一个主键列，更新数据和删除数据都需要根据主键更新，应该使用哪个列作为主键？id
	 * 2. Kudu表的字段有哪些？oracle和mysql的表有哪些字段，Kudu表就有哪些字段
	 * 3. kudu表的名字是什么？使用数据中传递的表明作为Kudu的表名
	 */
	def createKuduTable(tableName: String, streamDF: DataFrame,
	                    keys: Seq[String] = Seq("id")): Unit = {
		/**
		 * 实现步骤：
		 * step1. 获取Kudu的上下文对象：KuduContext
		 * step2. 判断Kudu中是否存在这张表，如果不存在则创建
		 * step3. 生成Kudu表的结构信息
		 * step4. 设置表的分区策略和副本数目
		 * step5. 创建表
		 */
		// step1. 构建KuduContext实例对象
		val kuduContext = new KuduContext(Configuration.KUDU_RPC_ADDRESS, streamDF.sparkSession.sparkContext)
		
		// step2. 判断Kudu中是否存在这张表，如果不存在则创建
		if(kuduContext.tableExists(tableName)){
			println(s"Kudu中表【${tableName}】已经存在，无需创建")
			return
		}
		
		/*
		  def createTable(
		      tableName: String,
		      schema: StructType,
		      keys: Seq[String],
		      options: CreateTableOptions
		  ): KuduTable
		 */
		// step3. 生成Kudu表的结构信息, 不能直接使用DataFrame中schema设置，需要设置主键列不为null
		val schema: StructType = StructType(
			streamDF.schema.fields.map{field =>
				StructField(
					field.name, //
					field.dataType, //
					nullable = if(keys.contains(field.name)) false else true
				)
			}
		)
		
		// step4. 设置Kudu表分区策略和副本数目
		val options: CreateTableOptions = new CreateTableOptions()
		options.setNumReplicas(1)
		// 将Scala集合对象与Java集合对象相互转换
		import scala.collection.JavaConverters._
		options.addHashPartitions(keys.asJava, 3)
		
		// step5. 传递参数，创建表
		val kuduTable = kuduContext.createTable(tableName, schema, keys, options)
		println(s"Kudu Table ID: ${kuduTable.getTableId}")
	}
	
}
