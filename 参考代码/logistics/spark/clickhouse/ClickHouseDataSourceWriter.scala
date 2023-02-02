package cn.itcast.logistics.spark.clickhouse

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/**
 * 批量保存数据至ClickHouse表中时，Writer对象
 */
class ClickHouseDataSourceWriter(options: ClickHouseOptions,
                                 schema: StructType) extends DataSourceWriter{
	/**
	 * 构建WriterFactory工厂对象，用于创建Writer实例，保存写入数据
	 */
	override def createWriterFactory(): DataWriterFactory[InternalRow] = {
		new ClickHouseDataWriterFactory(options, schema)
	}
	
	/**
	 * 当写入数据成功时信息动作
	 */
	override def commit(messages: Array[WriterCommitMessage]): Unit = {
		// TODO: 实现代码，暂不考虑
	}
	
	/**
	 * 当数据写入失败时信息动作
	 */
	override def abort(messages: Array[WriterCommitMessage]): Unit = {
		// TODO: 实现代码，暂不考虑
	}
}
