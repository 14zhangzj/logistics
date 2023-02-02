package cn.itcast.logistics.spark.clickhouse

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.types.StructType

/**
 * 流式数据保存至ClickHouse表中，主要StructuredStreaming Sink 实现，终端为ClickHouse
 */
class ClickHouseStreamWriter(schema: StructType,
                             options: ClickHouseOptions)  extends StreamWriter{
	/**
	 * 获取DataWriter工厂，提供DataWriter对象，将数据写入ClickHouse表中
	 */
	override def createWriterFactory(): DataWriterFactory[InternalRow] = {
		new ClickHouseDataWriterFactory(options, schema)
	}
	
	/**
	 * 数据插入成功
	 */
	override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
		// TODO： 暂不考虑，编程实现
	}
	
	/**
	 * 数据插入失败
	 */
	override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
		// TODO: 暂不考虑，编程实现
	}
	
}
