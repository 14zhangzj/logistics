package cn.itcast.logistics.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}

/**
 * 自定义的DataSourceWriter，将数据保存至外部数据源
	 * 继承DataSourceWriter
	 * 重写createWriterFactory方法用来创建RestDataWriter工厂类
	 * 重写commit方法,所有分区提交的commit信息
	 * 重写abort方法,当write异常时调用,该方法用于事务回滚，当write方法发生异常之后触发该方法
 *
	 * @param dataSourceOptions options
 */
case class CustomDataSourceV2Writer(dataSourceOptions: DataSourceOptions) extends DataSourceWriter{
	
	/**
	 * 创建RestDataWriter工厂类
	 */
	override def createWriterFactory(): DataWriterFactory[InternalRow] = ???
	
	/**
	 * commit
	 * @param messages 所有分区提交的commit信息
	 * 触发一次
	 */
	override def commit(messages: Array[WriterCommitMessage]): Unit = ???
	
	/**
	 * abort
	 * @param messages 当write异常时调用,该方法用于事务回滚，当write方法发生异常之后触发该方法
	 */
	override def abort(messages: Array[WriterCommitMessage]): Unit = ???
}
