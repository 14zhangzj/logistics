package cn.itcast.logistics.spark.sql

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriterFactory, WriterCommitMessage}

/**
 * 将批量数据DataFrame保存至外部数据源写入器：DataSourceWriter实现
 *      DataFrame = RDD[Row] + Schema
 *      RDD = List[Partition]， 在保存数据，针对每个分区进行操作foreachPartition
 */
class CustomerDataSourceWriterV2 extends DataSourceWriter{
	/**
	 * 构建DataWriter工厂，主要创建DataWriter对象，用于写入数据到外部数据源
	 */
	override def createWriterFactory(): DataWriterFactory[InternalRow] = ???
	
	/**
	 * 写入数据到外部数据源时，提交事务，如果成功的话， 返回信息
	 */
	override def commit(messages: Array[WriterCommitMessage]): Unit = ???
	
	/**
	 * 写入数据到外部数据源时，提交失败，进行回滚以后，返回信息
	 */
	override def abort(messages: Array[WriterCommitMessage]): Unit = ???
}
