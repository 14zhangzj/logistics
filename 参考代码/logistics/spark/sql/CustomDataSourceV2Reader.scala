package cn.itcast.logistics.spark.sql

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType

/**
 * 自定义的DataSourceReader，家在外部数据源的数据
	 * 继承DataSourceReader
	 * 重写readSchema方法用来生成schema
	 * 重写planInputPartitions,每个分区拆分及读取逻辑
 *
 * @param options options
 */
case class CustomDataSourceV2Reader(options: Map[String, String])extends DataSourceReader {
	/**
	 * 读取的列相关信息
	 */
	override def readSchema(): StructType = ???
	
	/**
	 * 每个分区拆分及读取逻辑
	 */
	override def planInputPartitions(): util.List[InputPartition[InternalRow]] = ???
}
