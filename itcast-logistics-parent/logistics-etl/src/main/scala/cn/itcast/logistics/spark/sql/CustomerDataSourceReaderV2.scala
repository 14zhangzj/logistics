package cn.itcast.logistics.spark.sql

import java.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition}
import org.apache.spark.sql.types.StructType

/**
 * 批量加载外部数据源数据时读取器Reader实现，用于读取数据
 *      DataFrame = RDD[Row] + Schema
 */
class CustomerDataSourceReaderV2 extends DataSourceReader{
	/**
	 * 返回读取数据的Schema信息
	 */
	override def readSchema(): StructType = ???
	
	/**
	 * 读取外部数据源的数据，按照分区读取的，封装在集合中，此处为列表List
	 */
	override def planInputPartitions(): util.List[InputPartition[InternalRow]] = ???
}
