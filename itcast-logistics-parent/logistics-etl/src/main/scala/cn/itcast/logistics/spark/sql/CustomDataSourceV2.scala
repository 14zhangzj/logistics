package cn.itcast.logistics.spark.sql

import java.util.Optional

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, StreamWriteSupport, WriteSupport}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

class CustomDataSourceV2 extends DataSourceV2 with ReadSupport with WriteSupport{
	
	/**
	 * 批量加载外部数据源数据的Reader读取器：DataSourceReader
	 *
	 * @param options 数据源参数信息
	 */
	override def createReader(options: DataSourceOptions): DataSourceReader = ???
	
	/**
	 * 批量数据保存至外部数据数据源的Writer写入器：DataSourceWriter
	 * @param writeUUID 写入数据时唯一标识符
	 * @param schema 保存数据Schema信息
	 * @param mode 保存模式，决定如何将数据保存到外部数据源
	 * @param options 数据源参数信息
	 * @return
	 */
	override def createWriter(writeUUID: String,
	                          schema: StructType,
	                          mode: SaveMode,
	                          options: DataSourceOptions): Optional[DataSourceWriter] = ???
	
}
