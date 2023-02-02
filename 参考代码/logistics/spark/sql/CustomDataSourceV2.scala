package cn.itcast.logistics.spark.sql

import java.util.Optional

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

/**
 * Spark SQL 基于DataSourceV2接口实现自定义数据源
	 * 1.继承DataSourceV2向Spark注册数据源
	 * 2.继承ReadSupport支持读数据
	 * 3.继承WriteSupport支持读数据
 */
class CustomDataSourceV2 extends DataSourceV2 with ReadSupport with WriteSupport{
	
	/**
	 * 创建Reader对象，用于加载读取数据源的数据
	 *
	 * @param options 用户自定义options选项
	 */
	override def createReader(options: DataSourceOptions): DataSourceReader = ???
	
	/**
	 * 创建Writer对象，将数据保存至外部数据源
	 *
	 * @param writeUUID
	 * @param schema 约束信息
	 * @param mode 保存模式
	 * @param options 用户自定义options选项
	 * @return
	 */
	override def createWriter(writeUUID: String,
	                          schema: StructType,
	                          mode: SaveMode,
	                          options: DataSourceOptions): Optional[DataSourceWriter] = ???
}
