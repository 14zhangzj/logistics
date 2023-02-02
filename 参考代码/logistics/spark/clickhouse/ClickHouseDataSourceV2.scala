package cn.itcast.logistics.spark.clickhouse

import java.util.Optional

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, ReadSupport, StreamWriteSupport, WriteSupport}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * 依据SparkSQL中DataSource V2接口，自定义实现ClickHouse外部数据源，批量读写数据和流式写入数据
 */
class ClickHouseDataSourceV2 extends DataSourceRegister
				with ReadSupport with WriteSupport with StreamWriteSupport {
	
	/**
	 * 使用数据源时简短名称（DataSourceRegister 方法）
	 */
	override def shortName(): String = "clickhouse"
	
	/**
	 * 从外部数据源读取数据Reader(ReadSupport 方法）
	 *
	 * @param options 加载数据时传递option参数
	 */
	override def createReader(options: DataSourceOptions): DataSourceReader = {
		// 解析程序传递options参数
		val clickHouseOptions: ClickHouseOptions = new ClickHouseOptions(options.asMap())
		// 将参数传递给DataSourceReader
		new ClickHouseDataSourceReader(clickHouseOptions)
	}
	
	/**
	 * 将数据保存外部数据源Writer（WriteSupport方法）
	 *
	 * @param writeUUID 表示JobID，针对SparkSQL中每个Job保存来说，就是JobID
	 * @param schema    保存数据Schema约束信息
	 * @param mode      保存模式
	 * @param options   保存数据时传递option参数
	 */
	override def createWriter(writeUUID: String,
	                          schema: StructType,
	                          mode: SaveMode,
	                          options: DataSourceOptions): Optional[DataSourceWriter] = {
		// TODO: 为什么返回值为 Optional对象呢？原因在于 SaveMode 保存模式，如果存储系统不支持某些保存模式，不进行任何操作，返回值为空
		
		// 解析程序传递options参数
		val clickHouseOptions: ClickHouseOptions = new ClickHouseOptions(options.asMap())
		// 依据保存模式，返回对象
		mode match {
			case SaveMode.Append =>
				// 传递参数创建Writer对象
				val writer = new ClickHouseDataSourceWriter(clickHouseOptions, schema)
				// 返回Writer对象
				Optional.of(writer)
			case _ =>
				Optional.empty[DataSourceWriter]()
		}
	}
	
	/**
	 * 将流式数据中每批次结果保存外部数据源StreamWriter（StreamWriteSupport方法）
	 *
	 * @param queryId 流式应用中查询ID（StreamingQuery ID）
	 * @param schema  保存数据Schema约束
	 * @param mode    输出模式
	 * @param options 保存数据时传递option参数
	 */
	override def createStreamWriter(queryId: String,
	                                schema: StructType,
	                                mode: OutputMode,
	                                options: DataSourceOptions): StreamWriter = {
		// 解析程序传递options参数
		val clickHouseOptions: ClickHouseOptions = new ClickHouseOptions(options.asMap())
		// 构建StreamWriter对象
		new ClickHouseStreamWriter(schema, clickHouseOptions)
	}
}
