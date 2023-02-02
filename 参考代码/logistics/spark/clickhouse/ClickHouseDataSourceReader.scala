package cn.itcast.logistics.spark.clickhouse

import java.sql.ResultSet
import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseStatement}

/**
 * SparkSQL批量加载数据源ClickHouse表中的数据，封装DataFrame
 *      DataFrame = RDD[Row] + Schema
 */
class ClickHouseDataSourceReader(options: ClickHouseOptions) extends DataSourceReader{
	
	/**
	 * 数据集DataFrame约束Schema
	 */
	override def readSchema(): StructType = {
		// TODO：Schema信息如何获取呢？-> ClickHouse数据库中获取表的Schema约束信息，构建DataFrame的Schema约束
		// 创建 ClickHouseHelper 对象，传递参数
		val clickHouseHelper: ClickHouseHelper = new ClickHouseHelper(options)
		// 获取Schema信息
		clickHouseHelper.getSparkSQLSchema
	}
	
	/**
	 * 读取数据源ClickHouse表的数据，按照分区进行读取，每条数据封装在Row对象中
	 */
	override def planInputPartitions(): util.List[InputPartition[InternalRow]] = {
		// TODO： 假设读取ClickHouse表的数据放在1个分区Partition中，存储到List列表中
		util.Arrays.asList(new ClickHouseInputPartition(options, readSchema()))
	}
}

/**
 * 从ClickHouse表中加载数据时，每个分区进行读取，将每行数据封装在Row对象中
 */
class ClickHouseInputPartition(options: ClickHouseOptions,
                               schema: StructType) extends InputPartition[InternalRow]{
	// 获取ClickHouse读取数据时每个分区读取器
	override def createPartitionReader(): InputPartitionReader[InternalRow] = {
		new ClickHouseInputPartitionReader(options, schema)
	}
}

/**
 * 从ClickHouse表中读取数据时，每个分区数据的具体加载
 *      TODO： 加载数据底层依然是ClickHouse JDBC方式, 编写SQL语句
 */
class ClickHouseInputPartitionReader(options: ClickHouseOptions,
                                     schema: StructType) extends InputPartitionReader[InternalRow] {
	// 创建ClickHouseHelper对象，传递参数
	private val clickHouseHelper: ClickHouseHelper = new ClickHouseHelper(options)
	
	// 定义变量
	var conn: ClickHouseConnection = _
	var stmt: ClickHouseStatement = _
	var result: ResultSet = _
	
	// 表示是否还有下一条数据，返回值为布尔类型
	override def next(): Boolean = {
		// println("========= 调用next函数，判断是否存在下一条数据，如果存在则返回true，否则返回false =========")
		// 查询 ClickHouse 表中的数据，根据查询结果判断是否存在数据
		if((conn == null || conn.isClosed)
			&& (stmt == null || stmt.isClosed)
			&& (result ==null || result.isClosed)){
			// 实例化connection连接对象
			conn = clickHouseHelper.getConnection
			stmt = conn.createStatement()
			result = stmt.executeQuery(clickHouseHelper.getSelectStatementSQL(schema))
			// println("=======初始化ClickHouse数据库成功===========")
		}
		
		// 如果查询结果集对象不是空并且没有关闭的话在，则指针下移
		if(result != null && !result.isClosed){
			// 如果next是true，表示有数据，否则没有数据
			result.next()
		}else{
			// 返回false表示没有数据
			false
		}
	}
	
	/**
	 * 表示获取下一条数据，封装在Row对象中
	 */
	override def get(): InternalRow = {
		// TODO: next()返回true，则该方法被调用，如果返回false，该方法不被调用
		// println("======调用get函数，获取当前数据============")
		val fields: Array[StructField] = schema.fields
		//一条数据所有字段的集合
		val record: Array[Any] = new Array[Any](fields.length)
		
		// 循环取出来所有的列
		for (i<- record.indices) {
			// 每个字段信息
			val field: StructField = fields(i)
			// 列名称
			val fieldName: String = field.name
			// 列数据类型
			val fieldDataType: DataType = field.dataType
			// 根据字段类型，获取对应列的值
			fieldDataType match {
				case DataTypes.BooleanType => record(i) = result.getBoolean(fieldName)
				case DataTypes.DateType => record(i) = DateTimeUtils.fromJavaDate(result.getDate(fieldName))
				case DataTypes.DoubleType => record(i) = result.getDouble(fieldName)
				case DataTypes.FloatType => record(i) = result.getFloat(fieldName)
				case DataTypes.IntegerType => record(i) = result.getInt(fieldName)
				case DataTypes.ShortType => record(i) = result.getShort(fieldName)
				case DataTypes.LongType => record(i) = result.getLong(fieldName)
				case DataTypes.StringType => record(i) = UTF8String.fromString(result.getString(fieldName))
				case DataTypes.TimestampType => record(i) = DateTimeUtils.fromJavaTimestamp(result.getTimestamp(fieldName))
				case DataTypes.ByteType => record(i) = result.getByte(fieldName)
				case DataTypes.NullType => record(i) = StringUtils.EMPTY
				case _ => record(i) = StringUtils.EMPTY
			}
		}
		
		// 创建InternalRow对象
		new GenericInternalRow(record)
	}
	
	/**
	 *  数据读取完成以后，关闭资源
	 */
	override def close(): Unit = {
		clickHouseHelper.closeJdbc(conn, stmt, result)
	}
	
}