package cn.itcast.logistics.spark.clickhouse

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

/**
 * 继承DataWriterFactory基类，构建工厂，创建DataWriter对象
 *      TODO：此处单独提取出来，在SparkSQL中，无论批量保存还是流式保存，底层都是重用DataWriterFactory，进行获取Writer对象，保存数据
 */
class ClickHouseDataWriterFactory(options: ClickHouseOptions,
                                  schema: StructType)  extends DataWriterFactory[InternalRow]{
	/**
	 * 每个分区数据，获取一个DataWriter对象
	 */
	override def createDataWriter(partitionId: Int, // 分区ID
	                              taskId: Long, // TaksID，每个分区数据被一个Task处理分析
	                              epochId: Long): DataWriter[InternalRow] = {
		new ClickHouseDataWriter(options, schema)
	}
}

/**
 * 创建DataWriter对象，将每个分区数据写入ClickHouse sink终端
 */
class ClickHouseDataWriter(options: ClickHouseOptions,
                           schema: StructType) extends DataWriter[InternalRow] {
	// 创建ClickHouseHelper工具类实例对象
	private val clickHouseHelper: ClickHouseHelper = new ClickHouseHelper(options)
	
	// 初始化操作，当构建DataWriter对象时，判断ClickHouse表是否存在，如果不存在，自动创建
	private val init: Unit = {
		// 如果允许自动创建表，依据DataFrame信息创建表
		if(options.autoCreateTable){
			// 生成建表的DDL语句
			val ddl: String = clickHouseHelper.createTableDdl(schema)
			//println(s"${ddl}")
			// 表的创建
			clickHouseHelper.executeUpdate(ddl)
		}
	}
	
	// 将需要操作SQL语句存储到集合列表中
	private var sqlArray: ArrayBuffer[String] = ArrayBuffer[String]()
	
	/**
	 * 写入数据（被多次调用，每条数据都会调用一次该方法）
	 * @param record 单条记录
	 *               每条记录都会触发一次该方法
	 *               该方法不实现数据的写入操作：频繁的操作数据库，dataSourceV2（支持事务），在该方法中只实现根据不同的操作生成不同的sql语句
	 */
	override def write(record: InternalRow): Unit = {
		// 实现数据写入的逻辑，每条数据触发一次该方法，如100条数据，会被触发100次
		// 根据当前数据的操作类型，生成不同的sql语句，如果OpType是insert，那么生成插入的sql语句
		val sqlStr: String = clickHouseHelper.createStatementSQL(schema, record)
		//println(s"${sqlStr}")
		
		// 如果生成的更新操作的sql语句为空，则不需要追加到更新操作的sql集合中
		if(StringUtils.isEmpty(sqlStr)){
			val message = "===========拼接的插入、更新、删除操作的sql语句失败================"
			throw new RuntimeException(message)
		}
		
		// 将当前操作的sql语句写入到sql集合中
		sqlArray += sqlStr
	}
	
	/**
	 * 提交方法（提交数据）
	 *      在该方法中完成数据写入clickhouse表的操作逻辑
	 *      每个分区都会触发一次该操作
	 */
	override def commit(): WriterCommitMessage = {
		// 真正的数据更新操作，将数据更新的到 ClickHouse数据库中
		if (sqlArray.nonEmpty) {
			//将数据操作的sql语句执行，更新到clickhouse表中
			clickHouseHelper.executeUpdateBatch(sqlArray.toArray)
		}
		// 清空列表
		sqlArray.clear()
		// 提交执行后，返回给Client消息
		new WriterCommitMessage() {
			override def toString: String = s"批量更新SQL语句：${sqlArray.mkString("\n")}"
		}
	}
	
	/**
	 * 数据插入失败
	 */
	override def abort(): Unit = {
		// TODO: 暂不考虑，编程实现
	}
}