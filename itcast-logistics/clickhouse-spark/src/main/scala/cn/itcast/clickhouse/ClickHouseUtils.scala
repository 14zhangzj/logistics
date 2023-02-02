package cn.itcast.clickhouse

import java.sql.PreparedStatement

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import ru.yandex.clickhouse._

/**
 * ClickHouse 工具类，创建表、删除表及对表数据进行CUD操作
 */
object ClickHouseUtils extends Serializable {
	
	/**
	 * 创建ClickHouse的连接实例，返回连接对象，通过连接池获取连接对象
	 *
	 * @param host ClickHouse 服务主机地址
	 * @param port ClickHouse 服务端口号
	 * @param username 用户名称，默认用户：default
	 * @param password 密码，默认为空
	 * @return Connection 对象
	 */
	def createConnection(host: String, port: String,
	                     username: String, password: String): ClickHouseConnection = {
		// TODO: 使用ClickHouse中提供ClickHouseDataSource获取连接对象
		val datasource: ClickHouseDataSource = new ClickHouseDataSource(s"jdbc:clickhouse://${host}:${port}")
		// 获取连接对象
		val connection: ClickHouseConnection = datasource.getConnection(username, password)
		// 返回对象
		connection
	}
	
	/**
	 * 传递Connection对象和SQL语句对ClickHouse发送请求，执行更新操作
	 *
	 * @param sql 执行SQL语句或者DDL语句
	 */
	def executeUpdate(sql: String): Unit = {
		// 声明变量
		var conn: ClickHouseConnection = null
		var pstmt: ClickHouseStatement = null
		try{
			// a. 获取ClickHouse连接对象
			conn = ClickHouseUtils.createConnection("node2.itcast.cn", "8123", "root", "123456")
			// b. 获取PreparedStatement实例对象
			pstmt = conn.createStatement()
			// c. 执行更新操作
			pstmt.executeUpdate(sql)
		}catch {
			case e: Exception => e.printStackTrace()
		}finally {
			// 关闭连接
			if (null != pstmt) pstmt.close()
			if (null != conn) conn.close()
		}
	}
	
	/**
	 * 依据DataFrame数据集中约束Schema信息，构建ClickHouse表创建DDL语句
	 *
	 * @param dbName 数据库的名称
	 * @param tableName 表的名称
	 * @param schema DataFrame数据集约束Schema
	 * @param primaryKeyField ClickHouse表主键字段名称
	 * @return ClickHouse表创建DDL语句
	 */
	def createTableDdl(dbName: String, tableName: String,
	                   schema: StructType, primaryKeyField: String = "id"): String = {
		/*
		areaName String,
		category String,
		id Int64,
		money String,
		timestamp String,
		 */
		val fieldStr: String = schema.fields.map{field =>
			// 获取字段名称
			val fieldName: String = field.name
			// 获取字段类型
			val fieldType: String = field.dataType match {
				case StringType => "String"
				case IntegerType => "Int32"
				case FloatType => "Float32"
				case LongType => "Int64"
				case BooleanType => "UInt8"
				case DoubleType => "Float64"
				case DateType => "Date"
				case TimestampType => "DateTime"
				case x => throw new Exception(s"Unsupported type: ${x.toString}")
			}
			// 组合字符串
			s"${fieldName} ${fieldType}"
		}.mkString(",\n")
	
		// 构建创建表DDL语句
		val createDdl: String = s"""
		  |CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
		  |${fieldStr},
		  |sign Int8,
		  |version UInt8
		  |)
		  |ENGINE=VersionedCollapsingMergeTree(sign, version)
		  |ORDER BY ${primaryKeyField}
		  |""".stripMargin
		
		// 返回DDL语句
		createDdl
	}
	
	/**
	 * 依据数据库名称和表名称，构建ClickHouse中删除表DDL语句
	 *
	 * @param dbName 数据库名称
	 * @param tableName 表名称
	 * @return 删除表的DDL语句
	 */
	def dropTableDdl(dbName: String, tableName: String): String = {
		s"DROP TABLE IF EXISTS ${dbName}.${tableName}"
	}
	
	/**
	 * 构建数据插入ClickHouse中SQL语句
	 *
	 * @param dbName 数据库名称
	 * @param tableName 表的名称
	 * @param columns 列名称
	 * @return INSERT 插入语句
	 */
	def createInsertSQL(dbName: String, tableName: String, columns: Array[String] ): String = {
		// 所有列名称字符串 -> areaName, category, id, money, timestamp
		val fieldsStr: String = columns.mkString(", ")
		// 所有列对应值的占位符 -> ?, ?, ?, ?, ?
		val valuesStr: String = columns.map(_ => "?").mkString(", ")
		// 插入INSERT SQL语句
		s"""
		   |INSERT INTO ${dbName}.${tableName} (${fieldsStr}, sign, version) VALUES (${valuesStr}, ?, ?);
		   |""".stripMargin
	}
	
	/**
	 * 插入数据：DataFrame到ClickHouse表
	 */
	def insertData(dataframe: DataFrame, dbName: String, tableName: String): Unit = {
		// 获取DataFrame中Schema信息
		val schema: StructType = dataframe.schema
		// 获取DataFrame中所有列名称
		val columns: Array[String] = dataframe.columns
		
		// 构建INSERT语句
		val insertSql: String = createInsertSQL(dbName, tableName, columns)
		// TODO：针对每个分区数据进行操作，每个分区的数据进行批量插入
		dataframe.foreachPartition{iter =>
			// 声明变量
			var conn: ClickHouseConnection = null
			var pstmt: PreparedStatement = null
			try{
				// a. 获取ClickHouse连接对象
				conn = ClickHouseUtils.createConnection("node2.itcast.cn", "8123", "root", "123456")
				// b. 构建PreparedStatement对象
				pstmt = conn.prepareStatement(insertSql)
				// TODO: c. 遍历每个分区中数据，将每条数据Row值进行设置
				var counter: Int = 0
				iter.foreach{row =>
					// 从row中获取每列的值，和索引下标 -> 通过列名称 获取 Row中下标索引 , 再获取具体值
					columns.foreach{column =>
						// 通过列名称 获取 Row中下标索引
						val index: Int = schema.fieldIndex(column)
						// 依据索引下标，从Row中获取值
						val value: Any = row.get(index)
						// 进行PreparedStatement设置值
						pstmt.setObject(index + 1, value)
					}
					pstmt.setObject(columns.length + 1, 1)
					pstmt.setObject(columns.length + 2, 1)
					
					// 加入批次
					pstmt.addBatch()
					counter += 1
					// 判断counter大小，如果大于1000 ，进行一次批量插入
					if(counter >= 1000) {
						pstmt.executeBatch()
						counter = 0
					}
				}
				// d. 批量插入
				pstmt.executeBatch()
			}catch {
				case e: Exception => e.printStackTrace()
			}finally {
				// e. 关闭连接
				if(null != pstmt) pstmt.close()
				if(null != conn) conn.close()
			}
		}
	}
	
	/**
	 * 依据列名称从Row中获取值，具体数据类型未知，返回Any类型
	 *
	 * @param row Row对象，表示每行数据
	 * @param column 具体列名称
	 * @return 列的值
	 */
	def getFieldValue(row: Row, column: String): Any = {
		// 依据列名获取对应索引位置
		val fieldIndex: Int = row.schema.fieldIndex(column)
		// 依据列的索引位置从Row中取得值
		row.get(fieldIndex)
	}
	
	/**
	 * 构建数据更新ClickHouse表中SQL语句
	 *
	 * @param dbName 数据库名称
	 * @param tableName 表名称
	 * @param row DataFrame中每行数据
	 * @param primaryKeyField 主键列
	 * @return update更新SQL语句
	 */
	def createUpdateSQL(dbName: String, tableName: String,
	                    row: Row, primaryKeyField: String = "id"): String = {
		/*
				 id     money    timestamp
			Row: 3,    9999, "2020-12-08T01:03.00Z"
						     |
			     money = '9999', timestamp = '2020-12-08T01:03.00Z'
		 */
		val updatesStr: String = row.schema.fields
    		.map(field => field.name) // 获取所有列名称
			// 过滤主键列名称
    		.filter(columnName => ! primaryKeyField.equals(columnName))
			// 依据列名称获取对应的值
    		.map{columnName =>
			    val columnValue: Any = getFieldValue(row, columnName)
			    s"${columnName} = '${columnValue}'"
		    }.mkString(", ")
		// 获取主键的值
		val primaryKeyValue: Any = getFieldValue(row, primaryKeyField)
		// 构建UPDATE更新SQL语句
		s"""
		   |ALTER TABLE ${dbName}.${tableName}
		   |    UPDATE ${updatesStr} WHERE ${primaryKeyField} = ${primaryKeyValue} ;
		   |""".stripMargin
	}
	
	/**
	 * 将DataFrame数据集更新至ClickHouse表中，依据主键进行更新
	 *
	 * @param dbName 数据库名称
	 * @param tableName 表名称
	 * @param dataframe 数据集DataFrame，更新的数据，其中要包含主键
	 */
	def updateData(dataframe: DataFrame, dbName: String, tableName: String): Unit = {
		// 对DataFrame每个分区数据进行操作
		dataframe.foreachPartition{iter =>
			// 声明变量
			var conn: ClickHouseConnection = null
			var pstmt: ClickHouseStatement = null
			try{
				// a. 获取ClickHouse连接对象
				conn = ClickHouseUtils.createConnection("node2.itcast.cn", "8123", "root", "123456")
				// b. 构建PreparedStatement对象
				pstmt = conn.createStatement()
				
				// TODO: 遍历分区中每条数据Row，构建更新Update语句，进行更新操作
				iter.foreach{row =>
					// b. 依据Row对象，创建更新SQL语句
					// ALTER TABLE db_ck.tbl_orders UPDATE money = '9999', timestamp = '2020-12-08T01:03.00Z' WHERE id = 3 ;
					val updateSql: String = createUpdateSQL(dbName, tableName, row)
					// d. 执行更新操作
					pstmt.executeUpdate(updateSql)
				}
			}catch {
				case e: Exception => e.printStackTrace()
			}finally {
				// e. 关闭连接
				if(null != pstmt) pstmt.close()
				if(null != conn) conn.close()
			}
		}
	}
	
	/**
	 * 删除数据：依据主键，将ClickHouse表中数据删除
	 */
	def deleteData(dataframe: DataFrame, dbName: String,
	               tableName: String, primaryField: String = "id"): Unit = {
		// 对DataFrame每个分区数据进行操作
		dataframe.foreachPartition{iter =>
			// 声明变量
			var conn: ClickHouseConnection = null
			var pstmt: ClickHouseStatement = null
			try{
				// a. 获取ClickHouse连接对象
				conn = ClickHouseUtils.createConnection("node2.itcast.cn", "8123", "root", "123456")
				// b. 构建PreparedStatement对象
				pstmt = conn.createStatement()
				
				// TODO: 遍历分区中每条数据Row，构建更新Update语句，进行更新操作
				iter.foreach{row =>
					// b. 依据Row对象，创建删除SQL语句
					// ALTER TABLE db_ck.tbl_orders DELETE WHERE id = "3" ;
					val deleteSql: String = s"ALTER TABLE ${dbName}.${tableName} DELETE WHERE ${primaryField} = ${getFieldValue(row, primaryField)} ;"
					// d. 执行更新操作
					pstmt.executeUpdate(deleteSql)
				}
			}catch {
				case e: Exception => e.printStackTrace()
			}finally {
				// e. 关闭连接
				if(null != pstmt) pstmt.close()
				if(null != conn) conn.close()
			}
		}
	}
	
}
