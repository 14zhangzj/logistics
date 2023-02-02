package cn.itcast.kudu.table;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 基于Java API对Kudu进行CRUD操作，包含创建表及删除表的操作
 */
public class KuduTableDemo {

	private KuduClient kuduClient = null ;

	/**
	 * 初始化KuduClient实例对象，传递Kudu Master地址信息
	 */
	@Before
	public void init(){
		// Kudu Master地址信息
		String masterAddresses = "node2.itcast.cn:7051" ;
		// 传递Kudu Master地址，构架KuduClient实例对象
		kuduClient = new KuduClient.KuduClientBuilder(masterAddresses)
			// 设置对Kudu操作超时时间
			.defaultOperationTimeoutMs(10000)
			// 建造者设计模式构建实例对象，方便设置参数
			.build();
	}

	@Test
	public void testKuduClient(){
		System.out.println(kuduClient);
	}

	/**
	 * 依据列名称、列类型和是否为主键构建ColumnSchema对象
	 * @param name 列名称
	 * @param type 列类型
	 * @param key 是否为主键
	 */
	private ColumnSchema newColumnSchema(String name, Type type, boolean key){
		// 获取Builder对象
		ColumnSchema.ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder(name, type);
		// 设置是否为主键
		builder.key(key);
		// 返回对象
		return builder.build();
	}

	/**
	 * 创建Kudu中的表，表的结构如下所示：
	 create table itcast_user(
	 id int,
	 name string,
	 age byte,
	 primary key(id)
	 )
	 partition by hash(id) partitions 3
	 stored as kudu ;
	 */
	@Test
	public void createKuduTable() throws KuduException {
		// a. 定义表的Schema信息：字段名称和类型，是否为主键
		List<ColumnSchema> columns = new ArrayList<>();
		columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
		columns.add(newColumnSchema("name",Type.STRING, false));
		columns.add(newColumnSchema("age",Type.INT8, false));
		Schema schema = new Schema(columns) ;

		// b. 定义分区策略和副本数
		CreateTableOptions options = new CreateTableOptions() ;
		// 设置分区策略，此处使用哈希分区
		options.addHashPartitions(Arrays.asList("id"), 3);
		// 设置副本数，必须为奇数
		options.setNumReplicas(1) ;

		// 调用KuduClient中createTable方法创建表
		/*
			public KuduTable createTable(String name, Schema schema, CreateTableOptions builder)
		 */
		KuduTable kuduTable = kuduClient.createTable("itcast_users", schema, options);
		System.out.println("Table ID: " + kuduTable.getTableId());
	}

	/**
	 * 创建Kudu中的表，采用范围分区策略
	 */
	@Test
	public void createKuduTableRange() throws KuduException {
		// a. 定义表的Schema信息：字段名称和类型，是否为主键
		List<ColumnSchema> columns = new ArrayList<>();
		columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
		columns.add(newColumnSchema("name",Type.STRING, false));
		columns.add(newColumnSchema("age",Type.INT8, false));
		Schema schema = new Schema(columns) ;

		// b. 定义分区策略和副本数
		CreateTableOptions options = new CreateTableOptions() ;
		// TODO: 设置分区策略，此处使用Range范围分区
		// 设置范围分区字段，必须是主键或主键部分字段
		options.setRangePartitionColumns(Arrays.asList("id")) ;
		// 设置范围
		/**
		 * value < 100
		 * 100 <= value < 500
		 * 500 <= value
		 */
		// id < 100
		PartialRow upper100 = new PartialRow(schema);
		upper100.addInt("id", 100);
		options.addRangePartition(new PartialRow(schema), upper100) ;

		// 100 <= id < 500
		PartialRow lower100 = new PartialRow(schema);
		lower100.addInt("id", 100);
		PartialRow upper500 = new PartialRow(schema);
		upper500.addInt("id", 500);
		options.addRangePartition(lower100, upper500) ;

		//  500 <= id
		PartialRow lower500 = new PartialRow(schema);
		lower500.addInt("id", 500);
		options.addRangePartition(lower500, new PartialRow(schema)) ;

		// 设置副本数，必须为奇数
		options.setNumReplicas(1) ;

		// 调用KuduClient中createTable方法创建表
		/*
			public KuduTable createTable(String name, Schema schema, CreateTableOptions builder)
		 */
		KuduTable kuduTable = kuduClient.createTable("itcast_users_range", schema, options);
		System.out.println("Table ID: " + kuduTable.getTableId());
	}

	/**
	 * 创建Kudu中的表，先进行哈希分区，再进行范围分区
	 */
	@Test
	public void createKuduTableMulti() throws KuduException {
		// a. 定义表的Schema信息：字段名称和类型，是否为主键
		List<ColumnSchema> columns = new ArrayList<>();
		columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
		columns.add(newColumnSchema("age",Type.INT8, true));
		columns.add(newColumnSchema("name",Type.STRING, false));
		Schema schema = new Schema(columns) ;

		// b. 定义分区策略和副本数
		CreateTableOptions options = new CreateTableOptions() ;
		// TODO: 先进行设置哈希分区策略
		options.addHashPartitions(Arrays.asList("id"), 5) ;

		// TODO: 再设置Range范围分区
		// 设置范围分区字段，必须是主键或主键部分字段
		options.setRangePartitionColumns(Arrays.asList("age")) ;
		// 设置范围
		/**
		 * age < 20
		 * 20 <= value < 40
		 * 40 <= value
		 */
		// age < 20
		PartialRow upper20 = new PartialRow(schema);
		upper20.addByte("age", (byte)20);
		options.addRangePartition(new PartialRow(schema), upper20) ;

		// 100 <= id < 500
		PartialRow lower20 = new PartialRow(schema);
		lower20.addByte("age", (byte)20);
		PartialRow upper40 = new PartialRow(schema);
		upper40.addByte("age", (byte)40);
		options.addRangePartition(lower20, upper40) ;

		//  500 <= id
		PartialRow lower40 = new PartialRow(schema);
		lower40.addByte("age", (byte)40);
		options.addRangePartition(lower40, new PartialRow(schema)) ;

		// 设置副本数，必须为奇数
		options.setNumReplicas(1) ;

		// 调用KuduClient中createTable方法创建表
		/*
			public KuduTable createTable(String name, Schema schema, CreateTableOptions builder)
		 */
		KuduTable kuduTable = kuduClient.createTable("itcast_users_multi", schema, options);
		System.out.println("Table ID: " + kuduTable.getTableId());
	}

	/**
	 * 删除Kudu中的表
	 */
	@Test
	public void dropKuduTable() throws KuduException {
		if(kuduClient.tableExists("itcaset_users")){
			DeleteTableResponse response = kuduClient.deleteTable("itcaset_users");
			System.out.println(response.getElapsedMillis());
		}
	}

	/**
	 * 给Kudu表增加列：address -> String
	 */
	@Test
	public void alterTableAddColumn() throws KuduException {
		// 添加列
		AlterTableOptions ato = new AlterTableOptions() ;
		ato.addColumn("address", Type.STRING, "深圳市") ;

		// 修改表
		AlterTableResponse response = kuduClient.alterTable("itcast_users", ato);
		System.out.println(response.getTableId());
	}

	/**
	 * 删除Kudu表中列：address
	 */
	@Test
	public void alterTableDropColumn() throws KuduException {
		// 添加列
		AlterTableOptions ato = new AlterTableOptions() ;
		ato.dropColumn("address") ;

		// 修改表
		AlterTableResponse response = kuduClient.alterTable("itcast_users", ato);
		System.out.println(response.getTableId());
	}

	/**
	 * 当对Kudu进行DDL操作后，需要关闭连接
	 */
	public void close() throws KuduException {
		if(null != kuduClient) kuduClient.close();
	}

}
