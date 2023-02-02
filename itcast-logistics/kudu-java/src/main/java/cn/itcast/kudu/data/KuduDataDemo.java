package cn.itcast.kudu.data;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 基于Java API对Kudu进行CRUD操作，包含创建表及删除表的操作
 */
public class KuduDataDemo {

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
	 * 将数据插入到Kudu Table中： INSERT INTO (id, name, age) VALUES (1001, "zhangsan", 26)
	 */
	@Test
	public void insertData() throws KuduException {
		// a. 获取表的句柄
		KuduTable kuduTable = kuduClient.openTable("itcast_users");

		// b. 会话实例对象
		KuduSession kuduSession = kuduClient.newSession();
		kuduSession.setMutationBufferSpace(1000);
		kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

		Random random = new Random();
		for(int i = 1 ; i <= 100; i ++){
			// TODO： c. 构建INSERT对象
			Insert insert = kuduTable.newInsert();
			PartialRow insertRow = insert.getRow();
			// 设置值
			insertRow.addInt("id", i);
			insertRow.addString("name", "zhangsan-" + i);
			insertRow.addByte("age", (byte)(random.nextInt(10) + 20));
			// d. 添加到缓冲中
			kuduSession.apply(insert) ;
		}

		// 手动刷新提交数据
		kuduSession.flush();

		// e. 关闭连接
		kuduSession.close();
	}

	/**
	 * 从Kudu Table中查询数据
	 */
	@Test
	public void selectData() throws KuduException {
		// step1. 获取表句柄
		KuduTable kuduTable = kuduClient.openTable("itcast_users");

		// step2. 获取表的扫描器对象
		KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
		KuduScanner kuduScanner = scannerBuilder.build();

		// step3. 遍历迭代器获取数据
		int index = 1 ;
		while (kuduScanner.hasMoreRows()){
			System.out.println("Index = " + (index ++ ));
			RowResultIterator rowResults = kuduScanner.nextRows();
			// 遍历每个Tablet获取的数据
			while (rowResults.hasNext()){
				RowResult rowResult = rowResults.next();
				System.out.println(
					"id = " + rowResult.getInt("id")
					+ ", name = " + rowResult.getString("name")
					+ ", age = " + rowResult.getByte("age")
				);
			}
		}
	}

	/**
	 * 从Kudu Table中查询数据
	 */
	@Test
	public void queryData() throws KuduException {
		// step1. 获取表句柄
		KuduTable kuduTable = kuduClient.openTable("itcast_users");

		// step2. 获取表的扫描器对象
		KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
		// TODO： 只查询id和age两个字段的值 -> project ，年龄age小于25，id大于50 -> predicate
		// a. 设置选取字段
		List<String> columnNames = new ArrayList<>();
		columnNames.add("id");
		columnNames.add("age") ;

		// b. 设置过滤条件
		// id大于50
		KuduPredicate predicateId = KuduPredicate.newComparisonPredicate(
			newColumnSchema("id", Type.INT32, true), //
			KuduPredicate.ComparisonOp.GREATER, //
			50
		) ;

		// 年龄age小于25
		KuduPredicate predicateAge = KuduPredicate.newComparisonPredicate(
			newColumnSchema("age", Type.INT8, false), //
			KuduPredicate.ComparisonOp.LESS, //
			(byte) 25
		) ;

		KuduScanner kuduScanner = scannerBuilder
			.setProjectedColumnNames(columnNames)
			.addPredicate(predicateId)
			.addPredicate(predicateAge)
			.build();

		// step3. 遍历迭代器获取数据
		int index = 1 ;
		while (kuduScanner.hasMoreRows()){
			System.out.println("Index = " + (index ++ ));
			RowResultIterator rowResults = kuduScanner.nextRows();
			// 遍历每个Tablet获取的数据
			while (rowResults.hasNext()){
				RowResult rowResult = rowResults.next();
				System.out.println(
					"id = " + rowResult.getInt("id")
						+ ", age = " + rowResult.getByte("age")
				);
			}
		}
	}

	/**
	 * 在Kudu中更新数据时，只能依据主键进行更新
	 */
	@Test
	public void updateData() throws KuduException {
		// a. 获取表的句柄
		KuduTable kuduTable = kuduClient.openTable("itcast_users");

		// b. 会话实例对象
		KuduSession kuduSession = kuduClient.newSession();
		kuduSession.setMutationBufferSpace(1000);
		kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

		// TODO： c. 构建INSERT对象
		Update update = kuduTable.newUpdate();
		PartialRow updateRow = update.getRow();
		// 设置值
		updateRow.addInt("id", 1);
		updateRow.addString("name", "zhangsan疯");
		updateRow.addByte("age", (byte)120);
		// d. 添加到缓冲中
		kuduSession.apply(update) ;

		// 手动刷新提交数据
		kuduSession.flush();

		// e. 关闭连接
		kuduSession.close();
	}

	/**
	 * 在Kudu中更新插入数据时，只能依据主键进行操作
	 */
	@Test
	public void upsertData() throws KuduException {
		// a. 获取表的句柄
		KuduTable kuduTable = kuduClient.openTable("itcast_users");

		// b. 会话实例对象
		KuduSession kuduSession = kuduClient.newSession();
		kuduSession.setMutationBufferSpace(1000);
		kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

		// TODO： c. 构建INSERT对象
		Upsert upsert = kuduTable.newUpsert();
		PartialRow upsertRow = upsert.getRow();
		// 设置值
		upsertRow.addInt("id", 201);
		upsertRow.addString("name", "铁拐");
		upsertRow.addByte("age", (byte)100);
		// d. 添加到缓冲中
		kuduSession.apply(upsert) ;

		// 手动刷新提交数据
		kuduSession.flush();

		// e. 关闭连接
		kuduSession.close();
	}

	/**
	 * 在Kudu中更新插入数据时，只能依据主键进行操作
	 */
	@Test
	public void deleteData() throws KuduException {
		// a. 获取表的句柄
		KuduTable kuduTable = kuduClient.openTable("itcast_users");

		// b. 会话实例对象
		KuduSession kuduSession = kuduClient.newSession();
		kuduSession.setMutationBufferSpace(1000);
		kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

		// TODO： c. 构建INSERT对象
		Delete delete = kuduTable.newDelete();
		PartialRow deleteRow = delete.getRow();
		// 设置值
		deleteRow.addInt("id", 201);
		// d. 添加到缓冲中
		kuduSession.apply(delete) ;

		// 手动刷新提交数据
		kuduSession.flush();

		// e. 关闭连接
		kuduSession.close();
	}

	/**
	 * 当对Kudu进行DDL操作后，需要关闭连接
	 */
	public void close() throws KuduException {
		if(null != kuduClient) kuduClient.close();
	}

}
