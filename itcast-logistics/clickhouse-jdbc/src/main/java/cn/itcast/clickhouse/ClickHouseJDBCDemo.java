package cn.itcast.clickhouse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * 编写JDBC代码，从ClickHouse表查询分析数据
	 * step1. 加载驱动类
	 * step2. 获取连接Connection
	 * step3. 创建PreparedStatement对象
	 * step4. 查询数据
	 * step5. 获取数据
	 * step6. 关闭连接
 */
public class ClickHouseJDBCDemo {

	public static void main(String[] args) throws Exception{
		// 定义变量
		Connection conn = null ;
		PreparedStatement pstmt = null ;
		ResultSet result = null ;

		try {
			//step1. 加载驱动类
			Class.forName("ru.yandex.clickhouse.ClickHouseDriver");

			//step2. 获取连接Connection
			conn = DriverManager.getConnection(
				"jdbc:clickhouse://node2.itcast.cn:8123", "root", "123456"
			) ;

			//step3. 创建PreparedStatement对象
			pstmt = conn.prepareStatement("select count(1) from default.ontime");

			//step4. 查询数据
			result = pstmt.executeQuery();

			//step5. 获取数据
			while (result.next()){
				System.out.println(result.getLong(1));
			}
		}catch (Exception e){
			e.printStackTrace();
		}finally {
			//step6. 关闭连接
			if(null != result) result.close();
			if(null != pstmt) pstmt.close();
			if(null != conn) conn.close();
		}
	}

}
