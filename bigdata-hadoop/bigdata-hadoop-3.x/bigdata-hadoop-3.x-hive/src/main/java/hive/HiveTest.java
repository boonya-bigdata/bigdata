package hive;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.log4j.Logger;

public class HiveTest {

	static Logger logger = Logger.getLogger(HiveTest.class);

	public static void main(String[] args) {

		Connection conn = HiveService.getConn();
		Statement stmt = null;
		try {

			stmt = HiveService.getStmt(conn);
			stmt.execute("drop table  if exists users");// 需要拥有hdfs文件读写权限的用户才可以进行此操作
			logger.debug("drop table is susscess");

			stmt.execute("create table users(user_id int, fname string,lname string )  row format delimited fields terminated by ','");// 需要拥有hdfs文件读写权限的用户才可以进行此操作
			logger.debug("create table is susscess");

			stmt.execute("insert into users(user_id, fname,lname) values(222,'yang','yang2')");// 需要拥有hdfs文件读写权限的用户才可以进行此操作
			logger.debug("insert is susscess");

			stmt.execute("load data local inpath '/root/data.txt' overwrite into table users");// 需要拥有hdfs文件读写权限的用户才可以进行此操作
			logger.debug("load data is susscess");

			String sql = "select * from users";

			ResultSet res = null;
			res = stmt.executeQuery(sql);

			ResultSetMetaData meta = res.getMetaData();   //fields name

			for (int i = 1; i <= meta.getColumnCount(); i++) {
				System.out.print(meta.getColumnName(i) + "\t");
			}
			System.out.println();
			while (res.next()) {
				System.out.print(res.getInt(1) + "\t\t");
				System.out.print(res.getString(2) + "\t\t");
				System.out.print(res.getString(3));
				System.out.println();
			}

			sql = "show tables ";
			System.out.println("\nRunning: " + sql);
			res = stmt.executeQuery(sql);
			while (res.next()) {
				System.out.println(res.getString(1));
			}
			// describe table
			sql = "describe users";
			System.out.println("\nRunning: " + sql);
			res = stmt.executeQuery(sql);
			while (res.next()) {
				System.out.println(res.getString(1) + "\t" + res.getString(2));
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HiveService.closeStmt(stmt);
		HiveService.closeConn(conn);
	}
}