import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveHelloWorld {

	public static void main(String[] args) throws Throwable {

		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection(
				"jdbc:hive2://hadoop0:10000/wu", "root", "Wu2018!!");
		Statement stmt = con.createStatement();
		String querySQL = "show tables";

		ResultSet res = stmt.executeQuery(querySQL);
		while (res.next()) {
			System.out.println(res.getString(1));
		}

		res.close();
		stmt.close();
		con.close();

	}

}
