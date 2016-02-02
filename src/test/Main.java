package test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class Main {

	private static final JavaSparkContext sc = new JavaSparkContext("spark://192.168.1.49:7077", "spark-test");
	private static final SQLContext sqlContext = new SQLContext(sc);

	private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://192.168.1.32:3306/zcdh_uni_test?user=admin&password=123456";

	public static void main(String[] args) {
		sc.addJar("D:\\mysql-connector-java-5.1.34.jar");
		Map<String, String> options = new HashMap<String, String>();
		options.put("driver", MYSQL_DRIVER);
		options.put("url", MYSQL_CONNECTION_URL);
		options.put("dbtable", "zcdh_ent_enterprise");
		// options.put("partitionColumn", "id");
		// options.put("lowerBound", "1");
		// options.put("upperBound", "3");
		// options.put("numPartitions", "5");
		long begin = System.currentTimeMillis();
		DataFrame df = sqlContext.read().format("jdbc").options(options).load();
		List<Row> employeeFullNameRows = df.filter(df.col("ent_name").like("%职场导航%")).collectAsList();
		long end = System.currentTimeMillis();
		System.out.println("用时：" + (end - begin) / 1);
		System.out.println("个数：" + employeeFullNameRows.size());
		for (Row employeeFullNameRow : employeeFullNameRows) {
			// System.out.println(employeeFullNameRow);
		}
	}

}
