package com.boonya.lab.spark.hadoop.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.List;

/**
 * https://www.cnblogs.com/gaopeng527/p/5003259.html
 * https://www.cnblogs.com/gaopeng527/p/5003290.html
 *
 * @author Pengjunlin
 * @date 2024/9/10
 */
public class HDFSDataUtils {

    static SparkConf sparkConf = new SparkConf().setAppName("HDFSQuery").setMaster("local[2]");
    static JavaSparkContext sc = new JavaSparkContext(sparkConf);
    static SQLContext sqlContext = new SQLContext(sc);

    public static Dataset<Row> readHdfsByText(String tableName, String path) {
        Dataset<Row> dataset = sqlContext.read().text(path);
        // 打印模式
        dataset.printSchema();
        // 将数据框架注册成一个表
        dataset.registerTempTable(tableName);
        return dataset;
    }

    public static Dataset<Row> readHdfsByJson(String tableName, String path) {
        Dataset<Row> dataset = sqlContext.read().json(path);
        // 打印模式
        dataset.printSchema();
        // 将数据框架注册成一个表
        dataset.registerTempTable(tableName);
        return dataset;
    }

    public static List<String> readFromDb(String sql) {
        List<String> result = new ArrayList<>();
        // 使用sql语句从表中读取数据
        Dataset<Row> dataset = sqlContext.sql(sql);
        JavaRDD<Row> row = dataset.javaRDD();
        row.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row r) throws Exception {
                result.add(r.mkString());
            }
        });
        return result;
    }

    public static List<String> readFromDbAndSave(String tableName, String sql, String path) {
        readHdfsByText(tableName, path);


        List<String> result = new ArrayList<>();
        // 使用sql语句从表中读取数据
        Dataset<Row> dataset = sqlContext.sql(sql);
        JavaRDD<Row> row = dataset.javaRDD();

        // 将RDD数据存入HDFS(也可指定其他目录或格式)
        row.saveAsTextFile(path);

        row.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row r) throws Exception {
                result.add(r.mkString());
            }
        });
        return result;
    }

    public static void main(String[] args) {
        HDFSDataUtils.readHdfsByText("test", "utils://node2:9000/user/flume/events/2015-11-27-21/events-.1448629506841");
        HDFSDataUtils.readHdfsByJson("test", "utils://node2:9000/user/flume/events/2015-11-26-21/events-.1448543965316");
        HDFSDataUtils.readFromDb("SELECT * FROM test WHERE cid=57425749418");
        HDFSDataUtils.readFromDbAndSave("test","SELECT * FROM test WHERE cid=57425749418","utils://node2:9000/user/test.txt");
    }
}
