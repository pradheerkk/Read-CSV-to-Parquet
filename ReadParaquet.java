import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ForeachFunction;


public class ReadParaquet {
	
	public static void main(String[] args) {
		SparkSession spark = SparkSession
			      .builder().master("local")
			      .appName("Java SparkPartiotion Example")
			      .config("spark.some.config.option", "some-value")
			      .getOrCreate();
		runBasicDataSourceExample(spark);
		
	}
	private static void runBasicDataSourceExample(SparkSession spark) {
	   
	    Dataset<Row> usersDF = spark.read().load("hey.parquet");
	    usersDF.select("Author_Name", "Name").write().save("example.parquet");
	    usersDF.foreach((ForeachFunction<Row>) row -> System.out.println(row));
	    
	    

	}
