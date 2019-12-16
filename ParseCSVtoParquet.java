package service;

import java.io.File;
import java.io.IOException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParseCSVtoParquet 
{
 public static void main(String [] args) {
	 ParaquetConverter converter=  new ParaquetConverter();
	 converter.csvtolist();
	 converter.convertJsonToParquet();
}

}
