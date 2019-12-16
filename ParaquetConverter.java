package service;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.gson.Gson;

import model.Book;

import java.nio.file.Files;
import java.nio.file.Paths;

public class ParaquetConverter {


	List<Book> tb = new ArrayList<Book>();

	private static Log log = LogFactory.getLog(ParaquetConverter.class);
	public static final String FILE_EXTENSION = ".parquet";

	public void csvtolist() {

		try {
			BufferedReader reader = new BufferedReader(
					new FileReader("a.csv"));
			String line = null;
			ArrayList<String> listOfStrings = new ArrayList<String>();
			while ((line = reader.readLine()) != null) {
				listOfStrings.add(line);
			}
			reader.close();
			for (int i = 0; i < listOfStrings.size(); i++) {
				// loop till all the properties of book are read.
				Book book = new Book();
				String[] s = listOfStrings.get(i).split(",");
				book.setName(s[0]);
				book.setAuthor_Name(s[1]);
				book.setCost(s[2]);
				tb.add(book);
			}

		} catch (Exception e) {
			System.out.println("There is an Exception");
		}

	}

	public String convertJsonToParquet() {

		File parquetFile = null;
		try {
			parquetFile = convertToParquet(tb);
			if (parquetFile != null && parquetFile.getPath() != null) {
				final InputStream parquetStream = new DataInputStream(
						new FileInputStream(parquetFile.getAbsoluteFile()));
				String fileName = System.currentTimeMillis() * 1000 + FILE_EXTENSION;
			}
		} catch (Exception e) {
			log.error("exception {}", e);
			e.printStackTrace();
		}
		return " Covert from Json to Parquet File Sucessful !!!";
	}

	private File convertToParquet(List<Book> list) {
		JavaSparkContext sparkContext = null;
		File tempFile = null;
		try (SparkSession spark = SparkSession.builder().master("local").appName("ConvertorApp").getOrCreate()) {
			tempFile = this.createTempFile();
			Gson gson = new Gson();
			List<String> data = new ArrayList<>();
			for (int i = 0; i < list.size(); i++) {
				String s = null;
				s = gson.toJson(list.get(i));
				data.add(s);
			}
			sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
			Dataset<String> stringDataSet = spark.createDataset(data, Encoders.STRING());
			Dataset<Row> parquetDataSet = spark.read().json(stringDataSet);
			log.info("Inserted json conversion schema and value");
			parquetDataSet.printSchema();
			parquetDataSet.show();
			if (tempFile != null) {
				System.out.println("Parquet file write location::::::" + tempFile.getPath());
				parquetDataSet.write().parquet(tempFile.getPath());
				tempFile = this.retrieveParquetFileFromPath(tempFile);
			}
		} catch (Exception ex) {
			log.error("Stack Trace: {}", ex);
		} finally {
			if (sparkContext != null) {
				sparkContext.close();
			}
		}
		return tempFile;
	}

	// Create the temp file path to copy converted parquet data
	public File createTempFile() throws IOException {
		Path tempPath = null;
		File tempFile = null;
		try {
			tempPath = Files.createDirectories(Paths.get("D://hey")); // Folder Destination where file has to be created.
			tempFile = tempPath.toFile();
			if (tempFile != null && tempFile.exists()) {
				String tempFilePath = tempFile.getAbsolutePath();
				System.out.println("TempFilePath::" + tempFilePath);
				tempFile.deleteOnExit();
				Files.deleteIfExists(tempFile.toPath());
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return tempFile;
	}

	// Retrieve the parquet file path
	public File retrieveParquetFileFromPath(File tempFilePath) {
		List<File> files = Arrays.asList(tempFilePath.listFiles());
		return files.stream().filter(
				tmpFile -> tmpFile.getPath().contains(FILE_EXTENSION) && tmpFile.getPath().endsWith(FILE_EXTENSION))
				.findAny().orElse(null);
	}

}
