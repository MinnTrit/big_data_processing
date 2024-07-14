import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.Serializable;
import java.util.List;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import java.util.Properties;
import java.util.Map;
import java.util.Comparator;
import java.util.Optional;
import java.util.HashMap;


public class DataFrameProcessor implements Serializable {
	private ArrayList<String> dateList;
	private SparkSession spark;
	private String folderPath;
	
	public DataFrameProcessor(String folderPath, ArrayList<String> dateList) {
		this.folderPath = folderPath;
		this.dateList = dateList;
		this.spark = initialize();
	}
	
	public SparkSession initialize() {
		SparkConf conf = new SparkConf().setAppName("BigData").setMaster("local[*]");
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		return spark;
	}
	
	private Dataset<Row> extractingColumns(Dataset<Row> inputDataframe) {
		Dataset<Row> returnedDf;
		returnedDf = inputDataframe.select(
				col("_source.Contract").alias("Contract"),
				col("_source.Mac").alias("Mac"),
				col("_source.TotalDuration").alias("TotalDuration"),
				col("_source.AppName").alias("AppName")			
				);
		return returnedDf;
	}
	
	public Dataset<Row> readingFrame() {
		System.out.println("Start reading the dataframe in scope");
		System.out.println("----------------Processing----------------");
		//Initialize the struct fields
		StructField[] fields = new StructField[] {
			DataTypes.createStructField("Contract", DataTypes.StringType, true, Metadata.empty()),
			DataTypes.createStructField("Mac", DataTypes.StringType, true, Metadata.empty()),
			DataTypes.createStructField("TotalDuration", DataTypes.LongType, true, Metadata.empty()),
			DataTypes.createStructField("AppName", DataTypes.StringType, true, Metadata.empty())
		};
		
		//Initialize the schema and the final Data frame
		StructType schema = new StructType(fields);
		JavaRDD<Row> emptyRDD = spark.emptyDataFrame().toJavaRDD();
		Dataset<Row> finalDf = spark.createDataFrame(emptyRDD, schema);
		
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
		for (String dateString : dateList) {
			String fileName = dateString + ".json";
			String filePath = Paths.get(folderPath, fileName).toString();
			Dataset<Row> df = spark.read().json(filePath);
			LocalDate localDate = LocalDate.parse(dateString, formatter);
			df = extractingColumns(df);
			df = df.withColumn("Date", lit(localDate));
			if (finalDf.isEmpty()) {
				finalDf = df;
			}
			else {
				finalDf = finalDf.union(df);
			}
		}
		return finalDf;
	}
	
	public Dataset<Row> dataFrameTransform(Dataset<Row> inputDataframe) {
		Dataset<Row> returnedDf;
		System.out.println("Start naming the columns from the type");
		System.out.println("----------------Processing----------------");
		returnedDf = inputDataframe.withColumn("Type",
				when(col("AppName").equalTo("CHANNEL")
						.or(col("AppName").equalTo("DSHD"))
						.or(col("AppName").equalTo("KPLUS")), "Television")
				.when(col("AppName").equalTo("VOD")
						.or(col("AppName").equalTo("FIMS_RES"))
						.or(col("AppName").equalTo("BHD_RES"))
						.or(col("AppName").equalTo("VOD_RES"))
						.or(col("AppName").equalTo("FIMS"))
						.or(col("AppName").equalTo("BHD"))
						.or(col("AppName").equalTo("DANET")), "Film")
				.when(col("AppName").equalTo("RELAX"), "Entertainment")
				.when(col("AppName").equalTo("CHILD"), "Children")
				.when(col("AppName").equalTo("SPORT"), "Sport")
				.otherwise("Error")
				);
		
		Column TelevisionDuration = sum(
				when(col("Type").equalTo("Television"), col("TotalDuration"))
				.otherwise(lit(0))
				).alias("TelevisionDuration");
		
		Column FilmDuration = sum(
				when(col("Type").equalTo("Film"), col("TotalDuration"))
				.otherwise(lit(0))
				).alias("FilmDuration");
		
		Column EntertainmentDuration = sum(
				when(col("Type").equalTo("Entertainment"), col("TotalDuration"))
				.otherwise(lit(0))
				).alias("EntertainmentDuration");
		
		Column ChildrenDuration = sum(
				when(col("Type").equalTo("Children"), col("TotalDuration"))
				.otherwise(lit(0))
				).alias("ChildrenDuration");
		
		Column SportDuration = sum(
				when(col("Type").equalTo("Sport"), col("TotalDuration"))
				.otherwise(lit(0))
				).alias("SportDuration");
		
		returnedDf = returnedDf.groupBy("Date", "Contract").agg(TelevisionDuration, FilmDuration, 
				EntertainmentDuration, ChildrenDuration, SportDuration);
		
		return returnedDf;
	}
	
	public Dataset<Row> gettingTaste(Dataset<Row> inputDataframe) {
		System.out.println("Start getting the users' tastes");
		System.out.println("----------------Processing----------------");
		
		//Create the struct columns
		Column structCols = struct(
				inputDataframe.col("TelevisionDuration"),
				inputDataframe.col("FilmDuration"),
				inputDataframe.col("EntertainmentDuration"),
				inputDataframe.col("ChildrenDuration"),
				inputDataframe.col("SportDuration")
				);
		
		UDF1<Row, String> gettingTasteUDF = new UDF1<Row, String>() {
			//Initialize the result list
			ArrayList<String> resultList;
			@Override
			public String call(Row structRows) throws Exception {
				resultList = new ArrayList<>();
				long TelevisionValue = structRows.getLong(0);
				long FilmValue = structRows.getLong(1);
				long EntertainmentValue = structRows.getLong(2);
				long ChildrenValue = structRows.getLong(3);
				long SportValue = structRows.getLong(4);
				if (TelevisionValue > 0) {
					resultList.add("TV");
				}
				if (FilmValue > 0) {
					resultList.add("Film");
				}
				if (EntertainmentValue > 0) {
					resultList.add("EntertainMent");
				}
				if (ChildrenValue > 0) {
					resultList.add("Children");
				}
				if (SportValue > 0) {
					resultList.add("Sport");
				}
			return resultList.isEmpty() ? "Nothing" : String.join(" - ", resultList);
			}
		};
		
		spark.udf().register("gettingTaste", gettingTasteUDF, DataTypes.StringType);
		inputDataframe = inputDataframe.withColumn("Taste", callUDF("gettingTaste", structCols));
		return inputDataframe;
	}
	
	public Dataset<Row> gettingMostwatched(Dataset<Row> inputDataframe) {
		System.out.println("Start getting the most watched channels");
		System.out.println("----------------Processing----------------");
		Column structCols = struct(
				inputDataframe.col("TelevisionDuration"),
				inputDataframe.col("FilmDuration"),
				inputDataframe.col("EntertainmentDuration"),
				inputDataframe.col("ChildrenDuration"),
				inputDataframe.col("SportDuration")
				);	
		
		List<String> columnList = Arrays.asList("Television", "Film", "Entertainment", "Children", "Sport");
		UDF1<Row, String> gettingMostwatchedUDF = new UDF1<Row, String>() {
			ArrayList<Long> valueList;
			Map<Long, String> watchMap;
			@Override
			public String call(Row structRows) throws Exception {
				valueList = new ArrayList<>();
				watchMap = new HashMap<>();
				for (int index = 0; index < 5; index++) {
					long currentValue = structRows.getLong(index);
					String columnName = columnList.get(index);
					valueList.add(currentValue);
					watchMap.put(currentValue, columnName);
				}
			Optional<Long> optionalValue = valueList.stream().max(Comparator.comparingLong(Long::valueOf));
			long highestValue = optionalValue.orElse(0L);
			String returnedString = watchMap.get(highestValue);
			return returnedString;
			};
		};
		spark.udf().register("gettingMostWatch", gettingMostwatchedUDF, DataTypes.StringType);
		inputDataframe = inputDataframe.withColumn("MostWatched", callUDF("gettingMostWatch", structCols));
		return inputDataframe;
	};	
	
	public Dataset<Row> gettingActiveness(Dataset<Row> inputDataframe) {
		System.out.println("Start getting the activeness number");
		System.out.println("----------------Processing----------------");
		WindowSpec windowSpec = Window.partitionBy("Date", "Contract").orderBy("Date");
		inputDataframe = inputDataframe.withColumn("TotalActiveness", count("Contract").over(windowSpec));
		return inputDataframe;
	};
	
	public void saveToDatabase(Dataset<Row> inputDataframe,
			String username, String password, String tableName) {
		System.out.println("Stat saving the data to the database");
		System.out.println("----------------Processing----------------");
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", username);
		connectionProperties.put("password", password);
		inputDataframe.write().mode("append").jdbc("jdbc:mysql://localhost/minhtriet", tableName, connectionProperties);
	}
}
