import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.ArrayList;

public class SparkMain {
	public static void main (String[] args) {
		//Initialize the date processor
		//This code is used to test to process 1 day only
		String startDate = "2022-04-01";
		String endDate = "2022-04-01";
		DateProcessor dateProcessor = new DateProcessor(startDate, endDate);
		ArrayList<String> dateList = dateProcessor.getDatelist();
		
		//Initialize the data frame processor
		String folderPath = "";
		DataFrameProcessor dataFrameProcessor = new DataFrameProcessor(folderPath, dateList);
		Dataset<Row> df = dataFrameProcessor.readingFrame();
		df = dataFrameProcessor.dataFrameTransform(df);
		df = dataFrameProcessor.gettingTaste(df);
		df = dataFrameProcessor.gettingMostwatched(df);
		df = dataFrameProcessor.gettingActiveness(df);
		String username = "";
		String password = "";
		String tableName = "";
		dataFrameProcessor.saveToDatabase(df, username, password, tableName);
	}	
}
