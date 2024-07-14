import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.ArrayList;

public class SparkMain {
	public static void main (String[] args) {
		//Initialize the date processor
		String startDate = "2022-04-02";
		String endDate = "2022-04-03";
		DateProcessor dateProcessor = new DateProcessor(startDate, endDate);
		ArrayList<String> dateList = dateProcessor.getDatelist();
		
		//Initialize the data frame processor
		String folderPath = "D:\\Data Engineer Course\\Day4";
		DataFrameProcessor dataFrameProcessor = new DataFrameProcessor(folderPath, dateList);
		Dataset<Row> df = dataFrameProcessor.readingFrame();
		df = dataFrameProcessor.dataFrameTransform(df);
		df = dataFrameProcessor.gettingTaste(df);
		df = dataFrameProcessor.gettingMostwatched(df);
		df = dataFrameProcessor.gettingActiveness(df);
		String username = "spark";
		String password = "Bautroixanh12345";
		String tableName = "data_engineer";
		dataFrameProcessor.saveToDatabase(df, username, password, tableName);
	}	
}