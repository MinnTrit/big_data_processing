import java.time.format.DateTimeFormatter;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;

public class DateProcessor implements Serializable {
	String startDate;
	String endDate;
	ArrayList<String> dateList;
	public DateProcessor(String startDate, String endDate) {
		this.startDate = startDate;
		this.endDate = endDate;
		this.dateList = initialize(startDate, endDate);
	}
	
	public ArrayList<String> initialize(String startDate, String endDate) {
		ArrayList<String> resultList = new ArrayList<>();
		String stringToDate = "yyyy-MM-dd";
		DateTimeFormatter stringToDateFormatter = DateTimeFormatter.ofPattern(stringToDate);
		LocalDate startLocal = LocalDate.parse(startDate, stringToDateFormatter);
		LocalDate endLocal = LocalDate.parse(endDate, stringToDateFormatter);
		if (endLocal.isBefore(endLocal)) {
			System.out.println("End date can't come before the start date");
		}
		else {
			String expectedPattern = "yyyyMMdd";
			DateTimeFormatter dateToStringFormatter = DateTimeFormatter.ofPattern(expectedPattern);
			while (startLocal.isBefore(endLocal) || startLocal.equals(endLocal)) {
				String dateString = startLocal.format(dateToStringFormatter);
				resultList.add(dateString);
				startLocal = startLocal.plusDays(1);
			}
		}
		return resultList;
	}
	
	public ArrayList<String> getDatelist() {
		return dateList;
	}
	
	public String getStartDate() {
		return startDate;
	}
	
	public String getEndDate() {
		return endDate;
	}
}
