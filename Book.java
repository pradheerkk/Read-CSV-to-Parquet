import com.opencsv.bean.CsvBindByPosition;

public class Turbine {
	
	@CsvBindByPosition(position = 0)
	private String Name;
  
  @CsvBindByPosition(position = 1)
	private String Author_Name;
	
	@CsvBindByPosition(position = 2)
	private String Cost;
	
  //setters and getters
  
  }
