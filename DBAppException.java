package mama;

public class DBAppException extends Exception
{
	String errorMessage; 
	
	public DBAppException(String message)
	{
		this.errorMessage = message;
	}
	
	public void print()
	{
		System.out.println("ERROR: " + errorMessage);		
	}
	
	public String getMessage()
	{
		return this.errorMessage;
	}
	
}