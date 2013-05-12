import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 * @author easten
 *
 */
public class DBConnection {
	
	private Connection con;
	private Statement st;	

	public DBConnection(String url, String user, String password)	
	{			    
	        try
	        {
	            this.con = DriverManager.getConnection(url, user, password);

	        } catch (SQLException ex) {
	            Logger lgr = Logger.getLogger("test");
	            lgr.log(Level.SEVERE, ex.getMessage(), ex);
	        }     	        
	}
	
	public void ExecuteQuery(String query)
	{
		try
		{
		 st = this.con.createStatement();
         st.executeUpdate(query);         
		}
		catch(SQLException ex)
		{
			System.out.println(ex.getMessage());
		}
	}	
	
	/**
	 * create column in the database with all column type VARCHAR(MAX)
	 * @param query
	 * @param tableName
	 * @param columns
	 */
	public void CreateColumns(String tableName, String[] columns)
	{
		try
		{
			st = this.con.createStatement();
			StringBuilder strBuilder = new StringBuilder();			
			strBuilder.append(String.format("CREATE TABLE %s (", tableName));
			for (String column: columns)
			{
				strBuilder.append(String.format("%s VARCHAR(5000),", column));
			}
			strBuilder.replace(strBuilder.length() - 1, strBuilder.length(), "");
			strBuilder.append(");");
	        st.executeUpdate(strBuilder.toString());   	        
		}
		catch(SQLException ex)
		{
			System.out.println(ex.getMessage());
		}
		finally
		{
			try {
				this.st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void InsertColumn(String tableName, String[] values)
	{
		try
		{
			st = this.con.createStatement();
			StringBuilder strBuilder = new StringBuilder();			
			strBuilder.append(String.format("INSERT INTO %s VALUES(", tableName));			
			for (String value: values)
			{
				strBuilder.append(String.format("'%s',", value));				
			}
			strBuilder.replace(strBuilder.length() - 1, strBuilder.length(), "");
			strBuilder.append(");");
	        st.executeUpdate(strBuilder.toString());   
		}
		catch(SQLException ex)
		{
			System.out.println(ex.getMessage());
		}
		finally
		{
			try {
				st.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}	
}
