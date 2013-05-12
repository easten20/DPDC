import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class BruteForceAlgorithm {		
	
	private String fileLocation;
	private String logFile;
	private HashMap<String,String[]> columnsList = new HashMap<String,String[]>();
    	
	public BruteForceAlgorithm(String folderName, String logFileLocation)
	{		
		this.fileLocation = folderName;
		this.logFile = logFileLocation;
		this.FillHashMap();
	}	
	
	public BruteForceAlgorithm()
	{				
	}	
	
	public boolean IsDeptAttributeRef(String deptFileLoc, String refFileLoc)
	{
		boolean isSatisfied = true;
		FileInputStream fsDept;
		FileInputStream fsRef;
		try {
			fsDept = new FileInputStream(deptFileLoc);		
			DataInputStream inDept = new DataInputStream(fsDept);
			BufferedReader brDept = new BufferedReader(new InputStreamReader(inDept));
			String strLineDept;			
			fsRef = new FileInputStream(refFileLoc);				
			DataInputStream inRef = new DataInputStream(fsRef);
			BufferedReader brRef = new BufferedReader(new InputStreamReader(inRef));
			String strLineRef;
			while ((strLineDept = brDept.readLine()) != null)
			{									
				while(true)
				{
					strLineRef = brRef.readLine();
					if (strLineRef == null)
					{
						isSatisfied = false;
						break;
					}					
					else if (strLineDept.equals(strLineRef))
						break;
					else if (strLineDept.compareTo(strLineRef) < 0) //deptValue < refValue					
					{
						isSatisfied = false;
						break;
					}					
				}																
				if (isSatisfied == false)
					break;
			}
			fsRef.close();
			fsDept.close();
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
		return isSatisfied;
	}
	
	private void FillHashMap()
	{
		try
		{
		File[] files = new File(this.fileLocation).listFiles();
		 for (File file : files) {
		        if (!file.isDirectory()) {		        			        	
		        	  FileInputStream fileStream = new FileInputStream(file.getAbsolutePath());
					  DataInputStream in = new DataInputStream(fileStream);
					  BufferedReader br = new BufferedReader(new InputStreamReader(in));					  
					  String strLine = br.readLine();					  						 
					  String[] columns = strLine.split("\t");					 
					  String[] columnIndex = new String[columns.length]; 
					  for (int i = 0; i < columns.length; i++)
					  {	
						  Integer myInteger = new Integer(i);
						  columnIndex[i] = String.format("%s[c%03d]", file.getName(), myInteger.toString());						  										 
					  }
					  this.columnsList.put(file.getName(), columnIndex);
					  in.close();
		        }
		    }
		}
		catch(Exception ex)
		{
			
		}		
	}	
	
	public void RunAlgorithm()
	{
		HashMap<String, String[]> mapComparer = this.columnsList;
		for (String tableName : this.columnsList.keySet()) {
			
		}
	}
	
	/*
	public RunIteration()
	{
		try
		{
		File[] files = new File(this.fileLocation).listFiles();
		File[] filesComparer = new File(this.fileLocation).listFiles();
						
		 for (File file : files) {
		        if (!file.isDirectory()) {		        			        	
		        	  FileInputStream fileStream = new FileInputStream(file.getAbsolutePath());
					  DataInputStream in = new DataInputStream(fileStream);
					  BufferedReader br = new BufferedReader(new InputStreamReader(in));
					  String strLine;					  						  
					  String[] dependentColumns = strLine.split("\t");						  
					  for (int columnIndex = 0; columnIndex < dependentColumns.length; columnIndex++)
					  {
						  String dependentColumn = String.format("%s[c%s]", file.getName(), dependentColumns);
					  while ((strLine = br.readLine()) != null)   {						  						  						  
						  for (File fileComparer : filesComparer)
						  {
							    FileInputStream fileStreamComparer = new FileInputStream(file.getAbsolutePath());
						  		DataInputStream inComparer = new DataInputStream(fileStreamComparer);
						  		BufferedReader brComparer = new BufferedReader(new InputStreamReader(inComparer));
						  		String strLineComparer;			  
						  		//Read File Line By Line			  
						  		while ((strLine = brComparer.readLine()) != null)   {
						  			// Print the content on the console						
						  		}
						  		inComparer.close();
						  }
						  
					  }
					  }
					  in.close();
		        }
		    }
		}
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());			
		}
	}
	*/			

}
