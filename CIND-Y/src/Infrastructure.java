import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;


public class Infrastructure {

	private String fileLocation = "D:\\university\\Data Profiling and Data Cleansing\\exercise2\\table";	
	private String sortedTableLoc = "D:\\university\\Data Profiling and Data Cleansing\\exercise2\\sortedTable\\";
	private String resultLoc = "D:\\university\\Data Profiling and Data Cleansing\\exercise2\\result.tsv";
	private DBConnection dbConnect;
	private HashMap<String,String> colMaxValue = new HashMap<String,String>();
	
	public Infrastructure()
	{		
		this.dbConnect = new DBConnection( "jdbc:postgresql://localhost:5432/IND","postgres", "password");
	}
	
	public void CreateTable()
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
						  columnIndex[i] = String.format("C%03d",i);
						  System.out.println(columnIndex[i]);
					  }			
					  this.dbConnect.CreateColumns(file.getName().replace(".tsv", ""), columnIndex);
					  in.close();					  
		        }
		    }
		}
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}		
	}
	
	public void InsertIntoDB()
	{
		try
		{
		   File[] files = new File(this.fileLocation).listFiles();
		   for (File file : files) {
		        if (!file.isDirectory()) {		        			        	
		        	  FileInputStream fileStream = new FileInputStream(file.getAbsolutePath());
					  DataInputStream in = new DataInputStream(fileStream);
					  BufferedReader br = new BufferedReader(new InputStreamReader(in));
					  String strLine;
					  while ((strLine = br.readLine()) != null)   {							  					 
						  String[] columns = strLine.split("\t");					 
						  this.dbConnect.InsertColumn(file.getName().replace(".tsv", ""), columns);
						  System.out.println(columns.toString());
					  }
					  in.close();
		        }
		    }
		}
		catch(Exception ex)
		{
			
		}	
	}
	
	public void CreateCSVfromTable()	
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
						  columnIndex[i] = String.format("C%03d",i);						  
					  }			
					  String tblName = file.getName().replace(".tsv","");					  
					  in.close();					  
					  File sortedtableDir = new File(sortedTableLoc + tblName);
						if (!sortedtableDir.exists()) {
							if (sortedtableDir.mkdir()) {
								System.out.println("create directory" + this.sortedTableLoc + tblName);
							} else {
								System.out.println("Failed to create directory!");
							}
						}						
					  for (String column: columnIndex)
					  {
						  String sortedFileLocation = this.sortedTableLoc + tblName + "\\" + column + ".csv";  
						  System.out.println("create file " + column + ".csv");
						  this.dbConnect.ExecuteQuery(String.format("Copy (Select distinct %s from %s order by %s asc) To '%s' With CSV;",column, tblName, column, sortedFileLocation));
					  }
		        }
		    }
		}
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}			
	}	
	
	public void CheckDependencies()
	{
		try
		{
			this.FillColMaxValue();
			File[] filesDep = new File(this.sortedTableLoc).listFiles();
			File[] filesRef = new File(this.sortedTableLoc).listFiles();
			BruteForceAlgorithm algorithm = new BruteForceAlgorithm();
			for (File fileDep: filesDep)
			{
				File[] filesColDep = fileDep.listFiles();
				for (File fileColDep: filesColDep)
				{					
					for (File fileRef: filesRef)
					{
						if (fileRef.getName().equals(fileDep.getName()))
							continue;
						File[] filesColRef = fileRef.listFiles();
						for (File fileColRef: filesColRef)
						{		
							String deptLargestValue = this.colMaxValue.get(fileDep.getName() + fileColDep.getName());
							String refLargestValue = this.colMaxValue.get(fileRef.getName() + fileColRef.getName());
							if (deptLargestValue.compareTo(refLargestValue) > 0)
							{
								System.out.println(String.format("Pass Comparison %s[%s]\t%s[%s]", fileDep.getName(), fileColDep.getName().replace(".csv", ""), fileRef.getName(), fileColRef.getName().replace(".csv", "")));
								continue;
							}
							System.out.println(String.format("Compare %s[%s]\t%s[%s]", fileDep.getName(), fileColDep.getName().replace(".csv", ""), fileRef.getName(), fileColRef.getName().replace(".csv", "")));							
							if (algorithm.IsDeptAttributeRef(fileColDep.getAbsolutePath(), fileColRef.getAbsolutePath()))
							{
								System.out.println(String.format("IsSatisfied %s[%s]\t%s[%s]", fileDep.getName(), fileColDep.getName().replace(".csv", ""), fileRef.getName(), fileColRef.getName().replace(".csv", "")));
								this.WriteToLog(String.format("%s[%s]\t%s[%s]", fileDep.getName(), fileColDep.getName().replace(".csv", ""), fileRef.getName(), fileColRef.getName().replace(".csv", "")));
							}
						}
					}
				}
			}
		}
		catch(Exception ex)
		{			
		}		
	}
	
	private void WriteToLog(String content)
	{
		FileWriter fstream;
		try {
			fstream = new FileWriter(this.resultLoc, true);		
			BufferedWriter out = new BufferedWriter(fstream);			
			out.append(content);
			out.newLine();
			//Close the output stream
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void FillColMaxValue()
	{
		try
		{
			File[] filesParent = new File(this.sortedTableLoc).listFiles();						
			for (File fileParent: filesParent)
			{				
				for (File fileChild: fileParent.listFiles())
				{					
					FileInputStream fsDept = new FileInputStream(fileChild.getAbsolutePath());		
					DataInputStream inDept = new DataInputStream(fsDept);
					BufferedReader brDept = new BufferedReader(new InputStreamReader(inDept));
					String strLineDept;	
					String lastValue = "";
					while((strLineDept = brDept.readLine()) != null)
						lastValue = strLineDept;
					this.colMaxValue.put(fileParent.getName() + fileChild.getName(), lastValue);
					System.out.println(fileParent.getName() + fileChild.getName() + "   " + lastValue);
				}
			}
		}
		catch(Exception ex)
		{			
		}		
	}
}
