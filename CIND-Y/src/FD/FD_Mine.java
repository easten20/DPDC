package FD;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FD_Mine {
		
	Set<String> key_sets = new HashSet<String>();
	Set<String> prunedSets = new HashSet<String>();
	Set<Integer> nexLevelCandidate = new HashSet<Integer>();
	Set<String> rDimension = new HashSet<String>();
	Map<String, Integer> projections = new HashMap<String, Integer>();
	Map<String, String> Fd = new HashMap<String, String>();
	HashMap<Integer, ArrayList<Integer>> fd_sets = new HashMap<Integer, ArrayList<Integer>>();
	Map<Integer, Integer> distinctValues = new HashMap<Integer, Integer>();
	String TableLocation = "D:\\university\\Data Profiling and Data Cleansing\\exercise 3\\fd.tsv";
	String resultLoc = "D:\\university\\Data Profiling and Data Cleansing\\exercise 3\\result.tsv";
	int detProjection = 0;
	
	public FD_Mine()
	{
		try
		{
		FileInputStream fileStream = new FileInputStream(TableLocation);
		  DataInputStream in = new DataInputStream(fileStream);
		  BufferedReader br = new BufferedReader(new InputStreamReader(in));					  
		  String strLine = br.readLine();					  						 
		  String[] columns = strLine.split("\t");					 
		  String[] columnIndex = new String[columns.length];					  
		  for (int i = 0; i < columns.length; i++)
		  {							  
			  this.nexLevelCandidate.add(i);						  
		  }					  		 
		  in.close();
		}
		catch(Exception ex)
		{
			System.out.println(ex.getMessage());
		}
	}
		
	public void Run()
	{						
			Set<Integer> depCandidateSet = new HashSet<Integer>();
			depCandidateSet.addAll(this.nexLevelCandidate);
			for (Integer candidate : this.nexLevelCandidate)
			{
				try
				{
					FileInputStream fileStream = new FileInputStream(TableLocation);
					  DataInputStream in = new DataInputStream(fileStream);
					  BufferedReader br = new BufferedReader(new InputStreamReader(in));					  
					  String strLine;
					  Set<String> projectionValues = new HashSet<String>();
					  while ((strLine = br.readLine()) != null)			  
					  {				  
						  String[] columns = strLine.split("\t");
						  projectionValues.add(columns[candidate]);
					  }			  
					  in.close();
					  System.out.println(String.format("candidate %s distinct value: %s", candidate, projectionValues.size()));
					  this.distinctValues.put(candidate, projectionValues.size());
			     }
				catch(Exception ex)
				{		
					System.out.println(ex.getMessage());			
				}		
			}
			for (Integer candidate : this.nexLevelCandidate)
			{
				if (candidate < 2)
					continue;
				Integer SelfProjection = this.distinctValues.get(candidate);
				this.detProjection = SelfProjection;
				ArrayList<Integer> dependents;							
				dependents = new ArrayList<Integer>();				
				for (Integer depCandidate: depCandidateSet)
				{											
						if (candidate.equals(depCandidate) == false)
						{
							if (SelfProjection < this.distinctValues.get(depCandidate))
							{
								System.out.println(String.format("Pass Combination %s and %s", candidate, depCandidate));
								continue;
							}
							System.out.println(String.format("Check %s and %s", candidate, depCandidate));
							if (dependents.contains(depCandidate))
							 continue;
							if (SelfProjection == this.GetProjection(candidate,depCandidate))
					    	{							
								dependents.add(depCandidate);
								System.out.println(String.format("FD %s and %s", candidate, depCandidate));
								this.WriteToLog(String.format("C%03d\tC%03d",candidate, depCandidate));
						    	if (this.fd_sets.get(depCandidate) != null)
						    	{
						    		ArrayList<Integer> newDependents = this.fd_sets.get(depCandidate);						    							        	
						        	for (Integer addition : newDependents)
						        	{
						        		dependents.add(addition);
						        		System.out.println(String.format("FD %s and %s", candidate, addition));
						        		this.WriteToLog(String.format("C%03d\tC%03d",candidate, addition));
						        	}
						    	}					
					    	}							
						}					    
				}
				this.fd_sets.put(candidate, dependents);
			}		
	}
	
	private int GetProjection(Integer determinant, Integer dependent)
	{
		try
		{
			FileInputStream fileStream = new FileInputStream(TableLocation);
			  DataInputStream in = new DataInputStream(fileStream);
			  BufferedReader br = new BufferedReader(new InputStreamReader(in));					  
			  String strLine;
			  Set<String> projectionValues = new HashSet<String>();
			  while ((strLine = br.readLine()) != null)			  
			  {				  
				  String[] columns = strLine.split("\t");
				  if (determinant == dependent)
					  projectionValues.add(columns[determinant]);
				  else
				  {					  
					  projectionValues.add(columns[determinant]+","+columns[dependent]);
					  if (this.detProjection < projectionValues.size())
						  break;
				  }
			  }			  
			  in.close();
			  return projectionValues.size();
	     }
		catch(Exception ex)
		{		
			System.out.println(ex.getMessage());			
		}		
		return 99999;
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
	
}
