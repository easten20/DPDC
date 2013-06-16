package duplicatedetection;

import duplicatedetection.enums.columnInfo;
import duplicatedetection.enums.typeOfSearch;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Infrastructure {
	String tableLocation;	

	FileInputStream fileStream;
	Map<Integer, Integer> mapPostiontoIndex = new HashMap<Integer, Integer>();
	
	public Infrastructure(String tableLocation) throws IOException
	{
		this.tableLocation = tableLocation;
		fileStream = new FileInputStream(this.tableLocation);
		this.MapIndexToPosition();
	}
	
	public String[] GetRowByPosition(int stopPosition) throws IOException
	{
		FileInputStream fileStream = new FileInputStream(this.tableLocation);
		DataInputStream in = new DataInputStream(fileStream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		int position = 0;
		String strLine;
		String[] columns = null;
		while ((strLine = br.readLine()) != null)
		{
			if (stopPosition == position)
			{
				columns = strLine.split("\t");						
				break;
			}
			position++;
		}
		br.close();		
		return columns;
	}
	
	public String[] GetRowByIndex(int index) throws IOException
	{
		FileInputStream fileStream = new FileInputStream(this.tableLocation);
		DataInputStream in = new DataInputStream(fileStream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		int position = 0;
		String strLine;
		String[] columns = null;
		int stopPosition = this.mapPostiontoIndex.get(index);
		while ((strLine = br.readLine()) != null)
		{
			if (stopPosition == position)
			{
				columns = strLine.split("\t");						
				break;
			}
			position++;
		}
		br.close();		
		return columns;
	}
	
	public String GetRowFullByIndex(int index) throws IOException
	{
		FileInputStream fileStream = new FileInputStream(this.tableLocation);
		DataInputStream in = new DataInputStream(fileStream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));	
		String strLine;		
		int position = 0;
		int stopPosition = this.mapPostiontoIndex.get(index);
		while ((strLine = br.readLine()) != null)
		{			
			if (position == stopPosition)
				break;
			position++;
		}
		br.close();		
		return strLine;
	}
	
	public void dispose() throws IOException
	{
		fileStream.close();
	}
	
	public void ReadFile(int topN) 
	{
		try
		{
			FileInputStream fileStream = new FileInputStream(this.tableLocation);
			DataInputStream in = new DataInputStream(fileStream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			for (int i = 0 ; i<topN; i++)
			{
				String strLine = br.readLine();
				String[] columns = strLine.split("\t");
				System.out.println(Arrays.toString(columns));			
			}
			br.close();
			fileStream.close();
		}
		catch (IOException ex)
		{
			System.out.println("read file failed: " + ex.getMessage());
		}
	}
	
	public int GetFileLength() throws IOException 
	{
		int length = 0;		
			FileInputStream fileStream = new FileInputStream(this.tableLocation);
			DataInputStream in = new DataInputStream(fileStream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			while (br.readLine() != null)
			{
				length++;						
			}
			br.close();
			fileStream.close();					
			return length;
	}	
	
	public HashMap<String, Set<Integer>> GroupFileHashMap(SearchParameter[] groupParameters) throws IOException
	{		
			HashMap<String, Set<Integer>> hashMapComb = new HashMap<>(); 
			FileInputStream fileStream = new FileInputStream(this.tableLocation);
			DataInputStream in = new DataInputStream(fileStream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;						
			while (null!=(strLine=br.readLine()))
			{				
				String[] columns = strLine.split("\t");
				String strComb = ""; 
				String value = "";
				for (int i = 0; i < groupParameters.length; i++)
				{
					SearchParameter searchParam = groupParameters[i];
					value = columns[searchParam.GetColumnIndex()];					
				    strComb += searchParam.GetColumnValue(value);																					
				}
				Set<Integer> listIndexes = new HashSet<Integer>();
				if (hashMapComb.containsKey(strComb))				
					listIndexes = hashMapComb.get(strComb);				
				listIndexes.add(Integer.parseInt(columns[0]));				
				hashMapComb.put(strComb, listIndexes);												
			}			
			br.close();
			fileStream.close();
			return hashMapComb;			
	}
	
	private void MapIndexToPosition() throws IOException
	{
		FileInputStream fileStream = new FileInputStream(this.tableLocation);
		DataInputStream in = new DataInputStream(fileStream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		int position = 0;
		while (null!=(strLine=br.readLine()))
		{				
			String[] columns = strLine.split("\t");
			this.mapPostiontoIndex.put(Integer.parseInt(columns[0]), position);
			position++;
		}			
		br.close();
		fileStream.close();		
	}	
	
	public void CreateResultFile(ArrayList<Set<Integer>> resultCollection, String targetLocation) throws IOException
	{							
			BufferedWriter bufferedWriter = new BufferedWriter ( new FileWriter ( targetLocation ) );			
		    for (Set<Integer> result: resultCollection)
		    {
		    	String resultRows = "";
		    	for (Integer index: result)
		    	{
		    		resultRows += index + "\t";
		    	}
		    	resultRows = resultRows.substring(0, resultRows.length() - 1);
		    	bufferedWriter.write(resultRows);
		    	bufferedWriter.newLine();
		    }
		    bufferedWriter.flush();
		    bufferedWriter.close();
	}
	
	public ArrayList<Set<Integer>> CheckRowSimilarity(HashMap<String, Set<Integer>> hashMapComb)
	{
		ArrayList<Set<Integer>> arrayListSimSet = new ArrayList<Set<Integer>>();
		for (Set<Integer> collIndexes: hashMapComb.values())
		{			
			if (collIndexes.size() > 1)
			{
				Set<Integer> similarRowsSet = new HashSet<>();
				//do similarity check				
				
				similarRowsSet.addAll(collIndexes);
				if (similarRowsSet.size() > 1)
				{					
					arrayListSimSet.add(similarRowsSet);
				}
			}
		}
		return arrayListSimSet;
	}
	
	public ArrayList<Set<Integer>> CheckRowSimilarity(ArrayList<Set<Integer>> collectionIndexes)
	{
		ArrayList<Set<Integer>> arrayListSimSet = new ArrayList<Set<Integer>>();
		for (Set<Integer> collIndexes: collectionIndexes)
		{			
			if (collIndexes.size() > 1)
			{
				Set<Integer> similarRowsSet = new HashSet<>();
				
				//do similarity check				
				
				similarRowsSet.addAll(collIndexes);
				if (similarRowsSet.size() > 1)
				{					
					arrayListSimSet.add(similarRowsSet);
				}
			}
		}
		return arrayListSimSet;
	}
	
	public ArrayList<Set<Integer>> CombineIntersectSet(ArrayList<Set<Integer>> collection1, ArrayList<Set<Integer>> collection2)
	{
		for (Set<Integer> set1: collection1)
		{
			for (Set<Integer> set2: collection2)
			{
				Set<Integer> intersectSet = new HashSet<Integer>(set2);
				intersectSet.retainAll(set1);
				if (intersectSet.size() > 0)
				{
					set1.addAll(set2);
					collection2.remove(set2);
					break; 
					//we use break because we assume only one intersection happens
				}
			}			
			
		}		
		return collection1;
	}	
		
}
