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
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Infrastructure {
	String tableLocation;	
	
	IndexFile indexFile;
	
	public Infrastructure(String tableLocation) throws IOException
	{
		this.tableLocation = tableLocation;		
		this.indexFile = new IndexFile(tableLocation,5000);
	
	}
	
	/**
	 * use this method to get array of columns of each row index
	 * @param index
	 * @return
	 * @throws IOException
	 */
	public String[] GetRowByIndex(int index) throws IOException
	{		
		String strLine;
		String[] columns = null;			
		strLine = this.indexFile.GetIndexFileChannel(index);				
		columns = strLine.split("\t");		
		return columns;
	}
	/**
	 * return all the string of the row  
	 * @param index
	 * @return
	 * @throws IOException
	 */
	public String GetRowFullByIndex(int index) throws IOException
	{		
		String strLine;		
		strLine = this.indexFile.GetIndexFileChannel(index);						
		return strLine;
	}	
	
	/**
	 * currently not being used
	 * @param topN
	 */
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
	
	/**
	 * currentyly not being used
	 * @return
	 * @throws IOException
	 */
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

	/**
	 * this function will grouping all the value in the file 
	 * based on the parameter
	 * @param groupParameters
	 * @return set of the rows including the group name as the key 
	 * @throws IOException
	 */
	public HashMap<String, Set<Integer>> GroupFileHashMap(SearchParameter[] groupParameters) throws IOException
	{		
			HashMap<String, Set<Integer>> hashMapComb = new HashMap<>(); 
			FileInputStream fileStream = new FileInputStream(this.tableLocation);
			DataInputStream in = new DataInputStream(fileStream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;				
			boolean skipInsert = false;
			while (null!=(strLine=br.readLine()))
			{				
				String[] columns = strLine.split("\t");
				String strComb = ""; 
				String value = "";
				skipInsert = false;
				for (int i = 0; i < groupParameters.length; i++)
				{
					SearchParameter searchParam = groupParameters[i];
					value = columns[searchParam.GetColumnIndex()];
					value = searchParam.GetColumnValue(value.toLowerCase().trim());
					if (value.isEmpty() || value == null)
					{						
						skipInsert = true;
						break;
					}
				    strComb += value;																					
				}
				if (skipInsert == true)
					continue;
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
	
	/**
	 * create result file .tsv 
	 * the result will be the index of the rows
	 * example : 2031\t1009
	 * @param resultCollection
	 * @param targetLocation
	 * @throws IOException
	 */
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
	
	/**
	 * it use to check the similarity between rows group
	 * based on the hashmap parameter
	 * example group {1001,2002,500} 
	 * if 1001 and 2002 are similar then the result will be
	 * {1001,2002} 
	 * 500 not included because the similar level is below threshhold	
	 * @param hashMapComb
	 * @return
	 * @throws IOException
	 */
	public ArrayList<Set<Integer>> CheckRowSimilarity(HashMap<String, Set<Integer>> hashMapComb) throws IOException
	{
		ArrayList<Set<Integer>> arrayListSimSet = new ArrayList<Set<Integer>>();
		Similarity similarity = new Similarity();
		int totalHashMap = hashMapComb.keySet().size();
		int currentCheck = 0;
		for (Set<Integer> collIndexes: hashMapComb.values())
		{														
			currentCheck++;
			System.out.println("current check " + currentCheck + " out of " + totalHashMap); 
			if (collIndexes.size() > 1)
			{				
				Set<Integer> removeCandidateTarget = new HashSet<>();
				Integer[] arraysCandidate = collIndexes.toArray(new Integer[0]);
				for (int i = 0; i < arraysCandidate.length-1; i++)
				{
					Integer candidate = arraysCandidate[i]; 
					if (removeCandidateTarget.contains(candidate))
						continue;
					Set<Integer> similarRowsSet = new HashSet<>();					
					similarRowsSet.add(candidate);
					String[] candidateColumns = this.GetRowByIndex(candidate);					
					for (int j = i+1; j < arraysCandidate.length; j++)
					{
						Integer candidateTarget = arraysCandidate[j];
						if (candidateTarget == candidate) //we don't want to compare with itself
							continue;
						String[] candidateTargetColumns = this.GetRowByIndex(candidateTarget);
						if (similarity.AreTwoRowsSame(candidateColumns, candidateTargetColumns))
						{
							similarRowsSet.add(candidateTarget);
							removeCandidateTarget.add(candidateTarget);
							System.out.println("row number :" + candidateColumns[0]);
							System.out.println(Arrays.toString(candidateColumns));
							System.out.println("equal to row number :" + candidateTargetColumns[0]);
							System.out.println(Arrays.toString(candidateTargetColumns));
							System.out.println("\n\n");
						}
					}
					if (similarRowsSet.size() > 1)						
					{
						arrayListSimSet.add(similarRowsSet);
					}
				}								
			}
		}
		return arrayListSimSet;
	}
	
	/**
	 * same method as CheckRowSimilarity (see above)
	 * but different parameter 
	 * @param collectionIndexes
	 * @return
	 * @throws IOException
	 */
	public ArrayList<Set<Integer>> CheckRowSimilarity(ArrayList<Set<Integer>> collectionIndexes) throws IOException
	{
		ArrayList<Set<Integer>> arrayListSimSet = new ArrayList<Set<Integer>>();
		Similarity similarity = new Similarity();
		int totalHashMap = collectionIndexes.size();
		int currentCheck = 0;
		for (Set<Integer> collIndexes: collectionIndexes)
		{														
			currentCheck++;
			System.out.println("current check " + currentCheck + " out of " + totalHashMap); 
			if (collIndexes.size() > 1)
			{				
				Set<Integer> removeCandidateTarget = new HashSet<>();
				Integer[] arraysCandidate = collIndexes.toArray(new Integer[0]);
				for (int i = 0; i < arraysCandidate.length-1; i++)
				{
					Integer candidate = arraysCandidate[i]; 
					if (removeCandidateTarget.contains(candidate))
						continue;
					Set<Integer> similarRowsSet = new HashSet<>();					
					similarRowsSet.add(candidate);
					String[] candidateColumns = this.GetRowByIndex(candidate);					
					for (int j = i+1; j < arraysCandidate.length; j++)
					{
						Integer candidateTarget = arraysCandidate[j];
						if (candidateTarget == candidate) //we don't want to compare with itself
							continue;
						String[] candidateTargetColumns = this.GetRowByIndex(candidateTarget);
						if (similarity.AreTwoRowsSame(candidateColumns, candidateTargetColumns))
						{
							similarRowsSet.add(candidateTarget);
							removeCandidateTarget.add(candidateTarget);
							System.out.println("row number :" + candidateColumns[0]);
							System.out.println(Arrays.toString(candidateColumns));
							System.out.println("equal to row number :" + candidateTargetColumns[0]);
							System.out.println(Arrays.toString(candidateTargetColumns));
							System.out.println("\n\n");
						}
					}
					if (similarRowsSet.size() > 1)						
					{
						arrayListSimSet.add(similarRowsSet);
					}
				}								
			}
		}
		return arrayListSimSet;
	}
	
	/**
	 * to intersect two different set  from different group
	 * example group 1: {1001, 2002 } group 2 : {2002, 3003}
	 * result {1001,2002,3003}
	 * @param collection1
	 * @param collection2
	 * @return
	 */
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
					System.out.println("Intersection : " + Arrays.toString(set1.toArray()) + " and " + Arrays.toString(set2.toArray()));					
					set1.addAll(set2);					
					System.out.println("result : " + Arrays.toString(set1.toArray()));
					collection2.remove(set2);				
					break; 
					//we use break because we assume only one intersection happens
				}
			}			
			
		}		
		return collection1;
	}	
		
}
