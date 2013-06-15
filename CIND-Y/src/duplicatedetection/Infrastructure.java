package duplicatedetection;

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
	
	public Set<String> GroupFile(int[] groupParameters) throws IOException
	{		
			Set<String> groupComb = new HashSet<String>(); 
			FileInputStream fileStream = new FileInputStream(this.tableLocation);
			DataInputStream in = new DataInputStream(fileStream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			while (null!=(strLine=br.readLine()))
			{				
				String[] columns = strLine.split("\t");
				String strComb = ""; 
				for (int i = 0; i < groupParameters.length; i++)
				{
					strComb += columns[groupParameters[i]];
				}
				groupComb.add(strComb);
			}			
			br.close();
			fileStream.close();
			return groupComb;			
	}
	
	public HashMap<String, ArrayList<Integer>> GroupFileHashMap(int[] groupParameters) throws IOException
	{		
			HashMap<String, ArrayList<Integer>> hashMapComb = new HashMap<>(); 
			FileInputStream fileStream = new FileInputStream(this.tableLocation);
			DataInputStream in = new DataInputStream(fileStream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;						
			while (null!=(strLine=br.readLine()))
			{				
				String[] columns = strLine.split("\t");
				String strComb = ""; 
				for (int i = 0; i < groupParameters.length; i++)				
					strComb += columns[groupParameters[i]];
				ArrayList<Integer> listIndexes = new ArrayList<Integer>();
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
	
	public void CreateFileBasedOnIndex(Set<Integer> indexes, String targetLocation) throws IOException
	{							
			BufferedWriter bufferedWriter = new BufferedWriter ( new FileWriter ( targetLocation ) );			
		    for (Integer index: indexes)
		    {
		    	bufferedWriter.write(this.GetRowFullByIndex(index));
		    	bufferedWriter.newLine();
		    }
		    bufferedWriter.flush();
		    bufferedWriter.close();
	}
	
}
