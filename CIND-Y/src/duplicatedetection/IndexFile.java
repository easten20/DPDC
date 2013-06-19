package duplicatedetection;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.RandomAccess;

public class IndexFile {
			
	Map<Integer, Long> mapIndextoChannel = new HashMap<>();
	Map<Integer, Long> mapKeyToChannel = new HashMap<>(); 
	String fileLocation;	
	long numberOfIndexes;
	long sizeEachIndex;
	
	public IndexFile(String tableLocation, int numberOfRows) throws IOException
	{		
		this.numberOfIndexes = numberOfRows;
		this.fileLocation = tableLocation;
		this.GetIndexFileChannel();
	}	
	
	/**
	 * this method use to Index all the files
	 * so the fetch will be faster afterward
	 * @throws IOException
	 */
	private void GetIndexFileChannel() throws IOException
	{				
		RandomAccessFile randomAccess = new RandomAccessFile(this.fileLocation, "r");
		int key = 0;		
		this.mapKeyToChannel = new HashMap<>();
		this.mapKeyToChannel.put(key, (long)0);				
		this.sizeEachIndex = 1000000 / this.numberOfIndexes;		
		long limit = sizeEachIndex;
		String strLine;
		long acluster = 0;
		while((strLine=randomAccess.readLine())!=null)
		{				
			String[] columns = strLine.split("\t");
			try
			{
				Integer.parseInt(columns[0]);
			}
			catch (Exception ex)
			{
				continue;
			}
			if (limit <= Integer.parseInt(columns[0]))
			{
				key++;
				limit = limit + sizeEachIndex;
				if (acluster == 0)
					acluster = randomAccess.getFilePointer();								
				this.mapKeyToChannel.put(key,  randomAccess.getFilePointer());	
				System.out.println("key = " + key + " pointer: " + randomAccess.getFilePointer());				
				randomAccess.skipBytes((int)acluster-500);
			}			
		}	
		randomAccess.close();
	}	
	
	/** 
	 * method to get String from the file system
	 * firstly it will check through the index and get the rows
	 * point by the index  
	 * @param index
	 * @return the row String of the file
	 * @throws IOException
	 */
	public String GetIndexFileChannel(int index) throws IOException
	{	
		int key = (int) (index/ this.sizeEachIndex);		
		long bytePosition = 0;		
		if (key != 0)
			key--;
		bytePosition = this.mapKeyToChannel.get(key);		
		FileInputStream fileStream = new FileInputStream(this.fileLocation);							
		String strLine;				
		DataInputStream in = new DataInputStream(fileStream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));		
		fileStream.getChannel().position(bytePosition);						
		String comparison;
		int intComparison = 0; 		
		int isPrint = 0;
		while ((strLine = br.readLine()) != null)
		{
			comparison = strLine.split("\t")[0];
			if (isPrint < 2)
			{				
				//System.out.println("first Index: " + comparison);
				isPrint++;
			}
			try{
				intComparison= Integer.parseInt(comparison);
				  // is an integer!
				} catch (NumberFormatException e) {
				  continue;
				}

			if (index == intComparison)
				break;						
		}
		return strLine;
	}		
}
