package duplicatedetection;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

public class Infrastructure {
	String tableLocation = "D:\\university\\Data Profiling and Data Cleansing\\exercise4\\addresses.tsv";

	public Infrastructure(String tableLocation)
	{
		this.tableLocation = tableLocation;
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
}
