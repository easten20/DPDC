package duplicatedetection;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class TestProgramm {

	
	/**
	 * 
	 * @author easten
	 * to get columnInfo index please use ordinal 
	 * for example:
	 * columnInfo.id.ordinal() will return 0;
	 */
	public enum columnInfo 
	{
		id, 
		culture, 
		sex, 
		age, 
		date_of_birth, 
		title, 
		given_name, 
		surname, 
		state, 
		suburb, 
		postcode, 
		street_number, 
		address_1, 
		address_2, 
		phone_number
	}
	
	/**
	 * @param args
	 */	
	public static void main(String[] args) {
		try
		{
			Infrastructure test = new Infrastructure("C:\\DPDC_Exercise4\\addresses.tsv");
			HashMap<String, ArrayList<Integer>> grouping = test.GroupFileHashMap(new int[] { columnInfo.culture.ordinal(), columnInfo.postcode.ordinal(),columnInfo.date_of_birth.ordinal() });
			Set<Integer> groupIndex = new HashSet<Integer>();			
			for (String key: grouping.keySet())
			{							
				ArrayList<Integer> group = grouping.get(key);			
				groupIndex.add(group.get(0));								
			}			
			test.CreateFileBasedOnIndex(groupIndex, "C:\\DPDC_Exercise4\\test.tsv");									
		}
		catch (Exception ex)
		{
			System.out.println("read file failed: " + ex.getMessage());
		}
	}

}
