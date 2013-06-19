package duplicatedetection;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import duplicatedetection.enums.columnInfo;
import duplicatedetection.enums.typeOfSearch;

public class TestProgramm {

	
	
	
	/**
	 * @param args
	 */	
	public static void main(String[] args) {
		try
		{
			Infrastructure test = new Infrastructure("C:\\DPDC_Exercise4\\addresses.tsv");
			SearchParameter searchParam1 = new SearchParameter(columnInfo.culture, typeOfSearch.INDEX);
			SearchParameter searchParam2 = new SearchParameter(columnInfo.date_of_birth, typeOfSearch.INDEX);
			SearchParameter searchParam3 = new SearchParameter(columnInfo.surname, typeOfSearch.PREFIX, 2);
			HashMap<String, Set<Integer>> grouping1 = test.GroupFileHashMap(new SearchParameter[] { searchParam1, searchParam2, searchParam3 });								
			ArrayList<Set<Integer>> resultFile1 = test.CheckRowSimilarity(grouping1);						
			searchParam1 = new SearchParameter(columnInfo.given_name, typeOfSearch.PREFIX, 2);
			searchParam2 = new SearchParameter(columnInfo.postcode, typeOfSearch.INDEX);
			searchParam3 = new SearchParameter(columnInfo.age, typeOfSearch.INDEX);
			HashMap<String, Set<Integer>> grouping2 = test.GroupFileHashMap(new SearchParameter[] { searchParam1, searchParam2, searchParam3 });								
			ArrayList<Set<Integer>> resultFile2 = test.CheckRowSimilarity(grouping2);
			
			//take a long time to intersect to collection
			resultFile1 = test.CombineIntersectSet(resultFile1, resultFile2);						
			
			System.out.println("finish inersect");
			System.out.println("writing to file..... don't close!!!!");
			test.CreateResultFile(resultFile1, "C:\\DPDC_Exercise4\\test.tsv");
			System.out.println("finish!!");
			
		}
		catch (Exception ex)
		{
			System.out.println("read file failed: " + ex.getMessage());
		}
	}

}
