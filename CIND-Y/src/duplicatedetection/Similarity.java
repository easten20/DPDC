package duplicatedetection;

import com.wcohen.ss.MongeElkan;



public class Similarity {
	
	static double thresholdForColumn = 0.7;
	static double thresholdForRow = 0.7;
	static int numberOfColumns = 15;
	
	public Similarity()
	{
		
	}
	
	private double ScoreOfSimilarityForEachColumn(String value1, String value2){
		
		MongeElkan mongeElkan = new MongeElkan();
		
		return mongeElkan.score(value1, value2); 		// 0 < scoreOfSimilarity < 1
	}
	
	private boolean AreTwoColumnsSame(String value1, String value2)
	{
		MongeElkan mongeElkan = new MongeElkan();
		
		double scoreOfSimilarity = mongeElkan.score(value1, value2); // 0 < scoreOfSimilarity < 1
		
		if(scoreOfSimilarity > thresholdForColumn) return true;
		else return false;
	}	
	
	
	private double ScoreOfSimilarityForEntireRow(String[] value1, String[] value2){ //compare two rows. First compare each columns and calculate similarity for each column, and get the sum of all of them. And then divide by the number of columns to retrieve average similarity between two rows. 
		
		double sumOfSimilarityScore = 0;
		
		for(int i = 0;i<numberOfColumns;i++){
			if (i == 0) //we don't compare columns[0] == id
				continue;
			sumOfSimilarityScore += ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 
		}
		
		return sumOfSimilarityScore/numberOfColumns;
	}
	
	public boolean AreTwoRowsSame(String[] value1, String[] value2){
		
		if(ScoreOfSimilarityForEntireRow(value1,value2) > thresholdForRow) return true;
		else return false;
	}
	
}
