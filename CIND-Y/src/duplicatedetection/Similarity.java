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
		
		if(value1.isEmpty() || value2.isEmpty()){
			return -1.0;
		}
		
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
		int totalWeight = 0;
		double previousScore = 0;
		for(int i = 0;i<numberOfColumns;i++){
			previousScore = sumOfSimilarityScore;
			if (i == 0) //we don't compare columns[0] == id
				continue;
			if(i==1){
				sumOfSimilarityScore += 5 * ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 
			}else if(i==2){
				sumOfSimilarityScore += 2 * ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 	
			}else if(i==3){
				sumOfSimilarityScore += 4 * ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 	
			}else if(i==4){
				sumOfSimilarityScore += 8 * ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 	
			}else if(i==5){
				sumOfSimilarityScore += 2 * ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 	
			}else if(i==6){
				sumOfSimilarityScore += 10 * ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 	
			}else if(i==7){
				sumOfSimilarityScore += 10 * ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 	
			}else if(i==8){
				sumOfSimilarityScore += 4 * ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 	
			}else if(i==9){
				sumOfSimilarityScore += 10 * ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 	
			}else if(i==10){
				sumOfSimilarityScore += 4 * ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 	
			}else if(i==11){
				sumOfSimilarityScore += 4 * ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 	
			}else if(i==12){
				sumOfSimilarityScore += 10 * ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 	
			}else if(i==13){
				sumOfSimilarityScore += 10 * ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 	
			}else{
				sumOfSimilarityScore += 10 * ScoreOfSimilarityForEachColumn(value1[i],value2[i]); 	
			}		
			
			if (previousScore >  sumOfSimilarityScore)
				sumOfSimilarityScore = previousScore;
			else
			{
			if(i==1){
				totalWeight += 5; 
			}else if(i==2){
				totalWeight += 2; 	
			}else if(i==3){
				totalWeight += 4; 	
			}else if(i==4){
				totalWeight += 8; 	
			}else if(i==5){
				totalWeight += 2; 	
			}else if(i==6){
				totalWeight += 10; 	
			}else if(i==7){
				totalWeight += 10; 	
			}else if(i==8){
				totalWeight += 4; 	
			}else if(i==9){
				totalWeight += 10; 	
			}else if(i==10){
				totalWeight += 4; 	
			}else if(i==11){
				totalWeight += 4; 	
			}else if(i==12){
				totalWeight += 10; 	
			}else if(i==13){
				totalWeight += 10; 	
			}else{
				totalWeight += 10; 	
			}		
			}
		}
		
		return sumOfSimilarityScore/totalWeight;
	}
	
	public boolean AreTwoRowsSame(String[] value1, String[] value2){
		
		if(ScoreOfSimilarityForEntireRow(value1,value2) > thresholdForRow) return true;
		else return false;
	}
	

	
	
}



