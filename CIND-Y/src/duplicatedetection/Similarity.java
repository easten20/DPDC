package duplicatedetection;

import com.wcohen.ss.MongeElkan;



public class Similarity {
	
	static double threshold = 0.7;
	
	public Similarity()
	{
		
	}
	
	private double ScoreOfSimilarity(String value1, String value2){
		
		MongeElkan mongeElkan = new MongeElkan();
		
		return mongeElkan.score(value1, value2); 		// 0 < scoreOfSimilarity < 1
	}
	
	private boolean isSame(String value1, String value2)
	{
		MongeElkan mongeElkan = new MongeElkan();
		
		double scoreOfSimilarity = mongeElkan.score(value1, value2); // 0 < scoreOfSimilarity < 1
		
		if(scoreOfSimilarity > threshold) return true;
		else return false;
	}	
}
