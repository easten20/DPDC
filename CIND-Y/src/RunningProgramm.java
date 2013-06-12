import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.SortedSet;


public class RunningProgramm {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException{
			long start = System.currentTimeMillis();				
			Infrastructure build = new Infrastructure();					
			build.CheckDependencies();		
			System.out.println("Duration in ms: " + (System.currentTimeMillis() - start));
			
	}

}
