package DB;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;

public class testinggg {
	
	public static ArrayList<String []> metaReader3(String filePath) throws IOException{
		ArrayList<String []> ret= new ArrayList<String []>();
		//String csvFile = "C:\\Users\\Dell\\eclipse-workspace\\DB\\src\\DB\\metadata.csv";
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		String line = br.readLine();
		while (line != null) {
			String[] sp = line.split(",");
			line = br.readLine();
			ret.add(sp);
		}
		br.close();
		return ret;
	}
	
	public static void main(String[] args) throws IOException {
    	ArrayList<ArrayList<Object>> keyAndValue= new ArrayList<ArrayList<Object>>();
    	
    	ArrayList<Object> use1= new ArrayList<Object>();
    	use1.add(0.8);
    	use1.add(3);
    	use1.add("gpa");
    	use1.add("java.lang.Double");
    	keyAndValue.add(use1);
    	
    	ArrayList<Object> use2= new ArrayList<Object>();
    	use2.add(20);
    	use2.add(0);
    	use2.add("id");
    	use2.add("java.lang.Integer");
    	keyAndValue.add(use2);
    	
    	ArrayList<Object> use3= new ArrayList<Object>();
    	use3.add(20);
    	use3.add(1);
    	use3.add("number");
    	use3.add("java.lang.Integer");
    	keyAndValue.add(use3);
    	
    	ArrayList<String> testing= new ArrayList<String>();
    	/*for(int i=0; i < meta.size(); i++) {
    		testing.add(meta.get(i)[1]);
    	}*/
    	testing.add("id");
    	testing.add("number");
    	testing.add("name");
    	testing.add("gpa");
    	//System.out.println(testing);
    	
    	///////////////////
    	ArrayList<String> metaDataTypes= new ArrayList<String>();
    	metaDataTypes.add("java.lang.Integer");
    	metaDataTypes.add("java.lang.Integer");
    	metaDataTypes.add("java.lang.String");
    	metaDataTypes.add("java.lang.Double");
    	ArrayList<String> usedColumnsInDelete= new ArrayList<String>();
    	for(int x=0; x < testing.size(); x++) {
    		for(int y=0; y < keyAndValue.size(); y++) {
    			if((testing.get(x)).equals((String)keyAndValue.get(y).get(2))) {
    				usedColumnsInDelete.add(testing.get(x));
    			}
    		}
    	}
    	
    	System.out.println("usedColumnsInDelete :"+ usedColumnsInDelete);
    	
    	ArrayList<Integer> indexOfUsedColumnsInDelete= new ArrayList<Integer>();
    	for(int i=0; i < usedColumnsInDelete.size(); i++) {
    		for(int j=0; j < testing.size(); j++) {
    			if((usedColumnsInDelete.get(i)).equals(testing.get(j))) {
    				indexOfUsedColumnsInDelete.add(j);
    			}
    		}
    	}
    	
    	System.out.println(indexOfUsedColumnsInDelete);
    
    	boolean wrongDataType= true;
    	for(int i=0; i < usedColumnsInDelete.size(); i++) {
    		wrongDataType= true;
    		for(int j=0; j < keyAndValue.size(); j++) {
    			if(((usedColumnsInDelete.get(i)).equals((String)keyAndValue.get(j).get(2)))) {
    				if((indexOfUsedColumnsInDelete.get(i) == (int)keyAndValue.get(j).get(1)) && ((metaDataTypes.get(indexOfUsedColumnsInDelete.get(i))).equals(keyAndValue.get(j).get(3)))) {
        				wrongDataType= false;
        			}
    			}
    		}
    		if(wrongDataType == true) {
    			System.out.println("You are trying to delete wrong datatypes");
    			return;
    		}
    	}
    	
    	Properties properties = new Properties();
    	FileInputStream fis = new FileInputStream("C:\\Users\\Dell\\eclipse-workspace\\DBProject\\src\\DB\\DBApp.config");
        properties.load(fis);
        String maxSize = properties.getProperty("MaximumRowsCountinPage");
        System.out.println("size "+maxSize);
	}
}
