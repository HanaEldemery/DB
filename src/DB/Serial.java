package DB;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public class Serial implements Serializable{
	
	public static void ser(Page p, int pageIndex,String tname) {
		try {
			/*FileOutputStream fileOut = new FileOutputStream("./src/DB"+tname+pageIndex+".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();*/
			ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("./src/DB/"+tname+pageIndex+".ser"));
			outputStream.writeObject(p);
			outputStream.close();
			
            //System.out.printf("Serialized data is saved in /tmp/employee.ser");
            //System.out.println();s
            //System.out.println("after"+ vecString.get(1));
		} 
		catch (IOException e) {
            e.printStackTrace();
        }
		
	}
	
	public static Page deser(int pageIndex, String tName) {
		Page retPage = null;
        try {
           FileInputStream fileIn = new FileInputStream("./src/DB/"+tName+pageIndex+".ser");
           ObjectInputStream in = new ObjectInputStream(fileIn);
           retPage = (Page) in.readObject();
           in.close();
           fileIn.close();
           return retPage;
        } 
        catch (IOException i) {
           i.printStackTrace();
        } 
        catch (ClassNotFoundException c) {
           System.out.println("Employee class not found");
           c.printStackTrace();
        }
		return retPage;
	}
	
	 public static void deleteFile(String fileName) {
	        File file = new File(fileName);
	        if (file.exists()) {
	            boolean deleted = file.delete();
	            if (deleted) {
	                //System.out.println("File deleted successfully.");
	            } else {
	                System.out.println("Failed to delete file.");
	            }
	        } else {
	            System.out.println("File does not exist.");
	        }
	    }

}
