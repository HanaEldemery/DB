package DB;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public class SerialBTree implements Serializable{
	
	public static void ser(BTree b,String colName, String tName) {
		try {
			/*FileOutputStream fileOut = new FileOutputStream("./src/DB"+tname+pageIndex+".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(p);
            out.close();
            fileOut.close();*/
			ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream("./src/DB/"+tName+colName+"B+Tree"+".ser")); 
			outputStream.writeObject(b);
			outputStream.close();
			
            //System.out.printf("Serialized data is saved in /tmp/employee.ser");
            //System.out.println();s
            //System.out.println("after"+ vecString.get(1));
		} 
		catch (IOException e) {
            e.printStackTrace();
        }
		
	}
	
	public static BTree deser(String colName, String tName) {
		BTree retPage = null;
        try {
           FileInputStream fileIn = new FileInputStream("./src/DB/"+tName+colName+"B+Tree"+".ser");
           ObjectInputStream in = new ObjectInputStream(fileIn);
           retPage = (BTree) in.readObject();
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
	


}

