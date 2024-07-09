package DB;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

class test implements Serializable{
    // Returns index of x if it is present in arr[l....r], else return -1
    int binarySearch(int arr[], int l, int r, int x)
    {
        while (l <= r) {
            int mid = (l + r) / 2;
 
            // If the element is present at the
            // middle itself
            if (arr[l]<x && arr[r]>x && r-l==1) {
                return r;
 
            // If element is smaller than mid, then
            // it can only be present in left subarray
            // so we decrease our r pointer to mid - 1 
            } else if (arr[mid] > x) {
                r = mid - 1;
 
            // Else the element can only be present
            // in right subarray
            // so we increase our l pointer to mid + 1
            } else {
              l = mid + 1;
            }  
        }
 
        // We reach here when element is not present
        //  in array
        return -1;
    }
    
    public static Vector<Integer> arrayListToVector1(ArrayList<Integer> tupleToAdd) {
		Vector<Integer> vector = new Vector<>();
		
        for(int i=0;i<tupleToAdd.size();i++) {
            vector.add(tupleToAdd.get(i));
        }
        return vector;
	}
    
    
    
    public static void main(String args[])
    {
    	/*
    	Vector<ArrayList<String>> vecString= new Vector<ArrayList<String>>();
    	ArrayList<String> arrayList= new ArrayList<String>();
    	arrayList.add("omar");
    	ArrayList<String> arrayList1= new ArrayList<String>();
    	arrayList1.add("hana");
    	ArrayList<String> arrayList2= new ArrayList<String>();
    	arrayList2.add("david");
    	vecString.add(arrayList);
    	vecString.add(arrayList1);
    	vecString.add(arrayList2);
    	*/
    	
    	//Table t=new Table("tName");
    	
    	
    	//SerialCount.ser(5,"test");
    	//SerialCount.ser(7, "test");
    	System.out.println(SerialCount.deser("test"));
    	SerialCount.ser(8, "test");
    	System.out.println(SerialCount.deser("test"));
    	SerialCount.ser(9, "test");
    	System.out.println(SerialCount.deser("test"));
    	System.out.println(SerialCount.deser("test"));
    	System.out.println(SerialCount.deser("test"));
    	SerialCount.ser(10, "test");
    	SerialCount.ser(11, "test");
    	SerialCount.ser(12, "test");
    	System.out.println(SerialCount.deser("test"));
    	
    	
    	/*Vector<Page> v=new Vector<Page>();
    	
    	
    	
    	
    	
    	Page p= new Page("test");
        Tuple myTuple = new Tuple();
        myTuple.addElement(123);
        Tuple myTuple2 = new Tuple();
        myTuple2.addElement(123);
        Tuple myTuple3 = new Tuple();
        myTuple3.addElement(123);
        Tuple myTuple4 = new Tuple();
        myTuple4.addElement(123);
        p.addElement(myTuple);
        p.addElement(myTuple2);
        p.addElement(myTuple3);
        p.addElement(myTuple4);
        
        Page p2= new Page("test2");
        Tuple myTuple5 = new Tuple();
        myTuple5.addElement(123);
        Tuple myTuple6 = new Tuple();
        myTuple6.addElement(123);
        Tuple myTuple7 = new Tuple();
        myTuple7.addElement(123);
        Tuple myTuple8 = new Tuple();
        myTuple8.addElement(123);
        p2.addElement(myTuple5);
        p2.addElement(myTuple6);
        p2.addElement(myTuple7);
        p2.addElement(myTuple8);
        
        Page p3= new Page("test3");
        Tuple myTuple9 = new Tuple();
        myTuple9.addElement(123);
        Tuple myTuple10 = new Tuple();
        myTuple10.addElement(123);
        p3.addElement(myTuple9);
        p3.addElement(myTuple10);
        
        
        
        v.add(p);
        v.add(p2);
        v.add(p3);
        
        
        
        try {
			FileOutputStream fileOut = new FileOutputStream("ttt.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(v);
            out.close();
            fileOut.close();
            //System.out.printf("Serialized data is saved in /tmp/employee.class");
            //System.out.println();
            //System.out.println("after"+ vecString.get(1));
		} catch (IOException e) {
            e.printStackTrace();
         }//akenny ba serialize page page
        
        
        Serial.ser(p, 0, "tn");
        Serial.ser(p2, 1, "tn");
        Serial.ser(p3, 2, "tn");
        
        
        Vector<Page> unser=null;
        try {
           FileInputStream fileIn = new FileInputStream("ttt.ser");
           ObjectInputStream in = new ObjectInputStream(fileIn);
           unser = (Vector<Page>) in.readObject();
           in.close();
           fileIn.close();
        } catch (IOException i) {
           i.printStackTrace();
           return;
        } catch (ClassNotFoundException c) {
           System.out.println("Employee class not found");
           c.printStackTrace();
           return;
        }
        
        System.out.println(unser.size());
        System.out.println(unser.get(0));*/
        
        
        //Page pp=Serial.deser(0, "tn");
        //Page pp1=Serial.deser(0, "tn");
        //System.out.println(pp1);
        
    	
    	//System.out.println(vecString);
    	/*System.out.println("before"+ vecString.get(1));
    	
    	
    	try {
    			FileOutputStream fileOut = new FileOutputStream("employeee.ser");
                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                out.writeObject(vecString);
                out.close();
                fileOut.close();
                //System.out.printf("Serialized data is saved in /tmp/employee.class");
                //System.out.println();
                //System.out.println("after"+ vecString.get(1));
    		} catch (IOException e) {
                e.printStackTrace();
             }//akenny ba serialize page page
    	
    	try {
			FileOutputStream fileOut = new FileOutputStream("employeee.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(vecString);
            out.close();
            fileOut.close();
            //System.out.printf("Serialized data is saved in /tmp/employee.class");
            //System.out.println();
            //System.out.println("after"+ vecString.get(1));
		} catch (IOException e) {
            e.printStackTrace();
         }//akenny ba serialize page page
         */
 
    	/*for (int i=0;i<vecString.size();i++) {
    		try {
    			FileOutputStream fileOut = new FileOutputStream("employeee"+i+".ser");
                ObjectOutputStream out = new ObjectOutputStream(fileOut);
                out.writeObject(vecString.get(i));
                out.close();
                fileOut.close();
                //System.out.printf("Serialized data is saved in /tmp/employee.ser");
                //System.out.println();
                //System.out.println("after"+ vecString.get(1));
    		} catch (IOException e) {
                e.printStackTrace();
             }//akenny ba serialize page page
    	}
    			
            
    	
    	//System.out.println("after"+ vecString.get(1));
        //System.out.println("\n no: "+ vecString);
    	
    	//ArrayList<String> unser = null;
    	
    	
    	Vector<ArrayList<String>> unser=null;
        try {
           FileInputStream fileIn = new FileInputStream("employeee.class");
           ObjectInputStream in = new ObjectInputStream(fileIn);
           unser = (Vector<ArrayList<String>>) in.readObject();
           in.close();
           fileIn.close();
        } catch (IOException i) {
           i.printStackTrace();
           return;
        } catch (ClassNotFoundException c) {
           System.out.println("Employee class not found");
           c.printStackTrace();
           return;
        }
        
        try {
            FileInputStream fileIn = new FileInputStream("employeee.class");
            ObjectInputStream in = new ObjectInputStream(fileIn);
            unser = (Vector<ArrayList<String>>) in.readObject();
            in.close();
            fileIn.close();
         } catch (IOException i) {
            i.printStackTrace();
            return;
         } catch (ClassNotFoundException c) {
            System.out.println("Employee class not found");
            c.printStackTrace();
            return;
         }
        
        
        System.out.println("\n yes: "+ unser.size());
        System.out.println("\n yes: "+ unser.get(1));
        */
        
        
    	
    	
    	
    	
    	
    	
    	
    	
    	//System.out.println(0/2);
    	/*Vector<Object> myVector = new Vector<>();
    	Vector<Object> forReturn = new Vector<>();
    	for(int i=1; i<11; i++) {
        	myVector.add(i);
        }
    	System.out.println(myVector.get(9));*/
        //myVector.add(null);
    	/*int i=6;
    	while(i>5 && myVector.get(i)!=null) {
    		forReturn.add(myVector.get(i));
    		i++;
    	}
    	int j=6;
    	while(j>5 && myVector.get(j)!=null) {
    		myVector.remove(j);
    		j++;
    	}
        
        System.out.println("myVector: " + myVector);
        System.out.println("forReturn: " + forReturn);*/
    	 /*ArrayList<Integer> arrayList = new ArrayList<>();
         arrayList.add(1);
         arrayList.add(2);
         arrayList.add(3);
    	
         Vector<Integer> vector = arrayListToVector1(arrayList);
         System.out.println("Vector: " + vector);*/
        /*test ob = new test();
 
        int arr[] = { 2, 3, 4, 10, 40 };
        int n = arr.length;
        int x = 10;
        int result = ob.binarySearch(arr, 0, n - 1, 11);
 
        if (result == -1)
            System.out.println("Element not present");
        else
            System.out.println("Element found at index "
                               + result);*/
    	/*Vector<Integer> myVector = new Vector<>(5);
        myVector.add(1);
        myVector.add(3);
        myVector.add(4);
        myVector.add(5);
        myVector.add(null);

        // Print the original vector
        System.out.println("Original Vector: " + myVector);

        // Insert the element '2' at index 1
        myVector.insertElementAt(2, 1);

        // Print the vector after inserting the element
        System.out.println("Vector after inserting element: " + myVector);*/
    	
    	/*Vector<String> tupleVectors = new Vector<String>(5);
    	tupleVectors.add("a");
    	tupleVectors.add("b");
    	tupleVectors.add("c");
    	System.out.println(tupleVectors.size());
    	tupleVectors.remove(0);
    	System.out.println(tupleVectors.get(1));
    	
    	Object x=5;
    	Object y=6;
    	//Comparable c1=x;
    	//Comparable c2=y;
    	
    	System.out.println(((Comparable)x).compareTo(y));
    	//Double y=7.0;
    	//String s="a";*/
    	
    	
    	
    	
    }
}