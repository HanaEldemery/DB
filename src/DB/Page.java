/*package DB;
import java.io.Serializable;
import java.util.Vector;

public class page implements Serializable{
	private Vector<tuple> p;
	final static int size= ;
	public page(){
		p= new Vector<tuple>();
		
	}
	public void addElem(tuple t) {
		p.add(t);
	}
	   public String toString() {
	        StringBuilder sb = new StringBuilder();
	        sb.append("Rows:\n");
	        for (int i = 0; i<p.size() ; i++) {
	        	if(i>=size){
	        		break;
	        	}
	        	else {
	        		sb.append("Row ").append(i).append(": ");
	                sb.append(p.get(i)).append("\n");
	        	}
	            
	        }
	        return sb.toString();
	    }
	
	public static void main(String[] args) {
		tuple t=new tuple();
		t.addElem("ahmed");
		t.addElem(18);
		//System.out.print(t);
		tuple t1=new tuple();
		t1.addElem("David");
		t1.addElem(20);
		//System.out.print(t1);
		page p= new page();
		p.addElem(t);
		p.addElem(t1);
		System.out.print(t );
		System.out.print(t1);
	}
}*/
package DB;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Vector;

//add serialize
//vector wala array
public class Page implements Serializable{
	private Vector<Tuple> page; //changed from static 9.4
	static /*final*/ int size;
	private static String pName;
	private int count;
	private Tuple first; //changed 9.4
	private Tuple last; //changed 9.4
	private static String tableNameOfPage;

	
	static {
        Properties properties = new Properties();
        String filePath = System.getProperty("user.dir")+"/src/DB/DBApp.config";
        try (FileInputStream fis = new FileInputStream(filePath)) {
            properties.load(fis);
            String maxSize = properties.getProperty("MaximumRowsCountinPage");
            size = Integer.parseInt(maxSize);
        } catch (IOException e) {
            e.printStackTrace();
            //size= -1;
        }
    }
	
    /*public Page() {
    	Properties properties = new Properties();
        try (FileInputStream fis = new FileInputStream("C:\\Users\\Dell\\eclipse-workspace\\DBProject\\src\\DB\\DBApp.config")) {
            properties.load(fis);
            String maxSize = properties.getProperty("MaximumRowsCountinPage");
            size = Integer.parseInt(maxSize); 
        } catch (IOException e) {
            e.printStackTrace();
        }
    }*/
    
	public void setFirst(Tuple first) {
        this.first = first;
    }

    public Tuple getFirst() {
        return first;
    }

    public void setLast(Tuple last) {
        this.last = last;
    }

    public Tuple getLast() {
        return last;
    } 
    
    public void setCount(int num) {
    	this.count= num;
    }
    
    public int getCount() {
    	return count;
    }
	
    public Page(String name) {
    	pName=name;
    	//first=page.get(0);
    	//last=page.get(count);
    	page = new Vector<Tuple>(size);
    }
    
    public int getSize() { //4.4 recheck
    	return page.size();
    	/*Vector<Tuple> page= getTupleVectors();
    	int i= 0;
    	int count= 1;
    	while(page.get(i)!=null) {
    		i++;
    		count++;
    	}
    	return count;*/
    }
    
    /*public Vector<Tuple> getTupleVectors() {
        return page;
    }*/
    
    public String toString() {
    	 StringBuilder stringBuilder = new StringBuilder();
         for (int i = 0; i < this.getSize(); i++) {
             Tuple tuple = this.getTuple(i);
             /*for (int j=0;j<tuple.getSize();j++) {
            	 /*if (j < tuple.getSize()-1)
            		 stringBuilder.append(tuple.getElement(j)).append(", ");
            	 else
            		 stringBuilder.append(tuple.getElement(j)).append(", ");*/
            	 stringBuilder.append(tuple.toString());
                 
             //}
             //stringBuilder.append(tuple.toString());
             if (i < this.getSize() - 1) {
                 stringBuilder.append(", ");
             }
         }
         return stringBuilder.toString();
        /*StringBuilder sb = new StringBuilder();
        sb.append("Page: \n");
        for (int i = 0; i<page.size() ; i++) {
        	if(i>=size){
        		break;
        	}
        	else {
        		sb.append("Tuple ").append(i).append(": ");
                sb.append(page.get(i)).append("\n");
        	}
            
        }
        return sb.toString();*/
    }
    
    public void addElement(Tuple element) {
        page.add(element);
    }
    
    public void removeElement(Tuple element) {
    	page.remove(element);
    }
    
    public void insertElementAt(Tuple tuple, int index) { //4.4 recheck inserts more than 200, must loop and relocate extra to the next page
    	Vector<Tuple> forReturn= new Vector<Tuple>();
    	
    	//System.out.println(tuple);
    	
    	int theSize= getSize();
    	
    	for(int i=index; i<theSize; i++) {
    		forReturn.add(getTuple(i));
    	}
  
    	for(int i=index; i<theSize; i++) {
    		removeElement(getTuple(index));
    	}
    	
    	addElement(tuple);
    	
    	for(int i=0; i<forReturn.size(); i++) {
    		addElement(forReturn.get(i));
    	}
    	
      	setCount(getCount()+1); //increments count by 1 after insertion
      	setFirst(getTuple(0));
      	if(getCount() > size) { //overflow
      		setLast(getTuple(size-1)); //last is set to the tuple on top of the overflow tuple
      	}
      	else {
      		setLast(getTuple(getCount()-1)); //last is set to the last tuple of the page
      	}
      	
  
    }
    
    public Tuple relocateExtra() { //4.4 goes right after insertElementAt
    	Tuple forReturn= new Tuple();
    	forReturn= getTuple(size);
    	removeElement(getTuple(size));
    	setCount(getCount()-1); //decrement count after removing overflow tuple
    	return forReturn;
    }
    
    public static Tuple relocateExtra2(Page p) { //4.4 goes right after insertElementAt
    	Tuple forReturn= new Tuple();
    	forReturn= p.getTuple(size);
    	p.removeElement(p.getTuple(size));
    	return forReturn;
    }

    public static Tuple arrayToVector(ArrayList<Object> a) {
    	Tuple t=new Tuple();
    	for (int i=0;i<a.size();i++) {
    		t.addElement(a.get(i));
    	}
    	return t;
    }
    
	public void insertIntoPage(ArrayList<Object> aValues) {
		Tuple t= arrayToVector(aValues);
		page.add(t);
		//serialize
	}
    
	public Tuple getTuple(int index) {
        if (index >= 0 && index < page.size()) {
            return page.get(index);
        } 
        else {
            return null; // or throw an exception indicating index out of bounds
        }
    }
	
	/*public void insertElementAt(ArrayList<Object> tupleToAdd, int i) {
		// TODO Auto-generated method stub
		
	}*/
	
   /* public static void main(String[] args) {
        page p = new page();

        tuple myTuple = new tuple(4);
        myTuple.addElement(123);
        p.addElement(new tuple(myTuple)); // Create a new tuple object and add it to p

        tuple myTuple2 = new tuple(5);
        myTuple2.addElement(179);
        p.addElement(new tuple(myTuple2)); // Create a new tuple object and add it to p

        // Repeat the above steps for other tuples as needed

        System.out.println(p.toString());
    }*/
	
	public static void main(String[] args) {
		
		
		/*Page p= new Page("test");
		Tuple t= new Tuple();
		t.addElement(1);
		t.addElement("hana");
		t.addElement(4);
		p.addElement(t);
		
		Tuple t2= new Tuple();
		t2.addElement(3);
		t2.addElement("hana");
		t2.addElement(4);
		p.addElement(t2);
		
		Tuple t3= new Tuple();
		t3.addElement(2);
		t3.addElement("hana");
		t3.addElement(4);
		p.insertElementAt(t3, 1);*/
		
		
		//System.out.println(p);
		//Vector<Page> tst= new Vector<Page>();
		
		Page p1 = new Page("page1");
        Tuple t1 = new Tuple();
        t1.addElement(1);
        t1.addElement("hana");
        t1.addElement(4);
        p1.addElement(t1);
        //p1.setFirst(p1.getTuple(0));
        //tst.add(p1);

        //Page p2 = new Page("page2");
        Tuple t2 = new Tuple();
        t2.addElement(3);
        t2.addElement("hana");
        t2.addElement(4);
        p1.addElement(t2);
        //p2.setFirst(p2.getTuple(0));
        //tst.add(p2);

        //Page p3 = new Page("page3");
        Tuple t3 = new Tuple();
        t3.addElement(2);
        t3.addElement("hana");
        t3.addElement(4);
        p1.addElement(t3);
        //p3.setFirst(p3.getTuple(0));
        //tst.add(p3);
        
        System.out.println(p1);

        /*System.out.println(p1);
        System.out.println(p1.getFirst());
        System.out.println(p1.getLast());
        System.out.println(p2);
        System.out.println(p2.getFirst());
        System.out.println(p2.getLast());
        System.out.println(p3);
        System.out.println(p3.getFirst());
        System.out.println(p3.getLast());*/
        //System.out.println(tst.get(1).getFirst());
        //System.out.println(tst.get(1).getLast());
        
        /*page p= new page();
        tuple myTuple = new tuple(4);
        myTuple.addElement(123);
        p.addElement(myTuple);
        tuple myTuple2 = new tuple(5);
        myTuple2.addElement(179);
        p.addElement(myTuple2);
        tuple myTuple3 = new tuple(3);
        myTuple3.addElement(1234);
        p.addElement(myTuple3);
        /*tuple myTuple4 = new tuple(4);
        myTuple4.addElement(123);
        p.addElement(myTuple4);
        tuple myTuple5 = new tuple(9);
        myTuple5.addElement(555);
        p.addElement(myTuple5);
        tuple myTuple6 = new tuple(98);
        myTuple6.addElement(999);
        p.addElement(myTuple6);
        tuple myTuple7 = new tuple(98);
        myTuple7.addElement(999);
        p.addElement(myTuple7);
        System.out.println(p.toString());*/
        
     }

	


}
