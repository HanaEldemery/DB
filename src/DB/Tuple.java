/*package DB;
import java.io.Serializable;
import java.util.Vector;

public class tuple implements Serializable{
	private Vector<Object> t;
	public tuple(){
		t= new Vector<Object>();
		
	}
	public void addElem(Object o) {
		t.add(o);
	}
	public String toString() {
	       String s="";
	       for(int i=0;i<t.size();i++) {
	    	   s=s+""+t.get(i);
	    	   if(i != t.size()-1) {
	    		   s=s+",";
	    	   }
	       }
	       return s;
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
		
	}
	  
	
}
/*
 * 
 */
package DB;

import java.io.Serializable;
import java.util.Collection;
import java.util.Vector;

public class Tuple implements Serializable{
	private Vector<Object> tuple;
	//Vector<E> v = new Vector<E>();
	private int tupleClusterIndex;
	
    public Tuple() {
    	tuple = new Vector<Object>();
        //tupleClusterIndex
    }

    //public void createIndex(ArrayList<> a) {
    
    public Vector<Object> getTupleVectors() {
    	return tuple;
    }
    
    public void addElement(Object element) {
    	tuple.add(element);
    }
    
    public Object getElement(int indexElement) {
    	return tuple.get(indexElement);
    }
    
    public int getSize() { //4.4 recheck
    	/*Vector<Object> tuple= getTupleVectors();
    	int i= 0;
    	int count= 1;
    	while(tuple.get(i)!=null) {
    		i++;
    		count++;
    	}
    	return count;*/
    	return tuple.size();
    }
    
    public boolean equals2(Tuple t) {
    	System.out.println("tuple: "+t);
    	if(t.getSize()!=t.getSize())
    		return false;
    	for(int i=0;i<t.getSize();i++) {
    		if(tuple.get(i)!=t.getElement(i))
    			return false;
    	}
    	return true;
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        //sb.append("(");
        for (int i = 0; i < tuple.size(); i++) {
            sb.append(tuple.get(i));
            if (i < tuple.size() - 1) {
                sb.append(", ");
            }
        }
        //sb.append(")");
        return sb.toString();
    }
    
    /*int getIndexLas(Vector<Object> vecobj) {
    	for(int i=0; i<vecobj.size(); i++) {
    		
    	}
    	return retval;
    }*/

    public static void main(String[] args) {
    	Vector<Object> vecobj = new Vector<>();
    	Tuple t= new Tuple();
    	t.addElement("omar");
    	t.addElement(1);
    	Tuple t2= new Tuple();
    	t2.addElement("elmeligy");
    	Tuple t3= new Tuple();
    	t3.addElement(2);
    	Tuple t4= new Tuple();
    	t4.addElement(221);
    	t4.addElement('o');
    	vecobj.add(t);
    	vecobj.add(t2);
    	vecobj.add(t3);
    	vecobj.add(t4);
    	vecobj.remove(t3);
    	System.out.println(vecobj.size());
    	//int size= t.getTupleVectors().size();
    	//System.out.println(size);
    }

}