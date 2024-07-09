package DB;

/** * @author Wael Abouelsaadat */ 

public class SQLTerm {

	 public String _strTableName,_strColumnName, _strOperator;
	    public Comparable _objValue;

	    public SQLTerm(){

	    }
	    public SQLTerm(String _strTableName ,String _strColumnName, String strOperator,Comparable _objValue){
	    	this._strTableName=_strTableName;
	    	this._strColumnName=_strColumnName;
	    	this._strOperator=strOperator;
	    	this._objValue=_objValue;
	    }

}