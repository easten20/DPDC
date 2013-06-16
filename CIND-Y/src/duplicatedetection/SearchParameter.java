package duplicatedetection;

import java.awt.datatransfer.StringSelection;

import duplicatedetection.enums.columnInfo;
import duplicatedetection.enums.typeOfSearch;

public class SearchParameter {
	
	private columnInfo columnIndex;	
	private int comparerLength;
	private typeOfSearch tos;
	
	public SearchParameter(columnInfo columnInfo, typeOfSearch tos)
	{
		this.columnIndex = columnInfo;
		this.tos = tos;
	}
	
	public SearchParameter(columnInfo columnInfo, typeOfSearch tos, int comparerLength)
	{
		this.columnIndex = columnInfo;
		this.tos = tos;		
		this.comparerLength = comparerLength;
	}
	
	public typeOfSearch GetTypeOfSearch()
	{
		return this.tos;
	}
	
	public int GetColumnIndex()
	{
		return this.columnIndex.ordinal();
	}
	
	public String GetColumnValue(String value)
	{		
		if (value.length() < this.comparerLength+1)
			return value;
		switch(this.tos)
		{
			case PREFIX:				
					value = value.substring(0, this.comparerLength+1);
				break;
			case SUFFIX:									
					value = value.substring(value.length()-this.comparerLength, value.length());
				break;	
			case INDEX:
				break;
		}
		
		return value;
	}

}
