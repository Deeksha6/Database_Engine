package Project2;

import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import Project2.CheckTable;
import Project2.GetColumns;

public class SelectWhere{
	public static List<String> selectWhere(String queryString) 
	{
		List<String> resultRecords = new ArrayList<String>();
		try
		{
			String[] elements = queryString.split(" ");
			String tableName = elements[3].trim();
			if(CheckTable.checkTable(tableName))
			{
				String toDisplay = queryString.substring(queryString.indexOf("select") + 6, queryString.indexOf("from")).trim();
				String[] toDisplayArray = toDisplay.split(",");
				/*for(int v=0;v<toDisplayArray.length;v++) {
					System.out.println("toDisplayArray : "+toDisplayArray[v]);
				}*/
				
				String whereValue = queryString.substring(queryString.indexOf("where") + 5, queryString.length()).trim();
				String[] whereValueArray = whereValue.replaceAll("\'","").replaceAll("\"","").split(" ");
				//String operator = whereValueArray[1];
				List<ColumnDetails> columnsList = GetColumns.fetchColumnDetails(tableName);
				ArrayList<String> toDisplayList = new ArrayList<>();
				if(toDisplayArray.length==1 && toDisplayArray[0].trim().equals("*")) {
					for (int i=0; i<columnsList.size();i++) {
						toDisplayList.add(columnsList.get(i).getColumnName().trim());
					}
				
				}
				else {
				toDisplayList = new ArrayList<>(Arrays.asList(toDisplayArray));
				}
				//System.out.println("toDisplayList "+toDisplayList);
				String operator = whereValueArray[1];
				String currentDir = System.getProperty("user.dir");
				String path = currentDir + File.separator + "data" + File.separator + "userData" + File.separator + tableName +".tbl";
				RandomAccessFile tableFile = new RandomAccessFile(path, "rw");
				
				int firstByteLocation = 0;
				int numofRecords = 0;
				int startLocation = 0;
				int nextNodeLocation = 0;
				int lastPointerLocation = 0;
				int difference =0;
				int pageType=0;
				int pageNum = 0;
				int rowNum = 0;
				ArrayList<Short> recordAddress = new ArrayList<Short>();
				
				do 
				{
					tableFile.seek(firstByteLocation);
					pageType = tableFile.readByte();
					pageNum = tableFile.readByte();
					numofRecords = tableFile.readByte();
					startLocation = tableFile.readShort();
					nextNodeLocation = tableFile.readInt();
					for(int i=0; i<numofRecords;i++) 
					{
						recordAddress.add(tableFile.readShort());
					}
					if (nextNodeLocation != -1)
					{
						firstByteLocation = firstByteLocation +512;
					}
				}
				while(nextNodeLocation != -1);
				for(int j=0; j<recordAddress.size();j++)
				{
					tableFile.seek(( recordAddress.get(j)).shortValue());
					int isDeleted = tableFile.readByte();
					if(isDeleted == 0)
					{
						Boolean whereFlag = false;
						String outputString = "  ";
						String value = "";
						String val = "";
						//int dispFlag = 0;
						for (int i=0; i<columnsList.size();i++)
						{
							int dispFlag = 0;
							if(toDisplayList.contains(columnsList.get(i).getColumnName())) {
								dispFlag = 1;
							}
							//System.out.println("dispFlag "+dispFlag);
							String dataType= columnsList.get(i).getDataType();
							switch(dataType)
							{
								case "tinyint":
									value = "" + tableFile.readByte() ;
									if(dispFlag==1) {
									val = value ;
									}
									else {
										val="";
									}
									
									if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && value.equals(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Byte.parseByte(value) < Byte.parseByte(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Byte.parseByte(value) > Byte.parseByte(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Byte.parseByte(value) <= Byte.parseByte(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Byte.parseByte(value) >= Byte.parseByte(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									outputString = outputString + " " + val;
									break;
								case "smallint":
									value = "" + tableFile.readShort();
									if(dispFlag==1) {
										val = value;
									}
									else {
										val="";
									}
									
									if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && value.equals(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Short.parseShort(value) < Short.parseShort(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Short.parseShort(value) > Short.parseShort(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Short.parseShort(value) <= Short.parseShort(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Short.parseShort(value) >= Short.parseShort(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									outputString = outputString + " " + val;
									break;
								case "int":
									value = "" + tableFile.readInt();
									if(dispFlag==1) {
										val = value;
									}
									else {
										val="";
									}
									
									if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && value.equals(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Integer.parseInt(value) < Integer.parseInt(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Integer.parseInt(value) > Integer.parseInt(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Integer.parseInt(value) <= Integer.parseInt(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Integer.parseInt(value) >= Integer.parseInt(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									outputString = outputString + " " + val;
									break;
								case "bigint":
									value = "" + tableFile.readLong();
									if(dispFlag==1) {
										val = value;
									}
									else {
										val="";
									}
									
									if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && value.equals(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Long.parseLong(value) < Long.parseLong(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Long.parseLong(value) > Long.parseLong(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Long.parseLong(value) <= Long.parseLong(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Long.parseLong(value) >= Long.parseLong(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									outputString = outputString + " " + val;
									break;
								case "real":
									value = "" + tableFile.readFloat();
									if(dispFlag==1) {
										val = value;
									}
									else {
										val="";
									}
									
									if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && value.equals(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Float.parseFloat(value) < Float.parseFloat(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Float.parseFloat(value) > Float.parseFloat(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Float.parseFloat(value) <= Float.parseFloat(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Float.parseFloat(value) >= Float.parseFloat(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									outputString = outputString + " " + val;
									break;
								case "double":
									value = "" + tableFile.readDouble();
									if(dispFlag==1) {
										val = value;
									}
									else {
										val="";
									}
									
									if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && value.equals(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Double.parseDouble(value) < Double.parseDouble(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Double.parseDouble(value) > Double.parseDouble(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Double.parseDouble(value) <= Double.parseDouble(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Double.parseDouble(value) >= Double.parseDouble(whereValueArray[2])) 
									{
										whereFlag = true;
									outputString = outputString + " " + val;
									}
									break;
								case "datetime":
									value = "" + convertDateTimeToString(tableFile.readLong());
									if(dispFlag==1) {
										val = value;
									}
									else {
										val="";
									}
									
									if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && value.equals(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) < convertStringToDate(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) > convertStringToDate(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) <= convertStringToDate(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) >= convertStringToDate(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									outputString = outputString + " " + val;
									break;
								case "date":
									value = "" + convertDateToString(tableFile.readLong());
									if(dispFlag==1) {
										val = value;
									}
									else {
										val="";
									}
									
									if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && value.equals(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) < convertStringToDate(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) > convertStringToDate(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) <= convertStringToDate(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) >= convertStringToDate(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									outputString = outputString + " " + val;
									break;
								default:
									value = "" + tableFile.readUTF();
									if(dispFlag==1) {
									val = value;
									}
									else {
										val="";
									}
									
									if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && value.equals(whereValueArray[2])) 
									{
										whereFlag = true;
									}
									outputString = outputString + " " + val;
							
							}
							
						}
						if(whereFlag)
						{
							resultRecords.add(outputString) ; 
						}
						
					}
				}
				tableFile.close();
				
			}
		}
		catch(Exception e)
		{
			System.out.println(e);
		}
		return resultRecords;
	}
	
	
	public static String convertDateTimeToString(long date) {
		String datePattern = "YYYY-MM-DD_hh:mm:ss";
		SimpleDateFormat dateFormat = new SimpleDateFormat(datePattern);
		Date currentdate = new Date(date);
		return dateFormat.format(currentdate);
	}

	public static String convertDateToString(long date) {
		String datePattern = "MM/dd/yyyy";
		SimpleDateFormat dateFormat = new SimpleDateFormat(datePattern);
		Date currentdate = new Date(date);
		return dateFormat.format(currentdate);
	}
	
	public static long convertStringToDate(String dateString) 
	{
		String datePattern = "MM/dd/yyyy";
		SimpleDateFormat dateFormat = new SimpleDateFormat(datePattern);
		try {
			Date date = dateFormat.parse(dateString);
			return date.getTime();
			
		} catch (Exception e) {
			System.out.println(e);
		}
		return new Date().getTime();
	}
	
}