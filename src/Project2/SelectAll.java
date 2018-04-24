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
import Project2.Show;

public class SelectAll{
	public static void selectAll(String queryString) 
	{
		try
		{
			String[] tokens = queryString.split(" ");
			String tableName = tokens[3].trim();
			String toDisplay = queryString.substring(queryString.indexOf("select") + 6, queryString.indexOf("from")).trim();
			String[] toDisplayArray = toDisplay.split(",");
			ArrayList<String> toDisplayList = new ArrayList<>();
			if(tableName.equals("davisbase_tables"))
			{
				Show.showTables();
			}
			else if(CheckTable.checkTable(tableName))
			{
				String currentDir = System.getProperty("user.dir");
				String path = currentDir + File.separator + "data" + File.separator + "userData" + File.separator + tableName +".tbl";
				RandomAccessFile tableFile = new RandomAccessFile(path, "rw");
				List<ColumnDetails> columnsList = GetColumns.fetchColumnDetails(tableName);
				if(toDisplayArray.length==1 && toDisplayArray[0].trim().equals("*")) {
					for (int i=0; i<columnsList.size();i++) {
						toDisplayList.add(columnsList.get(i).getColumnName().trim());
					}
				
				}
				else {
				toDisplayList = new ArrayList<>(Arrays.asList(toDisplayArray));
				}
				String toAdd = "";
				for (int d = 0; d < 20; d++) {
					toAdd += " ";
				} 
				
				
				int firstByteLocation = 0;
				int numofRecords = 0;
				int startLocation = 0;
				int nextNodeLocation = 0;
				int lastPointerLocation = 0;
				int difference = 0;
				int pageType = 0;
				int pageNum = 0;
				int rowNum = 0;
				int count = 0;
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
						//recordAddress.add(Short.valueOf(tableFile.readShort()));
						recordAddress.add(tableFile.readShort());
					}
					if (nextNodeLocation != -1)
					{
						firstByteLocation = firstByteLocation +512;
					}
				}
				while(nextNodeLocation != -1);
				
				
				for(int k=0; k<recordAddress.size();k++)
				{
					tableFile.seek(( recordAddress.get(k)).shortValue());
					int isDeleted = tableFile.readByte(); 
					if(isDeleted == 0)
					{
						count += 1;
						if(count==1) {
							for (String c : toDisplayList)
							{
								System.out.print("  " +c.toUpperCase()+toAdd);
							}
							
							System.out.println("");
						}
						for (int i=0; i<columnsList.size();i++)
						{
							String col = columnsList.get(i).getColumnName();
							String dataType= columnsList.get(i).getDataType();
							String val = "";
							switch(dataType)
							{
								case "tinyint":
									val = ""+ tableFile.readByte() ;
									if(toDisplayList.contains(col)) {
									System.out.print("  " + val+toAdd+"|");
									}
									break;
								case "smallint":
									val = ""+tableFile.readShort();
									if(toDisplayList.contains(col)) {
									System.out.print("  " +val+toAdd+"|");
									}
									break;
								case "int":
									val = ""+tableFile.readInt();
									if(toDisplayList.contains(col)) {
									System.out.print("  " +val +toAdd+"|");
									}
									break;
								case "bigint":
									val = ""+tableFile.readLong();
									if(toDisplayList.contains(col)) {
									System.out.print("  " +val +toAdd+"|");
									}
									break;
								case "real":
									val = ""+tableFile.readFloat();
									if(toDisplayList.contains(col)) {
									System.out.print("  " +val +toAdd+"|");
									}
									break;
								case "double":
									val = ""+tableFile.readDouble();
									if(toDisplayList.contains(col)) {
									System.out.print("  " +val +toAdd+"|");
									}
									break;
								case "datetime":
									val = ""+convertDateTimeToString(tableFile.readLong());
									if(toDisplayList.contains(col)) {
									System.out.print("  " +val +toAdd+"|");
									}
									break;
								case "date":
									val = ""+convertDateToString(tableFile.readLong());
									if(toDisplayList.contains(col)) {
									System.out.print("  " + val+toAdd+"|");
									}
									break;
								default:
									val = ""+tableFile.readUTF();
									if(toDisplayList.contains(col)) {
									System.out.print("  "+val+toAdd+"|");
									}
							}
							}
						
						System.out.println();
					}
				}
				if(count==0) {
					System.out.println("No Records");
				}
				tableFile.close();
			}
		}
		catch (Exception e)
		{
			System.out.println(e);
		}
		
	
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
	
}