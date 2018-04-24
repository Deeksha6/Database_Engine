package Project2;


import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.io.File;
import java.util.ArrayList;

import java.util.Date;
import java.util.List;
import Project2.CheckTable;
import Project2.GetColumns;

public class Update {
	public static void parseUpdateString(String updateQuery) {
		try {
			String[] updateElements = updateQuery.split(" ");
			String tableName = updateElements[1].trim();
			if (CheckTable.checkTable(tableName)) {
				String setValue = updateQuery.substring(updateQuery.indexOf("set") + 3, updateQuery.indexOf("where"))
						.trim();
				String[] setValueArray = setValue.replaceAll("\\s+", "").replaceAll("\'","").replaceAll("\"","").split("=");
				String whereValue = updateQuery.substring(updateQuery.indexOf("where") + 5, updateQuery.length()).trim();
				String[] whereValueArray = whereValue.replaceAll("\'","").replaceAll("\"","").split(" ");
				String operator = whereValueArray[1];
				String currentDir = System.getProperty("user.dir");
				String path = currentDir + File.separator + "data" + File.separator + "userData"
						+ File.separator + tableName + ".tbl";
				RandomAccessFile tableFile = new RandomAccessFile(path, "rw");
				List<ColumnDetails> columnsList = GetColumns.fetchColumnDetails(tableName);

				int firstByteLocation = 0;
				int numofRecords = 0;
				int startLocation = 0;
				int nextNodeLocation = 0;
				int pageType = 0;
				int pageNum = 0;
				int rowNum = 0;
				int index = 0;
				String dType = "";
				
				Short valueSize = 0;
				int pointer = 0;

				ArrayList<Short> recordAdddress = new ArrayList<Short>();

				do {
					tableFile.seek(firstByteLocation);
					pageType = tableFile.readByte();
					pageNum = tableFile.readByte();
					numofRecords = tableFile.readByte();
					startLocation = tableFile.readShort();
					nextNodeLocation = tableFile.readInt();
					for (int i = 0; i < numofRecords; i++) {
						recordAdddress.add(Short.valueOf(tableFile.readShort()));
					}
					if (nextNodeLocation != -1) {
						firstByteLocation = firstByteLocation + 512;
					}
				} while (nextNodeLocation != -1);

				for (int j = 0; j < recordAdddress.size(); j++) {
					int columnSize = 2;
					tableFile.seek((recordAdddress.get(j)).shortValue());
					int isDeleted = tableFile.readByte(); 
					if (isDeleted == 0) {
						Boolean updateFlag = false;
						String outputString = "  ";
						Short address = 0;
						String value = "";
						for (int i = 0; i < columnsList.size(); i++) {
							String dataType = columnsList.get(i).getDataType();
							switch (dataType) {
							case "tinyint":
								value = "" + tableFile.readByte();
								if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0])
										&& value.equals(whereValueArray[2])) {
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Byte.parseByte(value) < Byte.parseByte(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Byte.parseByte(value) > Byte.parseByte(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Byte.parseByte(value) <= Byte.parseByte(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Byte.parseByte(value) >= Byte.parseByte(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								outputString = outputString + " " + value;
								break;
							case "smallint":
								value = "" + tableFile.readShort();
								if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0])
										&& value.equals(whereValueArray[2])) {
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Short.parseShort(value) < Short.parseShort(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Short.parseShort(value) > Short.parseShort(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Short.parseShort(value) <= Short.parseShort(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Short.parseShort(value) >= Short.parseShort(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								outputString = outputString + " " + value;
								break;
							case "int":
								value = "" + tableFile.readInt();
								if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0])
										&& value.equals(whereValueArray[2])) {
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Integer.parseInt(value) < Integer.parseInt(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Integer.parseInt(value) > Integer.parseInt(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Integer.parseInt(value) <= Integer.parseInt(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Integer.parseInt(value) >= Integer.parseInt(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								outputString = outputString + " " + value;
								break;
							case "bigint":
								value = "" + tableFile.readLong();
								if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0])
										&& value.equals(whereValueArray[2])) {
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Long.parseLong(value) < Long.parseLong(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Long.parseLong(value) > Long.parseLong(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Long.parseLong(value) <= Long.parseLong(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Long.parseLong(value) >= Long.parseLong(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								outputString = outputString + " " + value;
								break;
							case "real":
								value = "" + tableFile.readFloat();
								if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0])
										&& value.equals(whereValueArray[2])) {
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Float.parseFloat(value) < Float.parseFloat(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Float.parseFloat(value) > Float.parseFloat(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Float.parseFloat(value) <= Float.parseFloat(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Float.parseFloat(value) >= Float.parseFloat(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								outputString = outputString + " " + value;
								break;
							case "double":
								value = "" + tableFile.readDouble();
								if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0])
										&& value.equals(whereValueArray[2])) {
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Double.parseDouble(value) < Double.parseDouble(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Double.parseDouble(value) > Double.parseDouble(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Double.parseDouble(value) <= Double.parseDouble(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && Double.parseDouble(value) >= Double.parseDouble(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								outputString = outputString + " " + value;
								break;
							case "datetime":
								value = "" + convertDateTimeToString(tableFile.readLong());
								if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0])
										&& value.equals(whereValueArray[2])) {
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) < convertStringToDate(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) > convertStringToDate(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) <= convertStringToDate(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) >= convertStringToDate(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								outputString = outputString + " " + value;
								break;
							case "date":
								value = "" + convertDateToString(tableFile.readLong());
								if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0])
										&& value.equals(whereValueArray[2])) {
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) < convertStringToDate(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) > convertStringToDate(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals("<=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) <= convertStringToDate(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								if (operator.equals(">=") && columnsList.get(i).getColumnName().equals(whereValueArray[0]) && convertStringToDate(value) >= convertStringToDate(whereValueArray[2])) 
								{
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								outputString = outputString + " " + value;
								break;
							default:
								value = "" + tableFile.readUTF();
								if (operator.equals("=") && columnsList.get(i).getColumnName().equals(whereValueArray[0])
										&& value.equals(whereValueArray[2])) {
									updateFlag = true;
									address = recordAdddress.get(j).shortValue();
								}
								outputString = outputString + "  " + value;
							}

						}
						if (updateFlag) {
							ArrayList<String> dataTypeList = new ArrayList<String>();
							for (int m = 0; m < columnsList.size(); m++) {
								dataTypeList.add(columnsList.get(m).getDataType());
								if (columnsList.get(m).getColumnName().equals(setValueArray[0])) {
									index = m;
								}
							}
							
							tableFile.seek(address);
							tableFile.readByte();
							for (int k = 0; k <= index; k++) {
								dType = dataTypeList.get(k);
								if (k != index) {
									switch (dType) {
									case "tinyint":
										tableFile.readByte();
										columnSize += 1;
										break;
									case "smallint":
										tableFile.readShort();
										columnSize += 2;
										break;
									case "int":
										tableFile.readInt();
										columnSize += 4;
										break;
									case "bigint":
										tableFile.readLong();
										columnSize += 8;
										break;
									case "real":
										tableFile.readFloat();
										columnSize += 4;
										break;
									case "double":
										tableFile.readDouble();
										columnSize += 8;
										break;
									case "datetime":
										convertDateTimeToString(tableFile.readLong());
										columnSize += 8;
										break;
									case "date":
										convertDateToString(tableFile.readLong());
										columnSize += 8;
										break;
									default:
										columnSize += tableFile.readShort();
										tableFile.seek(address+columnSize);
										tableFile.readUTF();
									}
								} else {
									switch (dType) {
									case "tinyint":
										
										tableFile.writeByte(Byte.parseByte(setValueArray[1]));
										break;
									case "smallint":
										tableFile.writeInt(Short.parseShort(setValueArray[1]));
										break;
									case "int":
										tableFile.writeInt(Integer.parseInt(setValueArray[1]));
										break;
									case "bigint":
										tableFile.writeLong(Long.parseLong(setValueArray[1]));
										break;
									case "real":
										tableFile.writeFloat(Float.parseFloat(setValueArray[1]));
										break;
									case "double":
										tableFile.writeDouble(Double.parseDouble(setValueArray[1]));
										break;
									case "datetime":
										tableFile.writeLong(Long.parseLong(setValueArray[1]));
										break;
									case "date":
										tableFile.writeLong(convertStringToDate(setValueArray[1]));
										break;
									default:
										valueSize = tableFile.readShort();
										//System.out.println(columnSize);
										tableFile.seek(address + columnSize -1);
										int diff = valueSize - setValueArray[1].length();
										String newValue = setValueArray[1];
										if(diff>0) {
											String toAdd = "";
											for(int d=0;d<diff;d++) {
												toAdd += " ";
											}
											newValue = setValueArray[1]+toAdd;
										}
										
										tableFile.writeUTF(newValue);
									}

								

							}

							
						}
							System.out.println("Update successful");
					}
				}
				}
				tableFile.close();

			}
			
		} catch (Exception e) {
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