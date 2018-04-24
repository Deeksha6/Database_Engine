package Project2;

import java.io.File;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Delete {
	public static void parseDeleteString(String deleteQuery) {
		String[] deleteElements = deleteQuery.split(" ");
		String tableName = deleteElements[3].trim();
		try {
			if (CheckTable.checkTable(tableName)) {
				if (deleteElements.length == 4) {
					deleteAll(tableName);
					System.out.println("Deleted Successfully");
				} else {
					String whereValue = deleteQuery.substring(deleteQuery.indexOf("where") + 5, deleteQuery.length())
							.trim();
					String[] whereValueArray = whereValue.replaceAll("\'", "").replaceAll("\"", "").split(" ");
					String operator = whereValueArray[1];
					String currentDir = System.getProperty("user.dir");
					String path = currentDir + File.separator + "data" + File.separator + "userData" + File.separator
							+ tableName + ".tbl";
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
							Boolean deleteFlag = false;
							String outputString = "  ";
							Short address = 0;
							String value = "";
							for (int i = 0; i < columnsList.size(); i++) {
								String dataType = columnsList.get(i).getDataType();
								switch (dataType) {
								case "tinyint":
									value = "" + tableFile.readByte();
									if (operator.equals("=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& value.equals(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Byte.parseByte(value) < Byte.parseByte(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Byte.parseByte(value) > Byte.parseByte(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Byte.parseByte(value) <= Byte.parseByte(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Byte.parseByte(value) >= Byte.parseByte(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									break;
								case "smallint":
									value = "" + tableFile.readShort();
									if (operator.equals("=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& value.equals(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Short.parseShort(value) < Short.parseShort(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Short.parseShort(value) > Short.parseShort(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Short.parseShort(value) <= Short.parseShort(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Short.parseShort(value) >= Short.parseShort(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									break;
								case "int":
									value = "" + tableFile.readInt();
									if (operator.equals("=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& value.equals(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Integer.parseInt(value) < Integer.parseInt(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Integer.parseInt(value) > Integer.parseInt(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Integer.parseInt(value) <= Integer.parseInt(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Integer.parseInt(value) >= Integer.parseInt(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									break;
								case "bigint":
									value = "" + tableFile.readLong();
									if (operator.equals("=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& value.equals(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Long.parseLong(value) < Long.parseLong(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Long.parseLong(value) > Long.parseLong(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Long.parseLong(value) <= Long.parseLong(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Long.parseLong(value) >= Long.parseLong(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									break;
								case "real":
									value = "" + tableFile.readFloat();
									if (operator.equals("=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& value.equals(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Float.parseFloat(value) < Float.parseFloat(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Float.parseFloat(value) > Float.parseFloat(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Float.parseFloat(value) <= Float.parseFloat(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Float.parseFloat(value) >= Float.parseFloat(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									break;
								case "double":
									value = "" + tableFile.readDouble();
									if (operator.equals("=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& value.equals(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Double.parseDouble(value) < Double.parseDouble(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Double.parseDouble(value) > Double.parseDouble(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Double.parseDouble(value) <= Double.parseDouble(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& Double.parseDouble(value) >= Double.parseDouble(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									break;
								case "datetime":
									value = "" + convertDateTimeToString(tableFile.readLong());
									if (operator.equals("=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& value.equals(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& convertStringToDate(value) < convertStringToDate(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& convertStringToDate(value) > convertStringToDate(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& convertStringToDate(value) <= convertStringToDate(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& convertStringToDate(value) >= convertStringToDate(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									outputString = outputString + " " + value;
									break;
								case "date":
									value = "" + convertDateToString(tableFile.readLong());
									if (operator.equals("=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& value.equals(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& convertStringToDate(value) < convertStringToDate(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& convertStringToDate(value) > convertStringToDate(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals("<=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& convertStringToDate(value) <= convertStringToDate(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									if (operator.equals(">=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& convertStringToDate(value) >= convertStringToDate(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
									break;
								default:
									value = "" + tableFile.readUTF();
									if (operator.equals("=")
											&& columnsList.get(i).getColumnName().equals(whereValueArray[0])
											&& value.equals(whereValueArray[2])) {
										deleteFlag = true;
										address = recordAdddress.get(j).shortValue();
									}
								}
							}
							if(deleteFlag) {
								tableFile.seek(address);
								tableFile.writeByte(1);
							}
						}
					
					}
					tableFile.close();
					System.out.println("Deleted successfully");
				}
				
			} else {
				System.out.println("Table does not exist");
			}
			
		} catch (Exception e) {
			System.out.println("Error : " + e);
		}

	}

	public static void deleteAll(String tableName) {
		String currentDir = System.getProperty("user.dir");
		String path = currentDir + File.separator + "data" + File.separator + "userData" + File.separator
				+ tableName + ".tbl";
		try { 
			RandomAccessFile tableFile = new RandomAccessFile(path, "rw");
	
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
			tableFile.seek((recordAdddress.get(j)).shortValue());
			tableFile.writeByte(1);
		}	
		tableFile.close();
		}
		catch(Exception e) {
			System.out.println("Error : "+e);
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
