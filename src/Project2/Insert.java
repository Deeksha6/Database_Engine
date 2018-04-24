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
import Project2.SelectWhere;

public class Insert {
	public static void parseInsertString(String insertQuery) {
		try {
			boolean errorFlag = false;
			int recordLen = 1;
			ArrayList<String> insertElements = new ArrayList<String>(Arrays.asList(insertQuery.split(" ")));
			String columnValues = insertQuery.substring(insertQuery.indexOf("(") + 1, insertQuery.length() - 1);
			String[] values = columnValues.replaceAll("\"", "").replaceAll("\'", "").trim().replaceAll("\\s+", "")
					.split(",");
			String tableName = insertElements.get(2);
			String errorMsg = "";
			if (CheckTable.checkTable(tableName)) {
				List<ColumnDetails> columnList = new java.util.ArrayList<ColumnDetails>();
				columnList = GetColumns.fetchColumnDetails(tableName);
				if (values.length == columnList.size()) {
					for (int i = 0; i < values.length; i++) {
						if((columnList.get(i).getPrimaryKey().equals("Pri")) || (columnList.get(i).getNotNullable().equals("Yes")))
						{
							
							if(values[i] == null || values[i] == "null" || values[i].equals("null"))
							{
								errorFlag = true;
								errorMsg = "ERROR: "+columnList.get(i).getColumnName()+ " cannot be null.";
							}
						if (columnList.get(i).getPrimaryKey().equals("Pri")) {
							String checkPri = "select * from " + tableName + " where "
									+ columnList.get(i).getColumnName() + " = " + values[i];
							List<String> result = new ArrayList<String>();
							result = SelectWhere.selectWhere(checkPri);
							if (result.size() > 0) {
								errorFlag = true;
							}

						}
						}

						if (!errorFlag) {
							if (columnList.get(i).getDataType().equals("tinyint")) {
								recordLen++;
							} else if (columnList.get(i).getDataType().equals("smallint")) {
								recordLen += 2;
							} else if (columnList.get(i).getDataType().equals("int")) {
								recordLen += 4;
							} else if (columnList.get(i).getDataType().equals("bigint")) {
								recordLen += 8;
							} else if (columnList.get(i).getDataType().equals("real")) {
								recordLen += 4;
							} else if (columnList.get(i).getDataType().equals("double")) {
								recordLen += 8;
							} else if (columnList.get(i).getDataType().equals("datetime")) {
								recordLen += 8;
							} else if (columnList.get(i).getDataType().equals("date")) {
								recordLen += 8;
							} else {
								recordLen += values[i].length() + 1;
							}
						}

					}

					if (!errorFlag) {
						String workingDirectory = System.getProperty("user.dir");
						String absoluteFilePath = workingDirectory + File.separator + "data" + File.separator
								+ "userData" + File.separator + tableName + ".tbl";
						RandomAccessFile tableFile = new RandomAccessFile(absoluteFilePath, "rw");

						int firstByteLocation = 0;
						int numofRecords = 0;
						int startLocation = 0;
						int nextNodeLocation = 0;
						int lastPointerLocation = 0;
						int difference = 0;
						int pageType = 0;
						int pageNum = 0;
						int rowNum = 0;
						int rootPageFirstByte = 0;
						int rootPagenumofRecords = 0;
						int rootPageStartLocation = 512;
						int rootpageNextNodeLocation = 0;
						int rootPageLastPointerLocation = 0;
						int rootPageDifference = 0;
						int rootPagerecordLen = 5;
						int rootPageNumber = 0;

						while (recordLen > difference) {
							tableFile.seek(firstByteLocation);
							pageType = tableFile.readByte();
							pageNum = tableFile.readByte();
							numofRecords = tableFile.readByte();
							startLocation = tableFile.readShort();
							nextNodeLocation = tableFile.readInt();

							while (nextNodeLocation != -1) {
								firstByteLocation = firstByteLocation + 512;
								tableFile.seek(firstByteLocation);
								pageType = tableFile.readByte();
								pageNum = tableFile.readByte();
								numofRecords = tableFile.readByte();
								startLocation = tableFile.readShort();
								nextNodeLocation = tableFile.readInt();

							}
							lastPointerLocation = (((int) tableFile.getFilePointer()) + (numofRecords * 2));
							difference = startLocation - lastPointerLocation;
							if (recordLen > difference) {
								rowNum = numofRecords;
								tableFile.seek(0);
								tableFile.readByte();
								tableFile.readByte();
								tableFile.readByte();
								tableFile.readShort();
								pageNum = pageNum + 1;
								tableFile.writeInt(pageNum);
								nextNodeLocation = tableFile.readInt();
								tableFile.seek(firstByteLocation);
								tableFile.readByte();// page type
								tableFile.readByte();// page number
								tableFile.readByte();// no. of records
								tableFile.readShort();// start address
								firstByteLocation = firstByteLocation + 512;
								tableFile.setLength(tableFile.length() * pageNum * 1L);
								tableFile.seek(firstByteLocation);
								tableFile.writeByte(0);
								tableFile.writeByte(pageNum);
								tableFile.writeByte(0);
								tableFile.writeShort(512 * pageNum);
								tableFile.writeInt(-1);
								tableFile.seek(firstByteLocation);
								pageType = tableFile.readByte();
								pageNum = tableFile.readByte();
								numofRecords = tableFile.readByte();
								startLocation = tableFile.readShort();
								nextNodeLocation = tableFile.readInt();
								lastPointerLocation = (((int) tableFile.getFilePointer()) + (numofRecords * 2));
								difference = startLocation - lastPointerLocation;

								// root page creation
								if (pageNum > 1) {
									if ((pageNum - 1) == 1 || rootPagerecordLen > rootPageDifference) {
										rootPageFirstByte = firstByteLocation + 512;
										tableFile.setLength(tableFile.length() * (pageNum + 1) * 1L);
										rootpageNextNodeLocation = pageNum;
										rootPageNumber = pageNum + 1;

										tableFile.seek(rootPageFirstByte);
										tableFile.writeByte(1);
										tableFile.writeByte(rootPageNumber);
										tableFile.writeByte(0);
										tableFile.writeShort(512 * (pageNum + 1));
										rootPageStartLocation = 512 * (pageNum + 1);
										tableFile.writeInt(rootpageNextNodeLocation);
										rootPageLastPointerLocation = (((int) tableFile.getFilePointer())
												+ (rootPagenumofRecords * 2));
										rootPageDifference = rootPageStartLocation - rootPageLastPointerLocation;
										int rootPageInsertLocation = rootPageStartLocation - rootPagerecordLen - 1;
										tableFile.seek(rootPageInsertLocation);
										tableFile.writeByte(rowNum);
										tableFile.writeInt(pageNum - 1);
										rootPagenumofRecords++;
										tableFile.seek(rootPageFirstByte + 2);
										tableFile.writeByte(rootPagenumofRecords);
										tableFile.writeShort(rootPageInsertLocation);
										tableFile.writeInt(pageNum);
										tableFile.seek(rootPageLastPointerLocation);
										tableFile.writeShort(rootPageInsertLocation);

									} else {
										rootpageNextNodeLocation = pageNum;
										rootPagenumofRecords++;
										tableFile.seek(rootPageFirstByte + 2);
										tableFile.writeByte(rootPagenumofRecords);
										rootPageStartLocation = tableFile.readShort();
										tableFile.writeInt(rootpageNextNodeLocation);
										rootPageLastPointerLocation = (((int) tableFile.getFilePointer())
												+ (rootPagenumofRecords * 2));
										int rootPageInsertLocation = rootPageStartLocation - rootPagerecordLen - 1;
										tableFile.seek(rootPageInsertLocation);
										tableFile.writeByte(rowNum);
										tableFile.writeInt(pageNum);
										rootPagenumofRecords++;
										tableFile.seek(rootPageFirstByte + 2);
										tableFile.writeByte(rootPagenumofRecords);
										tableFile.seek(rootPageLastPointerLocation);
										tableFile.writeShort(rootPageInsertLocation);

									}
								}
							}
						}

						int insertLocation = startLocation - recordLen - 1;
						tableFile.seek(insertLocation);
						tableFile.writeByte(0);

						for (int i = 0; i < values.length; i++) {
							if (columnList.get(i).getDataType().equals("tinyint")) {
								tableFile.writeByte(Byte.parseByte(values[i]));
							} else if (columnList.get(i).getDataType().equals("smallint")) {
								tableFile.writeInt(Short.parseShort(values[i]));
							} else if (columnList.get(i).getDataType().equals("int")) {
								tableFile.writeInt(Integer.parseInt(values[i]));
							} else if (columnList.get(i).getDataType().equals("bigint")) {
								tableFile.writeLong(Long.parseLong(values[i]));
							} else if (columnList.get(i).getDataType().equals("real")) {
								tableFile.writeFloat(Float.parseFloat(values[i]));
							} else if (columnList.get(i).getDataType().equals("double")) {
								tableFile.writeDouble(Double.parseDouble(values[i]));
							} else if (columnList.get(i).getDataType().equals("datetime")) {
								tableFile.writeLong(Long.parseLong(values[i]));
							} else if (columnList.get(i).getDataType().equals("date")) {
								tableFile.writeLong(convertStringToDate(values[i]));
							} else {
								tableFile.writeUTF(values[i]);
							}
						}
						numofRecords++;
						tableFile.seek(firstByteLocation + 2);
						tableFile.writeByte(numofRecords);
						tableFile.writeShort(insertLocation);
						tableFile.seek(lastPointerLocation);
						tableFile.writeShort(insertLocation);
						tableFile.close();
						System.out.println("Insertion successful.");

					} else {
						System.out.println("This Primary Key value already exists");
					}

				} else {
					System.out.println("ERROR: Unequal number of attributes.");
				}
			}
		} catch (Exception e) {
			System.out.println("ERROR : " + e);
		}

	}

	public static long convertStringToDate(String dateString) {
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