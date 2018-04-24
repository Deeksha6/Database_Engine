package Project2;

import java.io.RandomAccessFile;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Create {
	public static void parseCreateTable(String createString) {
		ArrayList<String> createTableElements = new ArrayList<String>(Arrays.asList(createString.split(" ")));
		String tableName = createTableElements.get(2);
		String tableFileName = createTableElements.get(2) + ".tbl";
		try {
			String currentDir = System.getProperty("user.dir");
			String path = "";
			path = currentDir + File.separator + "data" + File.separator + "userData" + File.separator + tableFileName;
			File file = new File(path);
			if ((file.exists()) && (!file.isDirectory())) {
				System.out.println(tableName + " already exists");
			} else {
				try {
					RandomAccessFile userTableFile = new RandomAccessFile(path, "rw");
					userTableFile.seek(userTableFile.length());
					userTableFile.writeByte(0);//type of page
					userTableFile.writeByte(1);//page number
					userTableFile.writeByte(0);//no. of records
					userTableFile.writeShort(512);//start address
					userTableFile.writeInt(-1);//next node
					userTableFile.close();

					String catalogTablePath = currentDir + File.separator + "data" + File.separator + "catalog"
							+ File.separator + "davisbase_tables.tbl";

					RandomAccessFile catalogTableFile = new RandomAccessFile(catalogTablePath, "rw");
					int numOfTables = 0;
					
					if (catalogTableFile.length() > 0) {
						catalogTableFile.seek(0);
						numOfTables = catalogTableFile.readByte();
						catalogTableFile.seek(catalogTableFile.length());
					}
					if (catalogTableFile.length() == 0) {
						catalogTableFile.seek(catalogTableFile.length());
						catalogTableFile.writeByte(0);
					}
					catalogTableFile.writeByte(0);
					catalogTableFile.writeUTF(tableName); 
					catalogTableFile.seek(0);
					numOfTables++;
					catalogTableFile.writeByte(numOfTables);
					catalogTableFile.close();

					createString = createString.replace('(', '|').replace(',', '|').replace(')', ' ').trim();
					//System.out.println(createString);
					List<String> colList = new ArrayList<String>();
					colList = Arrays.asList(createString.split("\\|"));

					String catalogColumnPath = currentDir + File.separator + "data" + File.separator + "catalog"
							+ File.separator + "davisbase_columns.tbl";
					int numOfColumns = 0;
					RandomAccessFile catalogColumnFile = new RandomAccessFile(catalogColumnPath, "rw");
					if (catalogColumnFile.length() > 0) {
						catalogColumnFile.seek(0);
						numOfColumns = catalogColumnFile.readByte();
						catalogColumnFile.seek(catalogColumnFile.length());
					}
					if (catalogColumnFile.length() == 0) {
						catalogColumnFile.seek(catalogColumnFile.length());
						catalogColumnFile.writeByte(0);
					} 
					int rowId = numOfColumns;
					for (int i = 0; i < colList.size(); i++) {
						String prKey="No";
						String notNullValue = "No";
						String col1 = colList.get(i);
						col1 = col1.trim();
						if (!col1.contains("create") && !col1.contains("table") ) 
						{
							if ((!col1.isEmpty()) && (col1 != null)) {
								if (col1.contains("primary key")) {
								
									prKey="Pri";
									notNullValue="Yes";
									
								}
								else if (col1.contains("not null")) {
									
									notNullValue="Yes";
								}
								catalogColumnFile.seek(catalogColumnFile.length());
								catalogColumnFile.writeByte(0); 
								ArrayList<String> eachCol = new ArrayList<String>(Arrays.asList(col1.split(" ")));
								rowId = rowId + 1;
								String columnName= eachCol.get(0);
								String dataTypeValue= eachCol.get(1);
								catalogColumnFile.writeByte(rowId);
								catalogColumnFile.writeUTF(tableName);
								catalogColumnFile.writeUTF(columnName);
								catalogColumnFile.writeUTF(dataTypeValue);
								catalogColumnFile.writeUTF(prKey);
								catalogColumnFile.writeUTF(notNullValue);
								catalogColumnFile.seek(0);
								catalogColumnFile.writeByte(rowId);
							}
						}
					}
					catalogColumnFile.close();
					System.out.println("Table created successfully.");
				} catch (Exception e) {
					e.printStackTrace();
					System.out.println(e);
				}
			}
		} catch (Exception e) {
			System.out.println("Error :"+e);
		}
	}
}
