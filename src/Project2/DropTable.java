package Project2;

import java.io.RandomAccessFile;
import java.io.File;
import Project2.CheckTable;

public class DropTable {

	public static void dropTable(String dropTableString) {
		try {
			String[] tokens = dropTableString.split(" ");
			String tableName = tokens[2].trim();
			if (CheckTable.checkTable(tableName))
			{
				String currentDir = System.getProperty("user.dir");
				String filePath = currentDir + File.separator + "data" + File.separator + "userData"
						+ File.separator + tableName + ".tbl";
				File fileDel = new File(filePath);

				if (fileDel.delete())
				{
					String path = currentDir + File.separator + "data" + File.separator + "catalog"
							+ File.separator + "davisbase_tables.tbl";
					File file = new File(path);
					if ((file.exists()) && (!file.isDirectory())) {
						RandomAccessFile catalogTableFile = new RandomAccessFile(path, "rw");
						int tableCount = 0;
						if (catalogTableFile.length() > 0) 
						{
							tableCount = catalogTableFile.readByte();
						}
						if (catalogTableFile.length() == 0) 
						{
							System.out.println("No tables currently present in the database.");
						} else {
							long recordStartLocation = 0L;
							int isTableDeletedFlag = 0;
							boolean flag = false;
							catalogTableFile.seek(0);
							tableCount = catalogTableFile.readByte();
							for (int i = 0; i < tableCount; i++) {
								recordStartLocation = catalogTableFile.getFilePointer();
								isTableDeletedFlag = catalogTableFile.readByte();
								String currentTable = catalogTableFile.readUTF();
								if (tableName.equals(currentTable)) {
									if (isTableDeletedFlag == 0) 
									{
										catalogTableFile.seek(recordStartLocation);
										catalogTableFile.writeByte(1);
										flag = true;
										deleteColumns(tableName); 
										break;
									}
								}

							}
							if (flag) {
								System.out.println(tableName + " deleted successfully.");
							} else {
								System.out.println("Error while deleting the table.");
							}
						}

						catalogTableFile.close();

					} else {
						System.out.println("No tables currently present in the database.");

					}

				} else 
				{
					System.out.println("Error : Error while deleting the table");
				}

			}
		} catch (Exception e) {
			System.out.println("Error : "+e);

		}

	}
	
	public static void deleteColumns (String tableName)
	{
		try
		{
			int isColumnDeleted=0;
			Byte rowNum = 0;
			String currentDir = System.getProperty("user.dir"); // gets current working directory
			String path = currentDir + File.separator + "data" + File.separator + "catalog" + File.separator + "davisbase_columns.tbl";		
			File file = new File(path);
			if ((file.exists()) && (!file.isDirectory()))
			{
				RandomAccessFile catalogColumnFile = new RandomAccessFile(path, "rw");
				long fileLength = catalogColumnFile.length();
				long recordStartLocation =0;
				catalogColumnFile.seek(0);
				int numOfColumns = catalogColumnFile.readByte();
					while (catalogColumnFile.getFilePointer() < fileLength) 
					{	
						recordStartLocation = catalogColumnFile.getFilePointer();
						isColumnDeleted = catalogColumnFile.readByte();
						rowNum = catalogColumnFile.readByte();
						String columnTable = catalogColumnFile.readUTF();
						
						if ((columnTable.equals(tableName)))
						{
							if(isColumnDeleted == 0)  // check for flag after reading the record for the current table to get the pointer in position for the next table
							{
								catalogColumnFile.seek(recordStartLocation);
								catalogColumnFile.writeByte(1);	
							}else {
								catalogColumnFile.seek(recordStartLocation);
								catalogColumnFile.readByte();
							}
						}
						else {
							catalogColumnFile.seek(recordStartLocation);
							catalogColumnFile.readByte();
						}
						catalogColumnFile.readByte();//ROWNUM
						catalogColumnFile.readUTF();//table name
						catalogColumnFile.readUTF();//column name
						catalogColumnFile.readUTF();//data type
						catalogColumnFile.readUTF();//primary key
						catalogColumnFile.readUTF();//not null
					}
					catalogColumnFile.close();
			}
			else
			{
				System.out.println("No tables currently present in the database.");
			}
		}
		catch (Exception e)
		{
			System.out.println("Error : "+e);
		}
	}
	
	
}