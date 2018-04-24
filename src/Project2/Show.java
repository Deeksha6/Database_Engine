package Project2;

import java.io.RandomAccessFile;
import java.io.File;

public class Show {

	public static void showTables() {
		try {
			String currentDir = System.getProperty("user.dir"); 
			String path = "";
			path = currentDir + File.separator + "data" + File.separator + "catalog" + File.separator
					+ "davisbase_tables.tbl";
			File file = new File(path);
			int count = 0;
			if ((file.exists()) && (!file.isDirectory())) {
				RandomAccessFile tableFile = new RandomAccessFile(path, "rw");
				int tableCount = 0;
				if (tableFile.length() > 0)//if not the first time
				{
					tableCount = tableFile.readByte();
				}
				if (tableFile.length() == 0) //if it is the first table created
				{
					System.out.println("No tables in the database.");
				} 
				else {
					int isTableDeletedFlag = 0;
					int i=0;
					tableFile.seek(0);
					tableFile.readByte();
					while(i<tableCount) {
						isTableDeletedFlag = tableFile.readByte();
						String tableName = tableFile.readUTF();
						if (isTableDeletedFlag != 1)
						{
							count+=1;
							if(count==1) {
								System.out.println("TABLE NAME");
							}
							System.out.println(tableName);
						}
						i++;
					}
				}
				if(count==0) {
					System.out.println("No tables present in the database");
				}
				tableFile.close();

			} else {
				System.out.println("No tables in the database.");

			}
		} catch (Exception e) {
			System.out.println("Error : "+e);

		}

	}
}