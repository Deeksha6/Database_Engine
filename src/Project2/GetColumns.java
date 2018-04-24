package Project2;

import java.io.RandomAccessFile;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class GetColumns{
	public static List<ColumnDetails> fetchColumnDetails(String tableName) {
		List<ColumnDetails> ColumnDetailsList = new ArrayList<ColumnDetails>();
		try {
			int deleteFlag =0;
			int rowId =0;
			String currentDir = System.getProperty("user.dir");
			String path = currentDir + File.separator + "data" + File.separator + "catalog" + File.separator +  "davisbase_columns.tbl";
			RandomAccessFile catalogColumnDetailsFile = new RandomAccessFile(path, "rw");
			catalogColumnDetailsFile.seek(1);
			long fileLength = catalogColumnDetailsFile.length();
				while (catalogColumnDetailsFile.getFilePointer() < fileLength) 
				{
					deleteFlag = catalogColumnDetailsFile.readByte();
					rowId = catalogColumnDetailsFile.readByte();
					String tabName = catalogColumnDetailsFile.readUTF();
					String colName = catalogColumnDetailsFile.readUTF();
					String dataType = catalogColumnDetailsFile.readUTF();
					String priKey = catalogColumnDetailsFile.readUTF();
					String notNullValue = catalogColumnDetailsFile.readUTF();
				
					if ((tabName.equals(tableName)))
					{
						ColumnDetails col = new ColumnDetails();
						col.setColumnName(colName);
						col.setDataType(dataType);
						col.setPrimaryKey(priKey);
						col.setNotNullable(notNullValue);
						if(deleteFlag == 0)
						{
							ColumnDetailsList.add(col);
							
						}
					}
				}
				catalogColumnDetailsFile.close();
			
		} catch (Exception e) {
			System.out.println(e);
		}

		return ColumnDetailsList;
	}
}