package Project2;


import java.io.File;


public class CheckTable{
	public static boolean checkTable(String tableName)
	{
		try {
			String currentDir = System.getProperty("user.dir");
			String path = currentDir + File.separator + "data" + File.separator + "userData" + File.separator + tableName+ ".tbl";
			File file = new File(path);
			if ((file.exists()) && (!file.isDirectory()))
			{
				return true;
			}
				else
			{
				System.out.println("ERROR : "+ tableName + " does not exist in the database.");
				return false;
			}
		} catch (Exception e) {
			System.out.println("Error : "+e);
			return false;
		}

		
	}
}