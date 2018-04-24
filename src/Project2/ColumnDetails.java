package Project2;

public class ColumnDetails {
	
	private String columnName;
	private String dataType;
	private String primaryKey;
	private String NotNullable;
	
	
	public ColumnDetails() {
		super();
	}


	public ColumnDetails(String columnName, String dataType, String primaryKey, String NotNullable) {
		super();
		this.columnName = columnName;
		this.dataType = dataType;
		this.primaryKey = primaryKey;
		this.NotNullable = NotNullable;
	}


	public String getColumnName() {
		return columnName;
	}


	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}


	public String getDataType() {
		return dataType;
	}


	public void setDataType(String dataType) {
		this.dataType = dataType;
	}


	public String getPrimaryKey() {
		return primaryKey;
	}


	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}


	public String getNotNullable() {
		return NotNullable;
	}


	public void setNotNullable(String NotNullable) {
		this.NotNullable = NotNullable;
	}


		

}
