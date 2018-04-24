package Project2;

import Project2.Show;
import Project2.Update;
import Project2.Create;
import java.util.Scanner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import static java.lang.System.out;
import Project2.Insert;
import Project2.DropTable;
import Project2.Delete;

public class Header {

	static String prompt = "davisql> ";
	static String version = "v1.0b(example)";
	static String copyright = "©2018 Deeksha";
	static boolean isExit = false;

	static long pageSize = 512;

	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	public static void main(String[] args) {

		splashScreen();

		String userCommand = "";

		while (!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");

	}

	/**
	 * ***********************************************************************
	 * Static method definitions
	 */

	/**
	 * Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}

	/**
	 * @param s
	 *            The String to be repeated
	 * @param num
	 *            The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num
	 *         times.
	 */
	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}

	public static void printCmd(String s) {
		System.out.println("\n\t" + s + "\n");
	}

	public static void printDef(String s) {
		System.out.println("\t\t" + s);
	}

	/**
	 * Help: Display supported commands
	 */
	public static void help() {
		out.println(line("*", 80));
		out.println("SUPPORTED COMMANDS\n");
		out.println("All commands below are case insensitive\n");
		out.println("SHOW TABLES;");
		out.println("\tDisplay the names of all tables.\n");
		// printCmd("SELECT * FROM <table_name>;");
		// printDef("Display all records in the table <table_name>.");
		out.println("SELECT <column_list> FROM <table_name> [WHERE <condition>];");
		out.println("\tDisplay table records whose optional <condition>");
		out.println("\tis <column_name> = <value>.\n");
		out.println("DROP TABLE <table_name>;");
		out.println("\tRemove table data (i.e. all records) and its schema.\n");
		out.println("UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];");
		out.println("\tModify records data whose optional <condition> is\n");
		out.println("VERSION;");
		out.println("\tDisplay the program version.\n");
		out.println("HELP;");
		out.println("\tDisplay this help information.\n");
		out.println("EXIT;");
		out.println("\tExit the program.\n");
		out.println(line("*", 80));
	}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}

	public static String getCopyright() {
		return copyright;
	}

	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}

	public static void parseUserCommand(String userCommand) {

		/*
		 * commandTokens is an array of Strings that contains one token per array
		 * element The first token can be used to determine the type of command The
		 * other tokens can be used to pass relevant parameters to each command-specific
		 * method inside each case statement
		 */
		// String[] commandTokens = userCommand.split(" ");
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		/*
		 * This switch handles a very small list of hardcoded commands of known syntax.
		 * You will want to rewrite this method to interpret more complex commands.
		 */
		switch (commandTokens.get(0)) {
		case "show":
			Show.showTables();
			break;
		case "select":
			// System.out.println("CASE: SELECT");
			if (userCommand.contains("where")) {
				int max = 0;
				String toAdd1 = "";
				String[] queryElements = userCommand.split(" ");
				String tableName = queryElements[3].trim();
				List<ColumnDetails> columnsList = GetColumns.fetchColumnDetails(tableName);
				
				String toDisplay = userCommand.substring(userCommand.indexOf("select") + 6, userCommand.indexOf("from")).trim();
				String[] toDisplayArray = toDisplay.split(",");
				ArrayList<String> toDisplayList = new ArrayList<>();
				List<String> resultColumns = new ArrayList<String>();
				List<String> finalResultColumns = new ArrayList<String>();
				resultColumns = SelectWhere.selectWhere(userCommand);
				String newValue = " ";
				String toAdd = "";
				if(toDisplayArray.length==1 && toDisplayArray[0].trim().equals("*")) {
					for (int i=0; i<columnsList.size();i++) {
						toDisplayList.add(columnsList.get(i).getColumnName().trim());
					}
				
				}
				else {
				toDisplayList = new ArrayList<>(Arrays.asList(toDisplayArray));
				}
				if(resultColumns.size()>0) {
					for(String s : toDisplayList) {
						toAdd1 = "";
						for (int d = 0; d < 23; d++) {
							toAdd1 += " ";
						}
						System.out.print(s.toUpperCase() + toAdd1);
					}
					System.out.println();
				}else {
					System.out.println("No Records in the table for the given condition");
				}
				for (int r = 0; r < resultColumns.size(); r++) {
					String[] eachColumn = resultColumns.get(r).split(" ");
					for (int a = 0; a < eachColumn.length; a++) {
						if (max < eachColumn[a].length()) {
							max = eachColumn[a].length();
						}
					}
				}
				for (int r = 0; r < resultColumns.size(); r++) {
					String newEachColumn = new String();
					String[] eachColumn = resultColumns.get(r).replaceAll("\\s+", " ").trim().split(" ");
					for (int a = 0; a < eachColumn.length; a++) {
						toAdd = "";
						for (int d = 0; d < 20; d++) {
							toAdd += " ";
						}
						newValue = eachColumn[a] + toAdd;
						newEachColumn += newValue + "|";

					}
					finalResultColumns.add(newEachColumn);
				}
				for (int i = 0; i < finalResultColumns.size(); i++) {
					System.out.println(finalResultColumns.get(i));
				}
			}
			else {
				SelectAll.selectAll(userCommand);
			}
			break;
		case "update":
			// System.out.println("CASE: UPDATE");
			Update.parseUpdateString(userCommand);
			break;
		case "drop":
			// System.out.println("CASE: DROP");
			DropTable.dropTable(userCommand);
			break;
		case "create":
			// System.out.println("CASE: CREATE");
			checkDataType(userCommand);
			break;
		case "insert":
			// System.out.println("CASE: INSERT");
			Insert.parseInsertString(userCommand);
			break;
		case "delete":
			Delete.parseDeleteString(userCommand);
			break;
		case "help":
			help();
			break;
		case "version":
			displayVersion();
			break;
		case "exit":
			isExit = true;
			break;
		case "quit":
			isExit = true;
		default:
			System.out.println("I didn't understand the command: \"" + userCommand + "\"");
			break;
		}
	}

	public static void checkDataType(String createTableString) {
		String columnsList = createTableString
				.substring(createTableString.indexOf("(") + 1, createTableString.indexOf(")")).trim();
		String[] columnsArray = columnsList.trim().split(",");
		String[] dataTypes = { "tinyint", "smallint", "int", "bigint", "real", "double", "datetime", "date", "text" };
		Boolean errorFlag = false;
		for (int i = 0; i < columnsArray.length; i++) {
			String[] eachColumn = columnsArray[i].trim().split("\\s+");
			if (!Arrays.asList(dataTypes).contains(eachColumn[1])) {
				errorFlag = true;
				break;
			}

		}

		if (errorFlag) {
			System.out.println("ERROR: Invalid datatype in the query.");
		} else {
			Create.parseCreateTable(createTableString);
		}
	}

}