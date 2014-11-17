package me.ytg.findinsqlite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

	public static final String USAGE_ERROR_MESSAGE = "Wrong number of arguments\n" +
			"Usage: java -jar FindInSqLite.jar [string_to_search_for] [database_name]";

	public static void main(String[] args) {
		String searchFor;
		String db;

		if (args.length == 2) {
			searchFor = args[0];
			db = args[1];

			Connection connection = null;
			try {
				// create a database connection
				Class.forName("org.sqlite.JDBC");
				connection = DriverManager.getConnection("jdbc:sqlite:" + db);
				for (String tableName : getTableList(connection)) {
					// System.out.println("checking [" + tableName + ']');
					for (String columnName : getColumnList(connection, tableName)) {
						if (checkColumn(connection, tableName, columnName, searchFor)) {
							System.out.print("checking [" + tableName + '.' + columnName + "]: FOUND");
						}
					}
				}
			} catch (Exception e) {
				System.err.println("Exception while reading data from the database: [" + e.getClass() + "] "
						+ e.getMessage() + ' ' + Arrays.toString(e.getStackTrace()));
			} finally {
				try {
					if (connection != null) {
						connection.close();
					}
				} catch (SQLException e) {
					// connection close failed.
					System.err.println("Exception while closing connection: [" + e.getClass() + "] " + e.getMessage()
							+ ' ' + Arrays.toString(e.getStackTrace()));
				}
			}
			System.out.println("Database check complete.");
		} else {
			System.err.println(USAGE_ERROR_MESSAGE);
		}
	}

	/**
	 * Check if the column contains the given value.
	 * 
	 * @param connection SQLite connection to query the records
	 * @param tableName the table to check
	 * @param columnNanem the column to check
	 * @param searchFor the string value to search for
	 * @return true if the column contains the value, false if not
	 * @throws Exception
	 */
	private static boolean checkColumn(Connection connection, String tableName, String columnNanem, String searchFor)
			throws Exception {
		Statement statement = connection.createStatement();
		ResultSet values = null;
		try {
			String sqlQuery = "SELECT COUNT(" + columnNanem + ") FROM " + tableName + " WHERE " + columnNanem + " = '" + searchFor + '\'';
			values = statement.executeQuery(sqlQuery);
			if (values.next()) {
				int count = values.getInt(1);
				return count > 0;
			}
		} finally {
			if (values != null) {
				values.close();
			}
		}
		throw new Exception("SQL error");
	}

	/**
	 * Get the list of the columns in the table
	 * 
	 * @param connection SQLite connection to query the columns
	 * @param tableName the table to check
	 * @return the list of column names in the table
	 * @throws SQLException
	 */
	private static Iterable<String> getColumnList(Connection connection, String tableName) throws SQLException {
		List<String> result = new ArrayList<>();
		Statement statement = connection.createStatement();
		ResultSet table = null;
		try {
			String sqlQuery = "SELECT sql FROM sqlite_master WHERE tbl_name = '" + tableName + "' AND type = 'table'";
			table = statement.executeQuery(sqlQuery);
			if (table.next()) {
				Pattern columnPattern = Pattern.compile("\\((.*)\\)");
				String createSql = table.getString(1);
				Matcher lMatcher = columnPattern.matcher(createSql);
				if (lMatcher.find()) {
					String columnDeclarationList = lMatcher.group(0);
					columnDeclarationList = columnDeclarationList.substring(1, columnDeclarationList.length() - 1);
					String[] columnDeclarations = columnDeclarationList.split(",");
					result = new ArrayList<>(columnDeclarations.length);
					for (String columnDeclaration : columnDeclarations) {
						String columnName = columnDeclaration.trim().split(" ")[0];
						result.add(columnName);
					}
				}
			}
			return result;
		} finally {
			if (table != null) {
				table.close();
			}
		}
	}

	/**
	 * Gets the list of tables in the SQLite database
	 * 
	 * @return list of tables
	 * @throws SQLException
	 */
	private static List<String> getTableList(Connection connection)
			throws SQLException {
		List<String> result = new ArrayList<>();
		Statement statement = connection.createStatement();
		ResultSet tables = null;
		try {
			tables = statement.executeQuery("SELECT tbl_name FROM sqlite_master WHERE type='table'");
			while (tables.next()) {
				result.add(tables.getString(1));
			}
			return result;
		} finally {
			if (tables != null) {
				tables.close();
			}
		}
	}

}
