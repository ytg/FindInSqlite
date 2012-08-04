package me.ytg.findinsqlite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

	/**
	 * Entry point of the application.
	 * 
	 * @param args
	 *            command line arguments
	 */
	public static void main(String[] aArgs) {
		String lSearchFor;
		String lDb;

		if (aArgs.length == 2) {
			lSearchFor = aArgs[0];
			lDb = aArgs[1];

			Connection connection = null;
			try {
				// create a database connection
				Class.forName("org.sqlite.JDBC");
				connection = DriverManager.getConnection("jdbc:sqlite:" + lDb);
				for (String lTable : getTableList(connection)) {
					// System.out.println("checking [" + lTable + ']');
					for (String lColumn : getColumnList(connection, lTable)) {
						if (checkColumn(connection, lTable, lColumn, lSearchFor)) {
							System.out.print("checking [");
							System.out.print(lTable);
							System.out.print('.');
							System.out.print(lColumn);
							System.out.println("]: FOUND");
						}
					}
				}
			} catch (SQLException e) {
				System.err.println(e.getMessage());
			} catch (Exception e) {
				System.err.println(e.getMessage());
			} finally {
				try {
					if (connection != null)
						connection.close();
				} catch (SQLException e) {
					// connection close failed.
					System.err.println(e);
				}
			}
			System.out.println("Database check complete.");
		} else
			System.err
					.println("Wrong number of arguments\nUsage: java -jar FindInSqLite.jar [string_to_search_for] [database_name]");
	}

	/**
	 * Check if the column contains the given value.
	 * 
	 * @param aConnection
	 *            SQLite connection to query the records
	 * @param aTable
	 *            the table to check
	 * @param aColumn
	 *            the column to check
	 * @param aSearchFor
	 *            the string value to search for
	 * @return true if the column contains the value, false if not
	 * @throws Exception
	 */
	private static boolean checkColumn(Connection aConnection, String aTable,
			String aColumn, String aSearchFor) throws Exception {
		Statement lStatement = aConnection.createStatement();
		ResultSet lValues = null;
		try {
			lValues = lStatement.executeQuery("SELECT COUNT(" + aColumn
					+ ") FROM " + aTable + " WHERE " + aColumn + " = '"
					+ aSearchFor + '\'');
			if (lValues.next()) {
				int lCount = lValues.getInt(1);
				return lCount > 0;
			}
		} finally {
			if (lValues != null)
				lValues.close();
		}
		throw new Exception("SQL error");
	}

	/**
	 * Get the list of the columns in the table
	 * 
	 * @param aConnection
	 *            SQLite connection to query the columns
	 * @param aTable
	 *            the table to check
	 * @return the list of column names in the table
	 * @throws SQLException
	 */
	private static Iterable<String> getColumnList(Connection aConnection,
			String aTable) throws SQLException {
		List<String> lResult = new ArrayList<String>();
		Statement lStatement = aConnection.createStatement();
		ResultSet lTable = null;
		try {
			lTable = lStatement
					.executeQuery("SELECT sql FROM sqlite_master WHERE tbl_name = '"
							+ aTable + "' AND type = 'table'");
			if (lTable.next()) {
				Pattern lColumnPattern = Pattern.compile("\\((.*)\\)");
				String lCreateSql = lTable.getString(1);
				Matcher lMatcher = lColumnPattern.matcher(lCreateSql);
				if (lMatcher.find()) {
					String lColumnDeclarationList = lMatcher.group(0);
					lColumnDeclarationList = lColumnDeclarationList.substring(
							1, lColumnDeclarationList.length() - 1);
					String[] lColumnDeclarations = lColumnDeclarationList
							.split(",");
					lResult = new ArrayList<String>(lColumnDeclarations.length);
					for (String lColumnDeclaration : lColumnDeclarations) {
						String lColumnName = lColumnDeclaration.trim().split(
								" ")[0];
						lResult.add(lColumnName);
					}
				}
			}
			return lResult;
		} finally {
			if (lTable != null)
				lTable.close();
		}
	}

	/**
	 * Gets the list of tables in the SQLite database
	 * 
	 * @return list of tables
	 * @throws SQLException
	 */
	private static List<String> getTableList(Connection aConnection)
			throws SQLException {
		List<String> lResult = new ArrayList<String>();
		Statement lStatement = aConnection.createStatement();
		ResultSet lTables = null;
		try {
			lTables = lStatement
					.executeQuery("SELECT tbl_name FROM sqlite_master WHERE type='table'");
			while (lTables.next()) {
				lResult.add(lTables.getString(1));
			}
			return lResult;
		} finally {
			if (lTables != null)
				lTables.close();
		}
	}

}
