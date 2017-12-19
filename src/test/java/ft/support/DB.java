package ft.support;

import org.junit.Assert;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DB {

	private static final String DEFAULT_PROPERTIES_FILENAME = "target/test-classes/feature-tests.properties";
	private static String driverClassName, url, userName, password, schema;
	private static Connection connection;
	private static boolean initialized = false;

	public static void initializeIfRequired() {
		initializeIfRequired(DEFAULT_PROPERTIES_FILENAME);
	}

	public static void initializeIfRequired(String propertiesFileName) {
		if (!initialized) {
			Runtime.getRuntime().addShutdownHook(new Thread(DB::shutdown));
			loadProperties(propertiesFileName);
			openConnection();
			initialized = true;
		}
	}

	public static void shutdown() {
		closeConnection();
		initialized = false;
	}

	private static void loadProperties(String fileName) {
		File file = new File(fileName);
		if (!file.exists()) {
			throw new RuntimeException("Properties file not found: " + fileName +
					"\nShould be at: " + new File("").getAbsolutePath() + "/" + fileName);
		}
		try {
			try (InputStream is = new FileInputStream(file)) {
				java.util.Properties properties = new java.util.Properties();
				properties.load(is);

				driverClassName = (String) properties.get("database.driverClassName");
				url = (String) properties.get("database.connectionUrl");
				userName = (String) properties.get("database.userName");
				password = (String) properties.get("database.password");
				schema = (String) properties.get("database.schema");

				Objects.requireNonNull(driverClassName, "Driver class name is required.");
				Objects.requireNonNull(url, "Database URL is required.");
				Objects.requireNonNull(userName, "Database user name is required.");
				Objects.requireNonNull(password, "Database password is required.");
				Objects.requireNonNull(schema, "Database schema is required.");
			}
		} catch (IOException e) {
			throw new RuntimeException("Properties file unreadable: " + fileName);
		}
	}

	private static void openConnection() {
		try {
			Class.forName(driverClassName);
			connection = DriverManager.getConnection(url, userName, password);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Cannot find driver class " + driverClassName, e);
		} catch (SQLException e) {
			throw new RuntimeException("Cannot connect to database URL " + url, e);
		}
	}

	private static void verifyInitialized() {
		if (connection == null) {
			throw new RuntimeException("DB is not initialized. Please initialize DB first.");
		}
	}

	private static void closeConnection() {
		verifyInitialized();
		try {
			connection.close();
		} catch (SQLException e) {
			throw new RuntimeException("Cannot close connection.", e);
		}
	}

	private static void setParameter(
			PreparedStatement statement, int index, Object value)
			throws SQLException {
		if (null == value) {
			throw new RuntimeException("Setting parameter value to null is not supported.");
		}
		if (value instanceof Boolean) {
			statement.setBoolean(index, (boolean) value);
		}
		if (value instanceof Byte) {
			statement.setByte(index, (byte) value);
		}
		if (value instanceof Short) {
			statement.setShort(index, (short) value);
		}
		if (value instanceof Integer) {
			statement.setInt(index, (int) value);
		}
		if (value instanceof Long) {
			statement.setLong(index, (long) value);
		}
		if (value instanceof Float) {
			statement.setFloat(index, (float) value);
		}
		if (value instanceof Double) {
			statement.setDouble(index, (double) value);
		}
		if (value instanceof BigDecimal) {
			statement.setBigDecimal(index, (BigDecimal) value);
		}
		if (value instanceof Date) {
			statement.setDate(index, (Date) value);
		}
		if (value instanceof Time) {
			statement.setTime(index, (Time) value);
		}
		if (value instanceof Timestamp) {
			statement.setTimestamp(index, (Timestamp) value);
		}
		statement.setString(index, String.valueOf(value));
	}

	private static void executeUpdate(String sql) {
		executeUpdate(sql, null);
	}

	private static void executeUpdate(String sql, List<Object> parameters) {
		verifyInitialized();
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			if (parameters != null) {
				for (int index = 0; index < parameters.size(); index++) {
					Object paramValue = parameters.get(index);
					setParameter(statement, index + 1, paramValue);
				}
			}
			statement.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("Error executing DB update.", e);
		}
	}

	private static Map<String, String> queryRow(Collection<String> columns, ResultSet rs)
			throws SQLException {
		Map<String, String> row = new LinkedHashMap<>();
		for (String column : columns) {
			Object value = rs.getObject(column);
			String valueString;
			if (value instanceof BigDecimal) {
				valueString = ((BigDecimal) value).stripTrailingZeros().toPlainString();
			} else {
				valueString = String.valueOf(value);
			}
			row.put(column, valueString);
		}
		return Collections.unmodifiableMap(row);
	}

	private static List<Map<String, String>> executeRowsQuery(String sql, Collection<String> columns) {
		verifyInitialized();
		List<Map<String, String>> rows = new ArrayList<>();
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			try (ResultSet rs = statement.executeQuery()) {
				while (rs.next()) {
					rows.add(queryRow(columns, rs));
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error executing DB query.", e);
		}
		return rows;
	}

	private static Object executeScalarQuery(String sql, Collection<String> columns) {
		verifyInitialized();
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			try (ResultSet rs = statement.executeQuery()) {
				rs.next();
				return rs.getObject(1);
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error executing DB query.", e);
		}
	}

	public static void delete(String... tables) {
		for (String table : tables) {
			String sql = "delete from " + schema + "." + table;
			executeUpdate(sql);
		}
	}

	private static Long selectCount(String table) {
		String sql = "select count(*) from " + schema + "." + table;
		return ((Number) executeScalarQuery(sql, Collections.emptyList())).longValue();
	}

	private static List<Map<String, String>> select(String table, Collection<String> columns) {
		String sql = "select " + String.join(", ", columns) + " from " + schema + "." + table;
		return executeRowsQuery(sql, columns);
	}

	public static void awaitRowCount(String table, int count) {
		int repeat = 0;
		long actual = 0;
		try {
			do {
				if (repeat > 0) {
					Thread.sleep(200L);
				}
				actual = selectCount(table);
				repeat++;
			} while (actual != count && repeat < 50);
		} catch (InterruptedException e) {
			throw new RuntimeException("Error awaiting table row count.", e);
		}
		if (actual != count) {
			Assert.fail("Row count in table " + table + " expected " + count
					+ ", but was " + actual + ".");
		}
	}

	private static List<String> getNotMatchingColumns(
			Map<String, String> expectedRow, Map<String, String> actualRow) {
		List<String> notMatchingColumns = new ArrayList<>();
		for (String column : expectedRow.keySet()) {
			String expectedValue = expectedRow.get(column);
			String actualValue = actualRow.get(column);
			if (!expectedValue.equals(actualValue)) {
				notMatchingColumns.add(column);
			}
		}
		return notMatchingColumns;
	}

	private static Map<String, String> getBestMatch(
			Map<String, String> expectedRow, List<Map<String, String>> actualData) {
		Map<String, String> bestMatch = null;
		int bestMatchingColumnsCount = -1;
		for (Map<String, String> actualRow : actualData) {
			List<String> notMatchingColumns = getNotMatchingColumns(expectedRow, actualRow);
			int matchingColumnsCount = expectedRow.keySet().size() - notMatchingColumns.size();
			if (matchingColumnsCount > bestMatchingColumnsCount) {
				bestMatchingColumnsCount = matchingColumnsCount;
				bestMatch = actualRow;
			}
		}
		return bestMatch;
	}

	private static boolean dataRowMatches(
			Map<String, String> expectedRow, Map<String, String> actualRow) {
		List<String> notMatchingColumns = getNotMatchingColumns(expectedRow, actualRow);
		return notMatchingColumns.size() == 0;
	}

	private static void appendActualDBData(StringBuilder sb, List<Map<String, String>> actualData) {
		sb.append("\n Actual DB data:\n");
		for (Map<String, String> actualRow : actualData) {
			sb.append(actualRow);
			sb.append("\n");
		}
	}

	public static void verify(
			String table, Collection<String> columns,
			List<Map<String, String>> expectedData) {
		List<Map<String, String>> actualData = select(table, columns);
		List<Map<String, String>> actualDataToMatch = new ArrayList<>(actualData);
		for (Map<String, String> expectedRow : expectedData) {
			boolean matchFound = false;
			if (actualDataToMatch.size() == 0) {
				StringBuilder sb = new StringBuilder();
				sb.append("Missing row:\n");
				sb.append(expectedRow);
				appendActualDBData(sb, actualData);
				Assert.fail(sb.toString());
			}
			for (Map<String, String> actualRow : actualDataToMatch) {
				if (dataRowMatches(expectedRow, actualRow)) {
					matchFound = true;
					actualDataToMatch.remove(actualRow);
					break;
				}
			}
			if (!matchFound) {
				Map<String, String> bestMatch = getBestMatch(expectedRow, actualDataToMatch);
				List<String> notMatchingColumns = getNotMatchingColumns(expectedRow, bestMatch);
				StringBuilder sb = new StringBuilder();
				sb.append("No match found for expected row:\n");
				sb.append(expectedRow);
				sb.append("\n Best match:\n");
				sb.append(bestMatch);
				sb.append("\n Differences:");
				for (String column : notMatchingColumns) {
					sb.append("\n  ");
					sb.append(column);
					sb.append(" expected: ");
					sb.append(expectedRow.get(column));
					sb.append(", but was: ");
					sb.append(bestMatch.get(column));
				}
				appendActualDBData(sb, actualData);
				Assert.fail(sb.toString());
			}
		}
		if (actualDataToMatch.size() > 0) {
			StringBuilder sb = new StringBuilder();
			sb.append("Unexpected row(s):\n");
			for (Map<String, String> actualRow : actualDataToMatch) {
				sb.append(actualRow);
				sb.append("\n");
			}
			appendActualDBData(sb, actualData);
			Assert.fail(sb.toString());
		}
	}
}
