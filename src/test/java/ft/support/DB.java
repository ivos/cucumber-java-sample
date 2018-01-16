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
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class DB {

	public static final String DEFAULT_PROPERTIES_FILENAME =
			"target/test-classes/feature-tests.properties";
	public static String driverClassName, url, userName, password, schema, dialect;
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
				dialect = (String) properties.get("database.dialect");

				Objects.requireNonNull(driverClassName, "Driver class name is required.");
				Objects.requireNonNull(url, "Database URL is required.");
				Objects.requireNonNull(userName, "Database user name is required.");
				Objects.requireNonNull(password, "Database password is required.");
				Objects.requireNonNull(schema, "Database schema is required.");
				Objects.requireNonNull(dialect, "Database dialect is required.");
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
		} else if (value instanceof Byte) {
			statement.setByte(index, (byte) value);
		} else if (value instanceof Short) {
			statement.setShort(index, (short) value);
		} else if (value instanceof Integer) {
			statement.setInt(index, (int) value);
		} else if (value instanceof Long) {
			statement.setLong(index, (long) value);
		} else if (value instanceof Float) {
			statement.setFloat(index, (float) value);
		} else if (value instanceof Double) {
			statement.setDouble(index, (double) value);
		} else if (value instanceof BigDecimal) {
			statement.setBigDecimal(index, (BigDecimal) value);
		} else if (value instanceof Date) {
			statement.setDate(index, (Date) value);
		} else if (value instanceof Time) {
			statement.setTime(index, (Time) value);
		} else if (value instanceof Timestamp) {
			statement.setTimestamp(index, (Timestamp) value);
		} else {
			statement.setString(index, String.valueOf(value));
		}
	}

	private static Map<String, Object> removeEmptyStrings(Map<String, Object> row) {
		Map<String, Object> mappedRow = new LinkedHashMap<>();
		row.forEach((key, value) -> {
			if (!"".equals(value)) {
				mappedRow.put(key, value);
			}
		});
		return mappedRow;
	}

	public static void executeUpdate(String sql) {
		executeUpdate(sql, null);
	}

	public static void executeUpdate(String sql, Collection<Object> parameters) {
		verifyInitialized();
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			if (parameters != null) {
				int index = 1;
				for (Object paramValue : parameters) {
					setParameter(statement, index++, paramValue);
				}
			}
			statement.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException("Error executing DB update.", e);
		}
	}

	public static void delete(String... tables) {
		for (String table : tables) {
			String sql = "delete from " + schema + "." + table;
			executeUpdate(sql);
		}
	}

	public static void insert(String table, Map<String, Object> row) {
		Map<String, Object> nonEmptyRow = removeEmptyStrings(row);
		String sql = "insert into " + schema + "." + table
				+ " (" + String.join(",", nonEmptyRow.keySet()) + ")"
				+ " values ("
				+ String.join(",", Collections.nCopies(nonEmptyRow.size(), "?"))
				+ ")";
		executeUpdate(sql, nonEmptyRow.values());
	}

	public static void insert(String table, List<Map<String, Object>> rows) {
		for (Map<String, Object> row : rows) {
			insert(table, row);
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
			} else if (value == null) {
				valueString = null;
			} else {
				valueString = String.valueOf(value);
			}
			row.put(column, valueString);
		}
		return Collections.unmodifiableMap(row);
	}

	private static List<Map<String, String>> executeRowsQuery(
			String sql,
			Collection<String> columns,
			UnaryOperator<Map<String, String>> rowConverter) {
		verifyInitialized();
		List<Map<String, String>> rows = new ArrayList<>();
		try (PreparedStatement statement = connection.prepareStatement(sql)) {
			try (ResultSet rs = statement.executeQuery()) {
				while (rs.next()) {
					Map<String, String> row = queryRow(columns, rs);
					Map<String, String> convertedRow = rowConverter.apply(row);
					rows.add(convertedRow);
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error executing DB query.", e);
		}
		return rows;
	}

	private static List<Map<String, String>> select(
			String table,
			Collection<String> columns,
			UnaryOperator<Map<String, String>> rowConverter) {
		String sql = "select " + String.join(", ", columns) + " from " + schema + "." + table;
		return executeRowsQuery(sql, columns, rowConverter);
	}

	public static Object executeScalarQuery(String sql) {
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

	public static Long selectCount(String table) {
		String sql = "select count(*) from " + schema + "." + table;
		return ((Number) executeScalarQuery(sql)).longValue();
	}

	public static Long nextSequenceValue(String sequence) {
		String sql;
		if ("oracle".equals(dialect)) {
			sql = "select " + DB.schema + "." + sequence + ".nextval from dual";
		} else {
			sql = "select nextval('" + DB.schema + "." + sequence + "')";
		}
		return ((Number) executeScalarQuery(sql)).longValue();
	}

	public static void awaitRowCount(String table, int count) {
		int repeat = 0;
		long actual;
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
			if (!Objects.equals(expectedValue, actualValue)) {
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

	private static void appendActualDBData(
			StringBuilder sb, List<Map<String, String>> actualData) {
		sb.append("\n Actual DB data:\n");
		for (Map<String, String> actualRow : actualData) {
			sb.append(actualRow);
			sb.append("\n");
		}
	}

	private static List<Map<String, String>> emptyStringsAsNulls(
			List<Map<String, String>> data) {
		return data.stream()
				.map(row -> {
					Map<String, String> mappedRow = new LinkedHashMap<>();
					row.forEach((key, value) -> {
						if ("".equals(value)) {
							value = null;
						}
						mappedRow.put(key, value);
					});
					return mappedRow;
				})
				.collect(Collectors.toList());
	}

	public static void verify(String table, List<Map<String, String>> expectedData) {
		verify(table, expectedData, UnaryOperator.identity());
	}

	public static void verify(
			String table,
			List<Map<String, String>> expectedData,
			UnaryOperator<Map<String, String>> rowConverter) {
		if (expectedData.size() == 0) {
			Assert.assertEquals("Row count in table " + table,
					new Long(0), selectCount(table));
			return;
		}

		Set<String> columns = expectedData.get(0).keySet();
		List<Map<String, String>> expectedDataNulls = emptyStringsAsNulls(expectedData);
		List<Map<String, String>> actualData = select(table, columns, rowConverter);
		List<Map<String, String>> actualDataToMatch = new ArrayList<>(actualData);
		for (Map<String, String> expectedRow : expectedDataNulls) {
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

	public static class RowBuilder {
		private final Map<String, Object> row;

		private RowBuilder(Map<String, Object> row) {
			this.row = row;
		}

		public static RowBuilder create() {
			return new RowBuilder(new LinkedHashMap<>());
		}

		public static RowBuilder from(Map<String, String> row) {
			return new RowBuilder(new LinkedHashMap<>(row));
		}

		public RowBuilder with(String column, Object value) {
			if (!row.containsKey(column)) {
				row.put(column, value);
			}
			return this;
		}

		private void convertColumns(Function<Object, Object> converter, String... columns) {
			Arrays.asList(columns).forEach(column -> {
				Object value = row.get(column);
				if (value != null && !"".equals(value)) {
					row.put(column, converter.apply(value));
				}
			});
		}

		public RowBuilder asTimestamp(String... columns) {
			convertColumns(
					value -> Timestamp.from(ZonedDateTime.parse(String.valueOf(value)).toInstant()),
					columns);
			return this;
		}

		public RowBuilder asDate(String... columns) {
			convertColumns(
					value -> Date.valueOf(LocalDate.parse(String.valueOf(value))),
					columns);
			return this;
		}

		public Map<String, Object> build() {
			return Collections.unmodifiableMap(row);
		}
	}

	public static class RowsBuilder {
		private final List<Map<String, String>> originalData;
		private final Map<String, Function<Map<String, String>, Object>> providers;
		private final List<String> timestamps, dates;

		private RowsBuilder(List<Map<String, String>> originalData) {
			this.originalData = originalData;
			providers = new LinkedHashMap<>();
			timestamps = new ArrayList<>();
			dates = new ArrayList<>();
		}

		public static RowsBuilder from(List<Map<String, String>> data) {
			return new RowsBuilder(data);
		}

		public RowsBuilder with(String column, Function<Map<String, String>, Object> provider) {
			providers.put(column, provider);
			return this;
		}

		public RowsBuilder with(String column, Supplier<Object> provider) {
			providers.put(column, (row) -> provider.get());
			return this;
		}

		public RowsBuilder with(String column, Object value) {
			providers.put(column, (row) -> value);
			return this;
		}

		public RowsBuilder asTimestamp(String... columns) {
			timestamps.addAll(Arrays.asList(columns));
			return this;
		}

		public RowsBuilder asDate(String... columns) {
			dates.addAll(Arrays.asList(columns));
			return this;
		}

		private Map<String, Object> mapRow(Map<String, String> originalRow) {
			RowBuilder builder = RowBuilder.from(originalRow);
			providers.forEach((column, provider) -> {
				builder.with(column, provider.apply(originalRow));
			});
			builder.asTimestamp(timestamps.toArray(new String[0]));
			builder.asDate(dates.toArray(new String[0]));
			return builder.build();
		}

		public List<Map<String, Object>> build() {
			List<Map<String, Object>> data = originalData.stream()
					.map(this::mapRow)
					.collect(Collectors.toList());
			return Collections.unmodifiableList(data);
		}
	}

	public static class RowConverterBuilder {
		private final Map<String, UnaryOperator<String>> converters;

		private RowConverterBuilder() {
			converters = new LinkedHashMap<>();
		}

		public static RowConverterBuilder create() {
			return new RowConverterBuilder();
		}

		public RowConverterBuilder convert(String column, UnaryOperator<String> converter) {
			converters.put(column, converter);
			return this;
		}

		public UnaryOperator<Map<String, String>> build() {
			return row -> {
				Map<String, String> converted = new LinkedHashMap<>(row);
				converters.forEach((column, converter) -> {
					converted.put(column, converter.apply(row.get(column)));
				});
				return Collections.unmodifiableMap(converted);
			};
		}
	}
}
