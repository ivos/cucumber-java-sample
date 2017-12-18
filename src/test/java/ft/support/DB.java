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

    private static Map<String, Object> queryRow(Collection<String> columns, ResultSet rs) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        for (String column : columns) {
            row.put(column, rs.getObject(column));
        }
        return Collections.unmodifiableMap(row);
    }

    private static List<Map<String, Object>> executeRowsQuery(String sql, Collection<String> columns) {
        verifyInitialized();
        List<Map<String, Object>> rows = new ArrayList<>();
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
        return (Long) executeScalarQuery(sql, Collections.emptyList());
    }

    private static List<Map<String, Object>> select(String table, Collection<String> columns) {
        String sql = "select " + String.join(", ", columns) + " from " + schema + "." + table;
        return executeRowsQuery(sql, columns);
    }

    private static boolean dataRowMatches(Map<String, String> expectedRow, Map<String, Object> actualRow) {
        for (String column : expectedRow.keySet()) {
            String expectedValue = expectedRow.get(column);
            String actualValue = String.valueOf(actualRow.get(column));
            if (!expectedValue.equals(actualValue)) {
                return false;
            }
        }
        return true;
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
            Assert.fail("Row count in table " + table + " expected " + count + ", but was " + actual + ".");
        }
    }

    public static void verify(
            String table, Collection<String> columns,
            List<Map<String, String>> expectedData) {
        List<Map<String, Object>> actualData = select(table, columns);
        for (Map<String, String> expectedRow : expectedData) {
            boolean matchFound = false;
            if (actualData.size() == 0) {
                Assert.fail("Missing row:\n" + expectedRow);
            }
            for (Map<String, Object> actualRow : actualData) {
                if (dataRowMatches(expectedRow, actualRow)) {
                    matchFound = true;
                    actualData.remove(actualRow);
                    break;
                }
            }
            if (!matchFound) {
                StringBuilder sb = new StringBuilder();
                sb.append("No match found for expected row:\n");
                sb.append(expectedRow);
                sb.append("\n Actual DB data:\n");
                for (Map<String, Object> actualRow : actualData) {
                    sb.append(actualRow);
                    sb.append("\n");
                }
                Assert.fail(sb.toString());
            }
        }
        if (actualData.size() > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("Unexpected row(s):\n");
            for (Map<String, Object> actualRow : actualData) {
                sb.append(actualRow);
                sb.append("\n");
            }
            Assert.fail(sb.toString());
        }
    }
}
