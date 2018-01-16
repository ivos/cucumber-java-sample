package ft.support;

public class OracleDB {
    /**
     * Oracle returns dates as timestamps.
     * To keep verification the same for H2 and Oracle,
     * such values must be truncated to only include the date part.
     */
    public static String asDate(String value) {
        return value.substring(0, 10);
    }
}
