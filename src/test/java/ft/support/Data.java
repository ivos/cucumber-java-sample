package ft.support;

import cucumber.api.DataTable;
import org.junit.Assert;

import java.util.List;
import java.util.Map;

public class Data {

	public static List<Map<String, String>> asMaps(DataTable dataTable) {
		return dataTable.asMaps(String.class, String.class);
	}

	public static Map<String, String> asRow(DataTable dataTable) {
		List<Map<String, String>> data = asMaps(dataTable);
		if (data.size() != 1) {
			Assert.fail("Must provide exactly 1 data row.");
		}
		return data.get(0);
	}
}
