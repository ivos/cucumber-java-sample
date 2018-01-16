package ft.steps;

import cucumber.api.DataTable;
import cucumber.api.java8.En;
import ft.support.DB;

import java.util.List;
import java.util.Map;

public class DbTableSteps implements En {

	private static final String TABLE = "customer";

	public DbTableSteps() {
		Given("^DB table customer$", () -> {
			DB.executeUpdate("drop table customer if exists");
			DB.executeUpdate("create table customer (" +
					" id bigint not null primary key," +
					" name varchar2(100) not null," +
					" date_acquired date," +
					" time_created timestamp not null," +
					" comment varchar2(100)" +
					")");
		});

		Given("^customers:$", (DataTable dataTable) -> {
			List<Map<String, String>> data = dataTable.asMaps(String.class, String.class);
			DB.insert(TABLE,
					DB.RowsBuilder.from(data)
							.with("name", "Default customer name")
							.with("time_created", "2016-12-31T23:59:58.123Z")
							.asDate("date_acquired")
							.asTimestamp("time_created")
							.build());
		});

		Then("^there are (\\d+) customers$",
				(Integer count) -> DB.awaitRowCount(TABLE, count));

		Then("^the customers are:$", (DataTable dataTable) -> {
			List<Map<String, String>> expectedData = dataTable.asMaps(String.class, String.class);
			DB.verify(TABLE, expectedData);
		});
	}
}
