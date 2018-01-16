package ft.steps;

import cucumber.api.java.Before;
import ft.support.DB;

import java.time.ZoneOffset;
import java.util.TimeZone;

public class CommonHooks {

	static {
		TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
		// Set timezone for h2 database
		System.setProperty("user.timezone", "UTC");
	}

	@Before
	public void beforeEachScenario() {
		DB.initializeIfRequired();
//        DB.delete("customer");
	}
}
