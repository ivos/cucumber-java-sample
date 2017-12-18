package ft.steps;

import cucumber.api.java.Before;
import ft.support.DB;

public class CommonHooks {

	@Before
	public void beforeEachScenario() {
		DB.initializeIfRequired();
//        DB.delete("my_table");
	}
}
