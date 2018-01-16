package ft.steps;

import cucumber.api.java8.En;
import ft.support.DB;
import org.junit.Assert;

public class DbSequenceSteps implements En {

	private Long sequenceValue;

	public DbSequenceSteps() {
		Given("^DB sequence$", () -> {
			DB.executeUpdate("drop sequence seq_1 if exists");
			DB.executeUpdate("create sequence seq_1");
		});

		When("^I get next sequence value$", () -> {
			sequenceValue = DB.nextSequenceValue("seq_1");
		});

		Then("^the sequence value is (\\d+)$", (Long expectedValue) -> {
			Assert.assertEquals(expectedValue, sequenceValue);
		});
	}
}
