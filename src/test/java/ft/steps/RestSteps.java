package ft.steps;

import cucumber.api.DataTable;
import cucumber.api.java8.En;
import ft.support.Data;
import ft.support.REST;
import io.restassured.RestAssured;
import io.restassured.response.ValidatableResponse;

public class RestSteps implements En {

	public static final String API = "https://swapi.co/api";
	private static ValidatableResponse response;

	public RestSteps() {
		When("^I get character by id (\\d+)$", (Integer id) -> {
			response = RestAssured
					.get(API + "/people/" + id)
					.then();
		});

		Then("^the response status is (\\d+)$", (Integer status) -> {
			response.statusCode(status);
		});

		Then("^the response is:$", (DataTable dataTable) -> {
			REST.verifyResponse(this, "people-response.json", response,
					REST.override(Data.asRow(dataTable)),
					REST.replacer("films", "species", "vehicles", "starships", "created", "edited")
			);
		});
	}
}
