package ft.support;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import io.restassured.response.ValidatableResponse;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.Map;
import java.util.function.UnaryOperator;

import static ft.support.Matchers.jsonEqualTo;

public class REST {

	public static DocumentContext setBy(DocumentContext ctx, Map<String, String> data) {
		data.forEach((key, value) -> ctx.set(key, value));
		return ctx;
	}

	public static UnaryOperator<DocumentContext> override(Map<String, String> data) {
		return ctx -> REST.setBy(ctx, data);
	}

	public static UnaryOperator<DocumentContext> override() {
		return UnaryOperator.identity();
	}

	public static DocumentContext replace(DocumentContext ctx, String... paths) {
		Arrays.stream(paths)
				.forEach(path -> ctx.set(path, "REPLACED"));
		return ctx;
	}

	public static UnaryOperator<DocumentContext> replacer(String... paths) {
		return ctx -> REST.replace(ctx, paths);
	}

	public static void verifyResponse(
			Object testInstance,
			String fileName,
			ValidatableResponse response) {
		verifyResponse(testInstance, fileName, response, null, null);
	}

	public static void verifyResponse(
			Object testInstance,
			String fileName,
			ValidatableResponse response,
			UnaryOperator<DocumentContext> override) {
		verifyResponse(testInstance, fileName, response, override, null);
	}

	public static void verifyResponse(
			Object testInstance,
			String fileName,
			ValidatableResponse response,
			UnaryOperator<DocumentContext> override,
			UnaryOperator<DocumentContext> replacer) {
		String expected = File.load(testInstance, fileName);
		if (override != null) {
			expected = override.apply(JsonPath.parse(expected)).jsonString();
		}
		Matcher<String> matcher;
		if (replacer == null) {
			matcher = jsonEqualTo(fileName, expected);
		} else {
			matcher = jsonEqualTo(fileName, expected, replacer);
		}
		response.body(matcher);
	}
}
