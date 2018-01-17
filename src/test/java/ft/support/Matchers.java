package ft.support;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsEqual;

import java.util.Objects;
import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;

public class Matchers {

	/**
	 * Wraps {@link IsEqual} matcher
	 * and calls {@link org.junit.Assert#assertEquals(Object, Object)} when the values are not equal.
	 * <p>
	 * This provides proper IDE integration: the ability to compare expected and actual values in IDE side-by-side.
	 *
	 * @param expected expected value
	 */
	public static <T> Matcher<T> assertEqualTo(final T expected) {
		return new IsEqual<T>(expected) {
			@Override
			public boolean matches(Object actual) {
				boolean match = super.matches(actual);
				if (!match) {
					assertEquals(expected, actual);
				}
				return match;
			}
		};
	}

	/**
	 * Asserts that values are equal JSON Strings.
	 *
	 * @param templateFilename template filename
	 * @param expected         expected JSON value
	 */
	public static Matcher<String> jsonEqualTo(String templateFilename, String expected) {
		return new IsJsonEqual<>(templateFilename, expected, null);
	}

	/**
	 * Asserts that values are equal JSON Strings
	 * while applying a replacing function on the JSON model.
	 *
	 * @param templateFilename template filename
	 * @param expected         expected JSON value
	 * @param replacer         replacing function
	 */
	public static Matcher<String> jsonEqualTo(
			String templateFilename, String expected, UnaryOperator<DocumentContext> replacer) {
		return new IsJsonEqual<>(templateFilename, expected, replacer);
	}

	public static class IsJsonEqual<T> extends BaseMatcher<String> {
		private final String templateFilename;
		private final String expected;
		private final UnaryOperator<DocumentContext> replacer;

		public IsJsonEqual(String templateFilename, String expected,
		                   UnaryOperator<DocumentContext> replacer) {
			this.templateFilename = templateFilename;
			this.expected = expected;
			this.replacer = replacer;
		}

		private String normalize(String content) {
			String json = JSON.format(content);
			// pre-format first to ensure pretty error reporting of invalid JSON
			if (null != replacer) {
				json = replacer.apply(JsonPath.parse(json)).jsonString();
				json = JSON.format(json); // re-format again after replacing
			}
			// strip trailing zeroes from effectively whole numbers
			json = json.replaceAll("\\.0{1,3}(,\\r?\\n)", "$1");
			return json;
		}

		@Override
		public boolean matches(Object actual) {
			String expectedNormalized = normalize(expected);
			String actualNormalized = normalize((String) actual);
			boolean match = Objects.equals(expectedNormalized, actualNormalized);
			if (!match) {
				assertEquals("Template file: (" + templateFilename + ":1)",
						expectedNormalized, actualNormalized);
			}
			return match;
		}

		@Override
		public void describeTo(Description description) {
			description.appendValue(expected);
		}
	}
}
