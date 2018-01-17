package ft.support;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.StringWriter;

public class JSON {

	public static ObjectMapper objectMapper;

	static {
		objectMapper = new ObjectMapper();
		objectMapper.registerModule(new JavaTimeModule());
		objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
		objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
		objectMapper.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
	}

	/**
	 * Re-format a JSON {@link String}.
	 *
	 * @param json JSON {@link String} to be formatted
	 * @return Re-formatted JSON {@link String}
	 */
	public static String format(String json) {
		return convertToString(convertToJson(json));
	}

	private static JsonNode convertToJson(String json) {
		try {
			return objectMapper.readTree(json);
		} catch (Exception e) {
			throw new RuntimeException(
					"Cannot parse JSON string. Does it contain valid JSON? The content is:\n" + json, e);
		}
	}

	public static String convertToString(TreeNode jsonNode) {
		try {
			StringWriter stringWriter = new StringWriter();
			JsonGenerator generator = new JsonFactory().createGenerator(stringWriter);
			generator.useDefaultPrettyPrinter();
			objectMapper.writeTree(generator, jsonNode);
			return stringWriter.toString();
		} catch (Exception e) {
			throw new RuntimeException("Cannot format JSON string. Does it contain valid JSON?", e);
		}
	}
}
