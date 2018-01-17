package ft.support;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class File {

	public static String load(Object testInstance, String fileName) {
		return new String(loadRaw(testInstance.getClass(), fileName), StandardCharsets.UTF_8);
	}

	public static String load(Class<?> testClass, String fileName) {
		return new String(loadRaw(testClass, fileName), StandardCharsets.UTF_8);
	}

	public static byte[] loadRaw(Object testInstance, String fileName) {
		return loadRaw(testInstance.getClass(), fileName);
	}

	public static byte[] loadRaw(Class<?> testClass, String fileName) {
		InputStream inputStream = testClass.getResourceAsStream(fileName);
		if (null == inputStream) {
			throw new RuntimeException("Cannot read file " + fileName + ". Does it exist?");
		}
		try {
			return IOUtils.toByteArray(inputStream);
		} catch (IOException e1) {
			throw new RuntimeException("Cannot read file " + fileName
					+ ". Do I have permission to read it?");
		}
	}
}
