package com.azure.service.util;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class AzureUtil {

	/*
	 * This method generate SAS Token for given resource uri.
	 * 
	 * @param resourceUri
	 * 
	 * @param keyName
	 * 
	 * @param key
	 * 
	 * @return String
	 */
	public static String getSASToken(String resourceUri, String keyName, String key) {
		long epoch = System.currentTimeMillis() / 1000L;
		// NOTE : This is for demo one, Change this for your application specific expiry
		// duration.
		int week = 60 * 60 * 24 * 7;
		String expiry = Long.toString(epoch + week);
		String sasToken = null;
		try {
			String stringToSign = URLEncoder.encode(resourceUri, StandardCharsets.UTF_8.toString()) + "\n" + expiry;
			String signature = getHMAC256(key, stringToSign);
			sasToken = "SharedAccessSignature sr=" + URLEncoder.encode(resourceUri, StandardCharsets.UTF_8.toString())
					+ "&sig=" + URLEncoder.encode(signature, StandardCharsets.UTF_8.toString()) + "&se=" + expiry
					+ "&skn=" + keyName;
		} catch (Exception e) {
			System.err.println(" Could not url encode " + e.getMessage());
		}
		return sasToken;
	}

	/*
	 * Hashing string using service bus key
	 * 
	 * @param key
	 * 
	 * @param input
	 * 
	 * @return String
	 */
	public static String getHMAC256(String key, String input) {
		Mac sha256HMAC;
		String hash = null;
		try {
			sha256HMAC = Mac.getInstance("HmacSHA256");
			SecretKeySpec secretKey = new SecretKeySpec(key.getBytes(), "HmacSHA256");
			sha256HMAC.init(secretKey);
			Base64.Encoder encoder = Base64.getEncoder();

			hash = new String(encoder.encode(sha256HMAC.doFinal(input.getBytes(StandardCharsets.UTF_8))));

		} catch (InvalidKeyException | NoSuchAlgorithmException | IllegalStateException e) {
			e.printStackTrace();
		}

		return hash;
	}

}
