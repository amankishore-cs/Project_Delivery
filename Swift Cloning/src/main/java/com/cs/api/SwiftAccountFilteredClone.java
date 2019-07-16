package com.cs.api;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

@SuppressWarnings("rawtypes")
public class SwiftAccountFilteredClone {

	private Properties prop;
	private static final String X_STORAGE_URL = "X-Storage-Url";
	private static final String X_STORAGE_TOKEN = "X-Storage-Token";
	private static final String GET = "GET";
	private static final String SWIFT_SOURCE_URL = "swiftSourceUrl";
	private static final String X_AUTH_USER = "X-Auth-User";
	private static final String X_AUTH_KEY = "X-Auth-Key";
	private static final String ACCEPT = "Accept";
	private static final String APPLICATION_JSON = "application/json";
	private static String X_AUTH_TOKEN = "X-Auth-Token";
	private static String STORAGE_URL = "";
	private static String STORAGE_TOKEN = "";
	private static String DEST_AUTH_TOKEN;
	private static String DEST_AUTH_URL;
	private static String FILE_PATH;
	private List<String> alEntry = new ArrayList<String>();
	private final static Logger logger = Logger.getLogger("log.txt");
	HashMap<String, Object> headerMap = new HashMap<String, Object>();
	String output;

	public SwiftAccountFilteredClone() {
		try {
			String ConfigFile = "config.properties";
			InputStream input = getClass().getClassLoader().getResourceAsStream(ConfigFile);
			if (input == null) {

				return;
			}
			prop = new Properties();
			prop.load(input);

		} catch (IOException ex) {
			ex.printStackTrace();
		}

	}

	public String execute() throws Exception {
		try {
			String accountName = prop.getProperty("backupAccountName");
			String userName = accountName + ":" + accountName;
			/*ArrayList<String> sourceContainers = new ArrayList<String>(
					Arrays.asList("Image", "Document", "Video", "Attachment", "Icons"));*/
			FILE_PATH = prop.getProperty("filepath");
			if (FILE_PATH != null) {
				List<String> alEntry = getAssetIds(FILE_PATH);
				headerMap = getInfoToAccessAccount(accountName, userName, "pass@123");
				getObjectFromSourceSwift(alEntry, headerMap);

			} else {
				try {
					throw new FileNotFoundException("Property file: " + FILE_PATH + " Not found in the class path");
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return "Response Received";
	}

	private void getObjectFromSourceSwift(List<String> alEntry, HashMap<String, Object> headerMap) throws Exception {
		try {
			Iterator iterator = alEntry.iterator();
			while (iterator.hasNext()) {
				String sAssetEntry = (String) iterator.next();
				String[] result = sAssetEntry.split(";");
				
				String sObjectName = result[0];
				String containerName = result[1];
	
				String sStorageURL = (String) headerMap.get("X-Storage-Url");
	

					//Check if data already exist in Swift - B
					
					String uri = sStorageURL + "/" + containerName + "/" + sObjectName;
					X_AUTH_TOKEN = (String) headerMap.get("X-Auth-Token");
					Map<String, Object> requestHeaders = new HashMap<>();
					requestHeaders.put("X-Auth-Token", X_AUTH_TOKEN);
					HttpURLConnection sourceSwiftHTTPConnection = prepareConnection(uri, "GET", requestHeaders, false, true,
							true);
					sourceSwiftHTTPConnection.connect();

					if (sourceSwiftHTTPConnection.getResponseCode() != 200) {
						System.out.println("Failed : HTTP error code : " + sourceSwiftHTTPConnection.getResponseCode()
								+ ", filename:" + sObjectName);
					} else {
						logger.log(Level.INFO, "Source connection Established");
						InputStream inputStream = sourceSwiftHTTPConnection.getInputStream();
						uploadObjectToTarget(inputStream, sourceSwiftHTTPConnection, containerName, sObjectName);
						inputStream.close();
						logger.log(Level.INFO, "Object Id :: "+ sObjectName + "Container :"+containerName +"Copied");
					}

					
					/* byte[] byteArray =  IOUtils.toByteArray(sourceSwiftHTTPConnection.getInputStream()); 
					 File file = new File("D://test.png"); 
					 OutputStream os = new FileOutputStream(file);
					  
					  // Starts writing the bytes in it os.write(byteArray);
					 os.write(byteArray);
					 System.out.println("Successfully" + " byte inserted");
					  
					 // Close the file
					 os.close();*/
					 

			}
			System.out.println("Cloning DONE");
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		
	}

	private void uploadObjectToTarget(InputStream sourceInputStream, HttpURLConnection sourceSwiftHTTPConnection,
			String containerName, String objectName) throws Exception {
		String dest_Usernamme = prop.getProperty("dest_AccountName");
		String dest_Passkey = prop.getProperty("dest_passkey");
		dest_Usernamme = dest_Usernamme + ":" + dest_Usernamme;

		HashMap<String, String> mp_dest_header = getAuthtokenForDestination(dest_Usernamme, dest_Passkey);

		String sDestStorageURL = mp_dest_header.get("X-Storage-Url");
		String sAuthToken = mp_dest_header.get("X-Auth-Token");
		String dest_URI = sDestStorageURL + "/" + containerName + "/" + objectName;

		// Establish connection to destination:

		Map<String, Object> requestHeaders = new HashMap<String, Object>();
		// Adding the headers:
		requestHeaders.put("X-Auth-Token", sAuthToken);
		requestHeaders.put("Content-Length", sourceSwiftHTTPConnection.getHeaderField("Content-Length"));
		requestHeaders.put("Content-Type", sourceSwiftHTTPConnection.getHeaderField("Content-Type"));
		requestHeaders.put("last-modified", sourceSwiftHTTPConnection.getHeaderField("last-modified"));
		requestHeaders.put("X-Object-Meta-Deleted", sourceSwiftHTTPConnection.getHeaderField("X-Object-Meta-Deleted"));
		requestHeaders.put("X-Object-Meta-Format", sourceSwiftHTTPConnection.getHeaderField("X-Object-Meta-Format"));
		requestHeaders.put("X-Object-Meta-Name", sourceSwiftHTTPConnection.getHeaderField("X-Object-Meta-Name"));
		requestHeaders.put("X-Object-Meta-Resolution",
				sourceSwiftHTTPConnection.getHeaderField("X-Object-Meta-Resolution"));
		requestHeaders.put("X-Object-Meta-Type", sourceSwiftHTTPConnection.getHeaderField("X-Object-Meta-Type"));
		requestHeaders.put("X-Object-Meta-Format", sourceSwiftHTTPConnection.getHeaderField("X-Object-Meta-Format"));

		if (headerMap.containsKey("X-Delete-After")) {
			requestHeaders.put("X-Delete-After", sourceSwiftHTTPConnection.getHeaderField("X-Delete-After"));
		}

		if (headerMap.containsKey("X-Object-Meta-Original")) {
			requestHeaders.put("X-Object-Meta-Original",
					sourceSwiftHTTPConnection.getHeaderField("X-Object-Meta-Original"));
		}

		if (headerMap.containsKey("X-Object-Meta-Thumb")) {
			requestHeaders.put("X-Object-Meta-Thumb", sourceSwiftHTTPConnection.getHeaderField("X-Object-Meta-Thumb"));
		}
		HttpURLConnection destSwiftHTTPConnection = prepareConnection(dest_URI, "PUT", requestHeaders, false, true,
				true);
		 byte[] byteArray =IOUtils.toByteArray(sourceSwiftHTTPConnection.getInputStream());
		 destSwiftHTTPConnection.setFixedLengthStreamingMode(byteArray.length);
		 DataOutputStream dos = new DataOutputStream(destSwiftHTTPConnection.getOutputStream());
		 dos.write(byteArray);
		 dos.close();

		System.out.println("End of the Upload programme");
	}

	private HttpURLConnection prepareConnection(String uri, String requestMethod, Map<String, Object> requestHeaders,
			Boolean useCaches, Boolean doOutput, Boolean doInput) throws IOException {
		URL url = new URL(uri);
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		if (requestHeaders.containsValue("null")) {
		}
		for (Map.Entry<String, Object> entry : requestHeaders.entrySet()) {
			if (entry.getValue() != null) {
				connection.setRequestProperty(entry.getKey(), entry.getValue().toString());
			}
		}
		connection.setRequestMethod(requestMethod);
		connection.setUseCaches(useCaches);
		connection.setDoOutput(doOutput);
		connection.setDoInput(doInput);

		return connection;

	}

	private HashMap<String, String> getAuthtokenForDestination(String dest_userName, String dest_passkey)
			throws ClientProtocolException, IOException {
		HashMap<String, String> mp_dest_header = new HashMap<String, String>();
		String dest_URL = prop.getProperty("swiftDestinationUrl");
		HttpClient httpClient = HttpClientBuilder.create().build();

		HttpGet getRequest = new HttpGet(dest_URL);
		getRequest.addHeader("X-Auth-User", dest_userName);
		getRequest.addHeader("X-Auth-Key", dest_passkey);

		HttpResponse response = httpClient.execute(getRequest);
		Header[] headers = response.getAllHeaders();
		for (Header header : headers) {
			mp_dest_header.put(header.getName(), header.getValue());

		}

		return mp_dest_header;

	}

	private HashMap<String, Object> getInfoToAccessAccount(String accountName, String userName, String passkey) {

		try {

			String uri = prop.getProperty(SWIFT_SOURCE_URL);
			// Get Connection MetaData:
			HttpClient httpClient = HttpClientBuilder.create().build();
			HttpGet getRequest = new HttpGet(uri);
			getRequest.addHeader("X-Auth-User", userName);
			getRequest.addHeader("X-Auth-Key", passkey);

			HttpResponse response = httpClient.execute(getRequest);
			Header[] headers = response.getAllHeaders();
			for (Header header : headers) {
				headerMap.put(header.getName(), header.getValue());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return headerMap;
	}

	private List<String> getAssetIds(String filepath) throws IOException {
		// READ Asset_Ids from the filessystem.
		try {
			BufferedReader reader = new BufferedReader(new FileReader(filepath));
			String line;
			int x = 1;

			while ((line = reader.readLine()) != null && line != "") {
				System.out.println(">>>>>>"+line.trim());
				alEntry.add(line.trim());
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return alEntry;
	}

}
