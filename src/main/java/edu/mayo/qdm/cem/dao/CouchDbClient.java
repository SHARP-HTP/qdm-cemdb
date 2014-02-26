/*
 * Copyright: (c) 2004-2012 Mayo Foundation for Medical Education and 
 * Research (MFMER). All rights reserved. MAYO, MAYO CLINIC, and the
 * triple-shield Mayo logo are trademarks and service marks of MFMER.
 *
 * Except as contained in the copyright notice above, or as used to identify 
 * MFMER as the author of this software, the trade names, trademarks, service
 * marks, or product names of the copyright holder shall not be used in
 * advertising, promotion or otherwise in connection with this software without
 * prior written authorization of the copyright holder.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.mayo.qdm.cem.dao;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * The main CouchDb REST Client.
 *
 * @author <a href="mailto:kevin.peterson@mayo.edu">Kevin Peterson</a>
 */
public class CouchDbClient {
	
	protected final Logger log = Logger.getLogger(getClass());

	private final ObjectMapper objectMapper = new ObjectMapper();

	private String couchDbUrl;
	
	private static final String DEFAULT_URL = "http://10.148.2.153:5984";
	
	private static final String COUCH_DB_PASSWORD_ENV = "COUCHDB_PASSWORD";
	
    private final static String KEYS_PARAM = "keys";
    
    private final static String USERNAME = "htpuser";
    private String password;
    
	/**
	 * Instantiates a new couch db client.
	 */
	protected CouchDbClient() {
		this(DEFAULT_URL);
	}

	/**
	 * Instantiates a new couch db client.
	 *
	 * @param couchDbUrl the couch db url
	 */
	protected CouchDbClient(String couchDbUrl) {
		super();
		this.couchDbUrl = couchDbUrl;
		
		String password = System.getProperty(COUCH_DB_PASSWORD_ENV);
		
		if(StringUtils.isBlank(password)){
			password = System.getenv(COUCH_DB_PASSWORD_ENV);
		}
		
		this.password = StringUtils.trim(password);
	}

	/**
	 * Query view.
	 *
	 * @param view the view
	 * @param keys the keys
	 * @param params the params
	 * @return the map
	 */
	@SuppressWarnings("unchecked")
	protected Map<String, Object> queryView(String view, Collection<String> keys, Map<String,String> params) {

		try {
			URL url = new URL(this.couchDbUrl + "/" + view + this.paramsToString(params));
			log.debug("Calling: " + url.toString() + " Keys: " + keys);
			
			InputStream in;
			HttpURLConnection conn = (HttpURLConnection) this.getURLConnection(url, USERNAME, password);
			if(keys != null && keys.size() > 0){
				conn.setDoOutput(true);
				conn.setRequestMethod("POST");
				conn.setRequestProperty("Content-Type", "application/json");
		 
				Map<String,Collection<String>> map = new HashMap<String,Collection<String>>();
				map.put(KEYS_PARAM, keys);
				
				StringWriter sw = new StringWriter();
				this.objectMapper.writeValue(sw, map);
				
				OutputStream os = conn.getOutputStream();
				os.write(sw.toString().getBytes());
				os.flush();
				
				in = conn.getInputStream();
			} else {
				in = conn.getInputStream();
			}

			Map<String, Object> returnMap =  (Map<String, Object>) this.objectMapper.readValue(in, HashMap.class);
		
			return returnMap;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private URLConnection getURLConnection(
			URL url, 
			String username,
			String password) {
		String userPassword = username + ":" + password;
		String encoding = new sun.misc.BASE64Encoder().encode(userPassword.getBytes());
		URLConnection uc;
		try {
			uc = url.openConnection();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

        if(password != null){
		    uc.setRequestProperty("Authorization", "Basic " + encoding);
        }

		return uc;
	}
	
	/**
	 * Params to string.
	 *
	 * @param params the params
	 * @return the string
	 */
	private String paramsToString(Map<String, String> params) {
		String queryString = "";

		if (params != null) {
			queryString += "?";
			Iterator<String> itr = params.keySet().iterator();

			while (itr.hasNext()) {
				String key = itr.next();
				queryString += key + "=" + params.get(key);
				if (itr.hasNext()) {
					queryString += "&";
				}
			}
		}

		return queryString;
	}

	/**
	 * Gets the couch db url.
	 *
	 * @return the couch db url
	 */
	public String getCouchDbUrl() {
		return couchDbUrl;
	}

	/**
	 * Sets the couch db url.
	 *
	 * @param couchDbUrl the new couch db url
	 */
	public void setCouchDbUrl(String couchDbUrl) {
		this.couchDbUrl = couchDbUrl;
	}

	/**
	 * Gets the object mapper.
	 *
	 * @return the object mapper
	 */
	public ObjectMapper getObjectMapper() {
		return objectMapper;
	}

}
