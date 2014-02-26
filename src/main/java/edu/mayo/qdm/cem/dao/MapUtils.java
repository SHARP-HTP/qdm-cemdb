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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * Utilities for accessing JSON objects as a Map.
 *
 * @author <a href="mailto:kevin.peterson@mayo.edu">Kevin Peterson</a>
 */
public class MapUtils {
	
	protected static Pattern BRACKETS_MATCHER = Pattern.compile("(.*)\\[([0-9]*)\\]");

	/**
	 *
	 * @param keyString the key string
	 * @param map the map
	 * @return the object
	 */
	static Object get(String keyString, Map<?,?> map){
		String[] tokens = StringUtils.split(keyString, '.');
		if(tokens.length == 1){
			return getFromMap(tokens[0], map);
		} else {
			String dotPath = StringUtils.join(
					Arrays.copyOfRange(tokens, 1, tokens.length), ".");
			return get(dotPath, (Map<?,?>) getFromMap(tokens[0], map));	
		}
	}
	
	static boolean keyExists(String keyString, Map<?,?> map){
		try {
			get(keyString, map);
		} catch (KeyNotFoundException e) {
			return false;
		}
		
		return true;
	}
	
	/**
	 *
	 * @param key the key
	 * @param map the map
	 * @return the from map
	 */
	static Object getFromMap(String key, Map<?,?> map){
		Matcher matcher = BRACKETS_MATCHER.matcher(key);
		
		if(matcher.find()){
			String adjustedKey = matcher.group(1);
			int index = Integer.parseInt(matcher.group(2));
			
			if(!map.containsKey(adjustedKey)){
				throw getNotFoundException(adjustedKey, map);
			}
			return ((List<?>) map.get(adjustedKey)).get(index);
		} else {
			if(!map.containsKey(key)){
				throw getNotFoundException(key, map);
			}
			return map.get(key);
		}
	}
	
	/**
	 * Gets the not found exception.
	 *
	 * @param key the key
	 * @param map the map
	 * @return the not found exception
	 */
	private static KeyNotFoundException getNotFoundException(String key, Map<?,?> map){
		return new KeyNotFoundException("Key: " +  key + " not found. Valid keys are: " + map.keySet());
	}
	
	protected static class KeyNotFoundException extends RuntimeException {

		private static final long serialVersionUID = 2925733098865397476L;
		
		private KeyNotFoundException(String message){
			super(message);
		}
	}
}
