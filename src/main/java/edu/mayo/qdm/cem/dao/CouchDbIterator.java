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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * An Iterator for paginating CouchDB output.
 *
 * @param <T> the generic type
 * @author <a href="mailto:kevin.peterson@mayo.edu">Kevin Peterson</a>
 */
public class CouchDbIterator<T> implements Iterable<T> {

    private final static String LIMIT_PARAM = "limit";
    private final static String SKIP_PARAM = "skip";
	
	private CouchDbClient couchDbClient;
	private Transformer<T> transformer;
	private PageDecorator<T> pageDecorator;
	private List<String> keys;
	private String view;
	private Map<String,String> parameters;
	
	/**
	 * Instantiates a new couch db iterator.
	 *
	 * @param view the view
	 * @param keys the keys
	 * @param parameters the parameters
	 * @param transformer the transformer
	 * @param pageDecorator the page decorator
	 * @param couchDbClient the couch db client
	 */
	protected CouchDbIterator(
			String view, 
			List<String> keys,
			Map<String,String> parameters,
			Transformer<T> transformer, 
			PageDecorator<T> pageDecorator,
			CouchDbClient couchDbClient){
		super();
		this.view = view;
		this.keys = keys;
		this.couchDbClient = couchDbClient;
		this.transformer = transformer;
		this.pageDecorator = pageDecorator;
		this.parameters = parameters;
	}
	
	/**
	 * The Interface Transformer.
	 *
	 * @param <T> the generic type
	 * @author <a href="mailto:kevin.peterson@mayo.edu">Kevin Peterson</a>
	 */
	protected interface Transformer<T>{
		
		/**
		 * Transform.
		 *
		 * @param row the row
		 * @return the t
		 */
		public T transform(Map<String, Object> row);
		
	}
	
	/**
	 * The Interface PageDecorator.
	 *
	 * @param <T> the generic type
	 * @author <a href="mailto:kevin.peterson@mayo.edu">Kevin Peterson</a>
	 */
	protected interface PageDecorator<T>{
		
		/**
		 * Decorate.
		 *
		 * @param page the page
		 * @return the list
		 */
		public List<T> decorate(List<T> page);
		
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<T> iterator() {
		
		return new AbstractPageableIterator<T,Map<String,Object>>(){
			
			private static final long serialVersionUID = -5398591025205734109L;

			@SuppressWarnings("unchecked")
			@Override
			protected List<? extends Map<String,Object>> doPage(int currentPosition, int pageSize) {
				Map<String,String> params = getLimitSkipMap(pageSize, currentPosition);
				
				if(parameters != null){
					params.putAll(parameters);
				}
				
				Object rows = MapUtils.get("rows", 
						couchDbClient.queryView(
								view, 
								keys,
								params));
				
				return (List<? extends Map<String, Object>>) rows;
			}

			@Override
			protected T transform(Map<String,Object> inputItem) {
				return transformer.transform(inputItem);
			}
			
			@Override
			protected List<T> decoratePage(List<T> page) {
				if(pageDecorator != null){
					return pageDecorator.decorate(page);
				} else {
					return super.decoratePage(page);
				}
			}
	
		};
	}
	
	/**
	 * Gets the limit skip map.
	 *
	 * @param limit the limit
	 * @param skip the skip
	 * @return the limit skip map
	 */
	private Map<String,String> getLimitSkipMap(int limit, int skip){
		Map<String,String> map = new HashMap<String,String>();
		map.put(LIMIT_PARAM, Integer.toString(limit));
		map.put(SKIP_PARAM, Integer.toString(skip));
		
		return map;
	}

}
