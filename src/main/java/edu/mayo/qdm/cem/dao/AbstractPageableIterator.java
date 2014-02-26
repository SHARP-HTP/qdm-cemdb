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


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * The Class AbstractPageableIterator.
 *
 * @param <T> the generic type
 * @param <I> the generic type
 * @author <a href="mailto:kevin.peterson@mayo.edu">Kevin Peterson</a>
 */
public abstract class AbstractPageableIterator<T,I> implements Iterator<T>, Iterable<T>, Serializable{

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -5398591025205732109L;

	/** The DEFAUL t_ pag e_ size. */
	private static int DEFAULT_PAGE_SIZE = 1000;
	
	/** The cache. */
	private List<? extends T> cache = new ArrayList<T>();
	
	/** The page size. */
	private int pageSize;
	
	/** The global position. */
	private int globalPosition = 0;
	
	/** The in cache position. */
	private int inCachePosition = 0;
	
	/** The pager. */
	private Pager<T,I> pager;

	private boolean isExhausted = false;
	
	/**
	 * Instantiates a new abstract pageable iterator.
	 */
	protected AbstractPageableIterator(){
		this(DEFAULT_PAGE_SIZE);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<T> iterator() {
		return this;
	}

	/**
	 * Instantiates a new abstract pageable iterator.
	 * 
	 * @param pageSize the page size
	 */
	public AbstractPageableIterator(int pageSize){
		this.pageSize = pageSize;
		
		this.pager = new Pager<T,I>();
	}
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		if(isExhausted){
			return false;
		}
		
		pageIfNecessary();
		
		if(cache == null || cache.size() == 0) {
			isExhausted = true;
			return false;
		}
		
		int cacheSize = cache.size();
		
		boolean hasNext = inCachePosition < cacheSize;
		
		isExhausted = !hasNext;
		
		return hasNext;
		
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public T next() {
		pageIfNecessary();
		
		T returnItem = cache.get( inCachePosition );
		
		globalPosition++;
		inCachePosition++;
		
		
		return returnItem;
	}
	
	/**
	 * Transform.
	 *
	 * @param inputItem the input item
	 * @return the t
	 */
	protected abstract T transform(I inputItem);
	
	/**
	 * Page if necessary.
	 */
	protected void pageIfNecessary() {
		if(isPageNeeded()) {
			page();
		}
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
	
	/**
	 * Checks if is page needed.
	 * 
	 * @return true, if is page needed
	 */
	protected boolean isPageNeeded() {
		boolean page = inCachePosition > ( cache.size() - 1 );
		return page;
	}
	
	/**
	 * Page.
	 */
	protected final void page() {
		cache = doExecutePage();

		inCachePosition = 0;
	}
	
	/**
	 * Do execute page.
	 * 
	 * @return the list<? extends t>
	 */
	protected List<T> doExecutePage(){
		List<? extends I> page;
		try {
			page = this.pager.doPage(this, globalPosition, pageSize);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		List<T> transformedList = new ArrayList<T>();
		for(I item : page){
			transformedList.add(this.transform(item));
		}

		return this.decoratePage(transformedList);
	}
	
	/**
	 * Allow subclasses to decorate an entire page at at time.
	 *
	 * @param page the page
	 * @return the list<? extends i>
	 */
	protected List<T> decoratePage(List<T> page) {
		return page;
	}
	
	/**
	 * Returns a page of results.
	 * 
	 * NOTE: 'pageSize' is not binding -- it is the suggested page size.
	 * Implementing classes may return more or less than the suggested
	 * 'pageSize' parameter, although it is generally recommended to abide
	 * by the 'pageSize' parameter when possible.
	 * 
	 * A null or empty list returned from this method will signify
	 * that the underlying results are exhausted and paging should halt.
	 * 
	 * @param currentPosition the current position
	 * @param pageSize the page size
	 * 
	 * @return the list<? extends t>
	 */
	protected abstract List<? extends I> doPage(int currentPosition, int pageSize);

	/**
	 * The Class Pager.
	 *
	 * @param <T> the generic type
	 * @param <I> the generic type
	 * @author <a href="mailto:kevin.peterson@mayo.edu">Kevin Peterson</a>
	 */
	public static class Pager<T,I> implements Serializable {

		/** The Constant serialVersionUID. */
		private static final long serialVersionUID = 6142588013131141095L;

		/**
		 * Instantiates a new pager.
		 */
		public Pager() {
			super();
		}
		
		/**
		 * Do page.
		 * 
		 * @param abstractPageableIterator the abstract pageable iterator
		 * @param currentPosition the current position
		 * @param pageSize the page size
		 * 
		 * @return the list<? extends t>
		 */
		public List<? extends I> doPage(AbstractPageableIterator<T,I> abstractPageableIterator, int currentPosition, int pageSize){
			List<? extends I> returnList = abstractPageableIterator.doPage(currentPosition, pageSize);
			
			return returnList;
		}
	}

	/**
	 * Gets the page size.
	 * 
	 * @return the page size
	 */
	protected int getPageSize() {
		return pageSize;
	}

	/**
	 * Gets the global position.
	 * 
	 * @return the global position
	 */
	protected int getGlobalPosition() {
		return globalPosition;
	}	
}