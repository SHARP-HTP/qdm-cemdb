package edu.mayo.qdm.cem.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.regex.Matcher;

import org.junit.Test;

public class MapUtilsTest {

	@Test
	public void TestMatcherMatches() {
		assertTrue(MapUtils.BRACKETS_MATCHER.matcher("test[0]").find());
	}
	
	@Test
	public void TestMatcherMatchesNoMatch() {
		assertFalse(MapUtils.BRACKETS_MATCHER.matcher("test").find());
	}
	
	@Test
	public void TestMatcherGroupCount() {
		assertEquals(2,MapUtils.BRACKETS_MATCHER.matcher("test[1]").groupCount());
	}
	
	@Test
	public void TestMatcherGroup1() {
		Matcher m = MapUtils.BRACKETS_MATCHER.matcher("test[1]");
		assertTrue(m.find());
		assertEquals("test",m.group(1));
	}
	
	@Test
	public void TestMatcherGroup2() {
		Matcher m = MapUtils.BRACKETS_MATCHER.matcher("test[1]");
		assertTrue(m.find());
		assertEquals("1",m.group(2));
	}

}
