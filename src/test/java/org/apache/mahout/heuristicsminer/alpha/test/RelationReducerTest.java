package org.apache.mahout.heuristicsminer.alpha.test;

import junit.framework.Assert;

import org.apache.mahout.heuristicsminer.alpha.RelationReducer;
import org.junit.Test;


public class RelationReducerTest {

	@Test
	public void testComputeDependencyRelation() {
		Assert.assertEquals(0.3125, RelationReducer.computeDependencyRelation(10L, 5L));
	}

	@Test
	public void testComputeLengthOneLoopRelation() {
		Assert.assertEquals(0.875, RelationReducer.computeLengthOneLoopRelation(7L));
	}

	@Test
	public void testComputeLengthTwoLoopRelation() {
		Assert.assertEquals(0.9375, RelationReducer.computeLengthTwoLoopRelation(10L, 5L));		
	}

	
}
