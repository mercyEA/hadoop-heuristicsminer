package org.apache.mahout.heuristicsminer.cleanse.test;

import java.io.IOException;
import junit.framework.Assert;
import org.apache.mahout.heuristicsminer.cleanse.MercyCSVCleanseMapper;
import org.junit.Test;


public class CleanseTest {
	private String test_1 = "132421321312,\"wqewrqewrqwerqwe\",\"234324324324\",\"ssdfsdf,sdfsdfsd\",3425345";
	private String test_1_cleansed = "132421321312\twqewrqewrqwerqwe\t234324324324\tssdfsdf,sdfsdfsd\t3425345";
	private String test_2 = "132421321312,\"wqewrqewrqwerqwe\",\"234324324324\",\"ssdfsdfsdfsdfsd";
	private String test_2_cleansed = "132421321312\twqewrqewrqwerqwe\t234324324324\tssdfsdfsdfsdfsd";
	private String test_3 = " 5398207200.043213,'PRDAOK-53084702-12010','GDI',         90080201,'90080201'";
	private String test_3_cleansed = "5398207200.043213\tPRDAOK-53084702-12010\tGDI\t90080201\t90080201";
	
	@Test
	public void testCommaEncoding1() throws IOException {
	      Assert.assertEquals(test_1_cleansed, MercyCSVCleanseMapper.formatTSV(test_1));		
	}

	@Test
	public void testMissingEnclosing2() throws IOException {
	      Assert.assertEquals(test_2_cleansed, MercyCSVCleanseMapper.formatTSV(test_2));		
	}

	@Test
	public void testPadding() throws IOException {
	      Assert.assertEquals(test_3_cleansed, MercyCSVCleanseMapper.formatTSV(test_3));		
	}
	
	
}
