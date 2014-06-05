/**
 * 
 */
package com.fmarket.ifin.sql.util;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

/**
 * @author holly
 * 
 */
public class SequenceUtilTest {
	SequenceUtil seqUtil;

	@Before
	public void setup() throws Exception {
		seqUtil = new SequenceUtil();
		Map<String, String> name2seq = new HashMap<String, String>();
		name2seq.put("com.fmarket.ifin.sql.util.TestVO", "TEST_SEQ");
		seqUtil.setTable2sequence(name2seq);
		seqUtil.afterPropertiesSet();
	}

	@Test
	public void testSeq() {
		final AtomicLong c = new AtomicLong(0);
		final long startTime = System.currentTimeMillis();

		final long period = 1000 * 3;
//		seqUtil.generateKey(TestVO.class);
		for (;;) {
			if (System.currentTimeMillis() - startTime > period) {
				break;
			}
			Runnable r = new Runnable() {

				public void run() {
					if (System.currentTimeMillis() - startTime > period) {
						return;
					}
					seqUtil.generateKey(TestVO.class);
//					System.out.println(seqUtil.generateKey(TestVO.class));
					c.incrementAndGet();

				}
			};
			new Thread(r).start();

		}
		System.out.println("time: "
				+ ((System.currentTimeMillis() - startTime) / 1000) + " count:"
				+ c.get());
	}
}
