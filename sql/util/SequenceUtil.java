/**
 * 
 */
package com.fmarket.ifin.sql.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ibatis.session.SqlSession;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 获取sequence工具类
 * 
 * @author holly
 * 
 */
public class SequenceUtil {
	private static final int DEFAULT_LENGTH = 18;

	private static final String DEFAULT_DATE_FORMAT = "yyyyMMdd";

	private static final Log LOGGER = LogFactory.getLog(SequenceUtil.class);

	private ConcurrentHashMap<Class<?>, String> sequenceMapping = new ConcurrentHashMap<Class<?>, String>();

	private ConcurrentHashMap</* seq name */String, SequenceGenerator> cache = new ConcurrentHashMap<String, SequenceGenerator>();

	private Map</* VO class name */String, /* sequence name */String> table2sequence;

	private TransactionTemplate transactionTemplate;

	private volatile boolean isReady = false;

	private int length = DEFAULT_LENGTH;

	private SqlSession sqlSession;

	public Long generateKey(Class<?> vo) {
		while (!isReady) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
			}
		}

		String seqName = sequenceMapping.get(vo);
		if (StringUtils.isEmpty(seqName)) {
			return 0l;
		}

		SequenceGenerator generator = cache.get(seqName);
		if (generator == null) {
			return 0l;
		}

		return formatKey(generator.getKey());
	}

	/**
	 * @param key
	 * @return
	 */
	private Long formatKey(long key) {
		StringBuilder sb = new StringBuilder(DEFAULT_LENGTH);
		SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_DATE_FORMAT);
		sb.append(sdf.format(new Date()));
		sb.append(StringUtils.leftPad(String.valueOf(key), DEFAULT_LENGTH
				- DEFAULT_DATE_FORMAT.length(), "0"));
		return Long.valueOf(sb.toString());
	}

	/**
	 * 
	 * first init from db when start up
	 */
	public Long getCurrentValue(String seqName) {
		return 0l;
	}

	/**
	 * persist the current value to database
	 * 
	 * @param seqName
	 * @param currentValue
	 */
	public void updateDB(String seqName, Long currentValue) {

	}

	/**
	 * initialize the sequence in table if new sequence is not exist
	 */
	public void initDB() {

	}

	public void afterPropertiesSet() throws Exception {
		if (table2sequence == null || table2sequence.isEmpty()) {
			LOGGER.warn("table2sequence is empty");
			return;
		}

		for (Iterator<Map.Entry<String, String>> iterator = table2sequence
				.entrySet().iterator(); iterator.hasNext();) {
			Map.Entry<String, String> entry = iterator.next();
			sequenceMapping
					.put(Class.forName(entry.getKey()), entry.getValue());
			cache.put(entry.getValue(), new SequenceGenerator(entry.getValue(),
					null));
		}

		initDB();

		isReady = true;

	}

	public Map<String, String> getTable2sequence() {
		return table2sequence;
	}

	public void setTable2sequence(Map<String, String> table2sequence) {
		this.table2sequence = table2sequence;
	}

	public void setSqlSession(SqlSession sqlSession) {
		this.sqlSession = sqlSession;
	}

	public void setTransactionTemplate(TransactionTemplate transactionTemplate) {
		this.transactionTemplate = transactionTemplate;
	}

	public void setLength(int length) {
		this.length = length;
	}

	class SequenceGenerator {

		private int DEFAULT_CACHE_SIZE = 1000;

		private AtomicLong count = new AtomicLong(0);

		private AtomicLong currentValue = new AtomicLong(0);

		private Long maxValue = 99990000l;

		private ReentrantLock updateLock;

		private Condition notEmpty;

		private String seqName;

		public SequenceGenerator(String seqName, Long maxValue) {
			updateLock = new ReentrantLock();
			notEmpty = updateLock.newCondition();
			this.seqName = seqName;
			if (maxValue != null) {
				this.maxValue = maxValue;
			}
			currentValue.set(getCurrentValue(seqName));
			init();
		}

		public long getKey() {
			long myValue;
			long acquired = count.incrementAndGet();
			while (acquired > DEFAULT_CACHE_SIZE) {
				try {
					try {
						updateLock.lock();
						notEmpty.await();
					} finally {
						updateLock.unlock();
					}
				} catch (InterruptedException e) {
				}
				acquired = count.incrementAndGet();
			}

			myValue = currentValue.get() + acquired;
			if (acquired == DEFAULT_CACHE_SIZE) {
				init();
			}
			notifyNotEmpty();

			return myValue;
		}

		private void notifyNotEmpty() {
			try {
				updateLock.lock();
				notEmpty.signalAll();
			} finally {
				updateLock.unlock();
			}
		}

		private void init() {
			try {
				updateLock.lock();
				count.set(0);
				// from db, and set current
				long updatedCurrent = currentValue
						.addAndGet(DEFAULT_CACHE_SIZE);
				if (currentValue.get() > maxValue) {
					currentValue.set(0);
					updatedCurrent = 0l;
				}
				updateDB(seqName, updatedCurrent);
				notEmpty.signalAll();
			} finally {
				updateLock.unlock();
			}

		}
	}

}
