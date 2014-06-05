/**
 * 
 */
package com.fmarket.ifin.sql.util;

/**
 * @author holly
 * 
 */
public interface SequenceDAO {
	public void lockSequenceByName(String seqName);

	public int addSequence(String seqName, int steps);

	public long getCurrentValue(String seqName);

	public void reInitSequence(String seqName);
}
