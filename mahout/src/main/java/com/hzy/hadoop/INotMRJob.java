/**
 * 
 */
package com.hzy.hadoop;

import java.util.Map;

/**
 * �ύ��MR����Ļ���
 * @author fansy
 * @date 2015��8��5��
 */
public interface INotMRJob {

	public void setArgs(String[] args);
	
	public Map<String,Object> runJob();
}
