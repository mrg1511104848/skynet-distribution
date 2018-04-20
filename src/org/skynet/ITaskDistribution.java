package org.skynet;


public interface ITaskDistribution {
	public static final String taskCollectionName = "url_task_list1";
	public boolean distribution(String tarMechine , String taskName,String url);
	public void runTaskUrl();
}
