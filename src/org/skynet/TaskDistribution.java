package org.skynet;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.skynet.frame.util.date.DateUtil;
import org.skynet.frame.util.http.IpUtil;
import org.skynet.frame.util.mongo.MongoUtil;
import org.skynet.frame.util.zlib.ZLib;

public class TaskDistribution implements ITaskDistribution{
	private static Logger log = Logger.getLogger(TaskDistribution.class.getName());
	@Override
	public boolean distribution(String tarMechine , String taskName,String url){
		SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.DATE_FORMAT_01);
		if(StringUtils.isBlank(taskName))
			throw new NullPointerException();
		if(StringUtils.isBlank(url))
			throw new NullPointerException();
		
		Map<String, Object> query = new HashMap<String, Object>();
		query.put("taskName", taskName);
		query.put("url", url);
		query.put("toMechine", tarMechine);
		if(MongoUtil.getCount(taskCollectionName, query)>0){
			log.info("任务已存在！");
			return false;
		}
		Map<String, Object> taskMap = new HashMap<String, Object>();
		taskMap.put("taskName", taskName);
		taskMap.put("url", url);
		taskMap.put("st", 0);
		taskMap.put("toMechine", tarMechine);
		taskMap.put("distributionTime", sdf.format(new Date()));
		MongoUtil.saveDoc(taskCollectionName, taskMap);
		return true;
	}
	
	static String ip ; 
	static{
		if(StringUtils.isBlank(ip)){
			ip = IpUtil.getOutterIp();
			if(StringUtils.isBlank(ip)){
				throw new NullPointerException("getV4IP err !");
			}
		}
	}
	@Override
	public void runTaskUrl(){
		
		while (true) {
			SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.DATE_FORMAT_01);
			Map<String, Object> query = new HashMap<String, Object>();
			Map<String, Object> update = new HashMap<String, Object>();
			query.put("toMechine", ip);
			query.put("st", 0);
			update.put("st", 1);
			update.put("getTaskTime", sdf.format(new Date()));
			Document doc = MongoUtil.findOneAndUpdate(taskCollectionName, query, update);
			if(doc == null){
				log.info("目前任务为空，正在等待任务...");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				continue;
			}
			String url = doc.getString("url");
			log.info("runTaskUrl:"+url);
			TaskRun taskRun = null;
			try {
				taskRun = new TaskRun();
				taskRun.setCurrHref(url);
				taskRun.test();
			} catch (Exception e) {
				continue;
			}
			
			
			Map<String, Object> queryMap = new HashMap<String, Object>();
			Map<String, Object> updateMap = new HashMap<String, Object>();
			queryMap.put("toMechine", ip);
			queryMap.put("url", url);
			
			updateMap.put("st", 2);
			updateMap.put("runTaskTime", sdf.format(new Date()));
			if(taskRun.getHtml()==null){
				updateMap.put("st", -1);
			}else{
				Map<String, Object> runResult = new HashMap<String, Object>();
				runResult.put("html", ZLib.compress(taskRun.getHtml().getBytes()));
				updateMap.put("runResult", runResult);
			}
			MongoUtil.updateDoc(taskCollectionName, queryMap, updateMap);
		}
		
	}
	public static void main(String[] args) {
//		new TaskDistribution().distribution( "medicine","https://www.baidu.com");
//		new TaskDistribution().runTaskUrl();
	}
	
}
