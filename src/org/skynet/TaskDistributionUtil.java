package org.skynet;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.Binary;
import org.skynet.frame.config.Config;
import org.skynet.frame.util.mongo.MongoUtil;
import org.skynet.frame.util.zlib.ZLib;

import com.mongodb.BasicDBObject;

public class TaskDistributionUtil {
	private static Logger log = Logger.getLogger(TaskDistributionUtil.class
			.getName());
	public static List<String> mechineList = getMechineList();
	/**
	 * 随机分配任务
	 * 
	 * @param taskName
	 * @param url
	 * @return
	 */
	public static boolean distribution(ITaskDistribution taskDistribution,String taskName, String url) {
		int distributionIdx = new Random().nextInt(mechineList.size());
		boolean distributionResult = taskDistribution.distribution(
				mechineList.get(distributionIdx), taskName, url);
		if (!distributionResult) {
			log.error("任务重复");
			return false;
		}
		return true;
	}

	/**
	 * 根据 url 获取task html
	 * 
	 * @param url
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static String getHtml(String url) {
		int st = 0;
		String html = null;
		long startTime = System.currentTimeMillis();
		long endTime = System.currentTimeMillis();
		while (st == 0 || st == 1) {
			Document doc = MongoUtil
					.getCollection(ITaskDistribution.taskCollectionName)
					.find(new BasicDBObject("url", url)).limit(1).first();
			if (doc == null) {
				break;
			}
			st = doc.getInteger("st");
			if (st == 2) {
				
				Map<String, Object> runResult = (Map<String, Object>) doc
						.get("runResult");
				try {
					byte[] bytes = ZLib.decompress(((Binary) runResult
							.get("html")).getData());
					html = new String((bytes), MongoUtil.guessEncoding(bytes));
					return html;
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}
			endTime = System.currentTimeMillis();
			if (endTime - startTime > 7000) {
				log.warn(String.format(
						"[url] %s get html from task run > 7000 ms ! break",
						url));
				break;
			}
		}
		return null;
	}
	
	public static List<String> getMechineList(){
		List<String> mechineList = Config.MECHINE_LIST;
		return mechineList;
	}
}
