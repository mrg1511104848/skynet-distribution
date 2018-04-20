package org.skynet;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.bson.Document;
import org.bson.types.Binary;
import org.skynet.frame.util.mongo.MongoUtil;
import org.skynet.frame.util.zlib.ZLib;

import com.mongodb.BasicDBObject;

public class TaskUtil {
	private final static String TASK_COLLECTION_NAME = "url_task_list1";
	public static String getFieldValue(String field,String value){
		String dataSource1 = getFieldValue(TASK_COLLECTION_NAME,field, value);
		String dataSource2 = getFieldValue("url_task_list",field, value);
		return dataSource1 == null ?  dataSource2 : dataSource1;
	}
	public static String getFieldValue(String collectionName,String field,String value){
		Document doc = MongoUtil.getCollection(collectionName).find(new BasicDBObject(field,value)).limit(1).first();
		if(doc == null) return null;
		Map<String, Object> runResult = (Map<String, Object>)doc.get("runResult");
		if(runResult == null){
			return null;
		}
		try {
			byte[] bytes = ZLib.decompress(((Binary) runResult.get("html"))
					.getData());
			String html = new String((bytes), MongoUtil.guessEncoding(bytes));
			return html;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	}
	public static String getHtmlByUrl(String url){
		return getFieldValue("url", url);
	}
	public static void main(String[] args) {
		String result = getHtmlByUrl("https://db.yaozh.com/instruct?name=枫蓼肠胃康合剂&p=1&pageSize=20");
		System.out.println(result);
	}
}
