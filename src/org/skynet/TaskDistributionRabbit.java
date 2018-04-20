package org.skynet;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.skynet.frame.config.Config;
import org.skynet.frame.util.date.DateUtil;
import org.skynet.frame.util.http.IpUtil;
import org.skynet.frame.util.mongo.MongoUtil;
import org.skynet.frame.util.zlib.ZLib;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
public class TaskDistributionRabbit implements ITaskDistribution{
	private static Logger log = Logger.getLogger(TaskDistribution.class.getName());
	private static final String SPLIT = "~~~";
	public static void main(String[] args) {
		new TaskDistributionRabbit().distribution("111", "呵呵", "http://baidu233444.com");
		new TaskDistributionRabbit().runTaskUrl();
	}
	@Override
	public boolean distribution(String tarMechine , String taskName,String url){
		if(StringUtils.isBlank(tarMechine)){
			throw new NullPointerException("TarMechine must not be blank!");
		}
		if(StringUtils.isBlank(taskName)){
			throw new NullPointerException("TaskName must not be blank!");
		}
		if(StringUtils.isBlank(url)){
			throw new NullPointerException("Url must not be blank!");
		}
		Connection connection = null;
		com.rabbitmq.client.Channel channel = null;
		String TASK_QUEUE_NAME = tarMechine;
		String message = taskName+SPLIT+url;
		try {
			log.info(String.format("NewTask[%s] '%s'", tarMechine,message));
			
			Map<String, Object> query = new HashMap<String, Object>();
			query.put("taskName", taskName);
			query.put("url", url);
			if(MongoUtil.getCount(taskCollectionName, query)>0){
				log.info(String.format("exists Task[%s] '%s'", tarMechine,message));
				return false;
			}
			
			ConnectionFactory factory=new ConnectionFactory();
			factory.setHost(Config.MQ_IP);
			factory.setPort(Config.MQ_PORT);
			factory.setUsername(Config.MQ_USERNAME);
			factory.setPassword(Config.MQ_PASSWORD);
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(TASK_QUEUE_NAME,true,false,false,null);
		    channel.basicPublish("",TASK_QUEUE_NAME,
		            MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes());
		    
			SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.DATE_FORMAT_01);
			Map<String, Object> taskMap = new HashMap<String, Object>();
			taskMap.put("taskName", taskName);
			taskMap.put("url", url);
			taskMap.put("st", 0);
			taskMap.put("toMechine", tarMechine);
			taskMap.put("distributionTime", sdf.format(new Date()));
			MongoUtil.saveDoc(taskCollectionName, taskMap);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally{
			try {
				if(channel!=null)
					channel.close();
				if(connection!=null)
					connection.close();
			} catch (IOException | TimeoutException e) {
				e.printStackTrace();
			}
		}
		return true;
	}
	
	public static List<String> getMechineList(){
		List<String> mechineList = Config.MECHINE_LIST;
		return mechineList;
	}
	static String ip ; 
	static{
		if(StringUtils.isBlank(ip)){
			ip = IpUtil.getOutterIp();
			if(StringUtils.isBlank(ip)){
				//throw new NullPointerException("getV4IP err !");
			}
		}
	}
	@Override
	public void runTaskUrl(){
		log.info("开始等待任务...");
		try {
			final ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(Config.MQ_IP);
			factory.setPort(Config.MQ_PORT);
			factory.setUsername(Config.MQ_USERNAME);
			factory.setPassword(Config.MQ_PASSWORD);
			Connection connection = factory.newConnection();
			final com.rabbitmq.client.Channel channel = connection.createChannel();

			channel.queueDeclare(ip, true, false, false, null);
			log.info(String.format("Worker[%s]  Waiting for messages",ip));

			//每次从队列获取的数量
			channel.basicQos(1);

			final Consumer consumer = new DefaultConsumer(channel) {
			    @Override
				public void handleDelivery(String consumerTag, Envelope envelope,
						BasicProperties properties, byte[] body) throws IOException {
			    	String message = new String(body, "UTF-8");
			    	log.info(String.format("Worker[%s]  Received '%s'",ip,message));
			        try {
			            doWork(message);
			        }catch (Exception e){
			            channel.abort();
			        }finally {
			            log.info(String.format("Worker[%s][%s] Done",ip,message));
			            channel.basicAck(envelope.getDeliveryTag(),false);
			        }
				}
			};
			boolean autoAck=false;
			//消息消费完成确认
			channel.basicConsume(ip, autoAck, consumer);
		} catch (Exception e) {
			e.printStackTrace();
			log.error(String.format("[worker] %s 执行任务时出现异常",ip));
		}
	}
	private static void doWork(String task) {
        try {
        	log.info("开始执行任务"+task);
        	SimpleDateFormat sdf = new SimpleDateFormat(DateUtil.DATE_FORMAT_01);
        	String[] taskInfo  = task.split(SPLIT);
			TaskRun taskRun = null;
			taskRun = new TaskRun();
			taskRun.setCurrHref(taskInfo[1]);
			taskRun.test();
			
			
			
			Map<String, Object> queryMap = new HashMap<String, Object>();
			queryMap.put("toMechine", ip);
			queryMap.put("url", taskInfo[1]);
			
			Map<String, Object> updateMap = new HashMap<String, Object>();
			
			updateMap.put("st", 2);
			updateMap.put("runTaskTime", sdf.format(new Date()));
			
			if(taskRun.getHtml()==null){
				updateMap.put("st", -1);
			}else{
				Map<String, Object> runResult = new HashMap<String, Object>();
				runResult.put("html", ZLib.compress(taskRun.getHtml().getBytes()));
				updateMap.put("runResult", runResult);
				updateMap.put("st", 2);
			}
			MongoUtil.updateDoc(taskCollectionName, queryMap, updateMap);
            Thread.sleep(1000); // 暂停1秒钟
        } catch (InterruptedException _ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
