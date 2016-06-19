package com.kevin.kafkademo;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class KafkaProducer extends Thread{

	private String topic;  
    
    public KafkaProducer(String topic){  
        super();  
        this.topic = topic;  
    }  
      
      
    @Override  
    public void run() {  
        Producer producer = createProducer();  
        int i=0;  
        while(true){
        	String msg = "message: " + i++ ;
        	System.out.println("生产消息:" + msg);
            producer.send(new KeyedMessage<Integer, String>(topic, msg));            
            try {  
                TimeUnit.SECONDS.sleep(1);  
            } catch (InterruptedException e) {  
                e.printStackTrace();  
            }  
        }  
    }  
  
    private Producer createProducer() {  
        Properties properties = new Properties();  
        properties.put("zookeeper.connect", "192.168.1.108:2181");//声明zk  
        //配置value的序列化类
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        //配置key的序列化类
        properties.put("key.serializer.class", "kafka.serializer.StringEncoder");
        properties.put("metadata.broker.list", "192.168.1.108:9092");// 声明kafka broker
        //request.required.acks
        //0, which means that the producer never waits for an acknowledgement from the broker (the same behavior as 0.7). This option provides the lowest latency but the weakest durability guarantees (some data will be lost when a server fails).
        //1, which means that the producer gets an acknowledgement after the leader replica has received the data. This option provides better durability as the client waits until the server acknowledges the request as successful (only messages that were written to the now-dead leader but not yet replicated will be lost).
        //-1, which means that the producer gets an acknowledgement after all in-sync replicas have received the data. This option provides the best durability, we guarantee that no messages will be lost as long as at least one in sync replica remains.
        //properties.put("request.required.acks","-1");
        return new Producer<Integer, String>(new ProducerConfig(properties));  
     }  
      
      
    public static void main(String[] args) {  
        new KafkaProducer("test_topic").start();// 使用kafka集群中创建好的主题 test   
          
    }  
}
