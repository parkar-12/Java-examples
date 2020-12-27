import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;


public class KafkaReader {
    private Queue dataQueue;
    private Properties kafkaProps;
    private String topic;
    public String isAutoCommitEnabled;
    public KafkaReader(KafkaProperties properties, Queue dataQueue,String topic,String isAutoCommitEnabled){
        this.dataQueue=dataQueue;
        this.kafkaProps=properties.getKafkaReaderProps(isAutoCommitEnabled);
        this.topic=topic;
        this.isAutoCommitEnabled=isAutoCommitEnabled;
    }
    public void read(){
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProps);
        consumer.subscribe(Collections.singletonList(topic));
        JsonParser jsonParser = new JsonParser();
        if(isAutoCommitEnabled.equals("false")) {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        long offset = record.offset();
                        String jsonString = record.value();
                        try {
                            JsonObject row = jsonParser.parse(jsonString).getAsJsonObject();
                            if (row == null) {
                                System.out.println("");
                            } else {
                                dataQueue.add(row);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            } finally {
                consumer.close();
            }
        }
        else{
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        long offset = record.offset();
                        String jsonString = record.value();
                        try {
                            JsonObject row = jsonParser.parse(jsonString).getAsJsonObject();
                            if (row == null) {
                                System.out.println("");
                            } else {
                                dataQueue.add(row);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    consumer.commitAsync();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
            finally {
                try {
                    consumer.commitSync();
                } finally {
                    consumer.close();
                }
            }
        }
    }
}
