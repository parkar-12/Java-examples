import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Queue;

public class KafkaWriter{
    private Properties kafkaProp;
    private String topic;
    private Queue dataQueue;
    public KafkaWriter(KafkaProperties properties,Queue dataQueue){
        this.kafkaProp=properties.getKafkaWriterProps();
        this.dataQueue=dataQueue;
    }
    public void write(String topic,String type){
        this.topic=topic;
        KafkaProducer producer = new KafkaProducer<String, String>(kafkaProp);
        while(!dataQueue.isEmpty()) {
            String jsonString= (String) dataQueue.poll();
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,jsonString);
            if (type.equals("Fire_and_forget")) {
                try {
                    producer.send(record);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if (type.equals("Synchronus")) {
                try {
                    producer.send(record).get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if (type.equals("Asynchronous")) {
                try {
                    producer.send(record, new ProducerCallback());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
