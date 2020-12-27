import java.util.Properties;

public class KafkaProperties {
    private Properties kafkaProp=new Properties();
    private String bootstrap_servers;
    private String topic;

    public KafkaProperties(String bootstrap_servers,String topic){
        this.bootstrap_servers=String.join(",",bootstrap_servers);
        this.topic=topic;
    }
    public void setKafkaProps(Properties kafkaProps) {
        this.kafkaProp = kafkaProps;
    }
    public Properties getKafkaWriterProps() {
        kafkaProp.put("bootstrap.servers",this.bootstrap_servers);
        kafkaProp.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProp.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        return this.kafkaProp;
    }
    public Properties getKafkaReaderProps(String isAutoCommitEnabled){
        kafkaProp.put("bootstrap.servers",this.bootstrap_servers);
        kafkaProp.put("key.serializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProp.put("value.serializer","org.apache.kafka.common.serialization.StringDeserializer");
        if(isAutoCommitEnabled.equals("False")){
            kafkaProp.put("enable.auto.commit","false");
        }
        return this.kafkaProp;
    }
}
