package start;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.mesos.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class TwitterPro {
@SuppressWarnings("deprecation")
public static void main(String[] args)throws Exception{
	StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();

	//props.setProperty(TwitterSource.CONSUMER_KEY, "rFj9JE01ulSnODViH8A42rTZA");
	//props.setProperty(TwitterSource.CONSUMER_SECRET, "5p9H6ybk5KuHqkLn0Z7PX48w9e78b243O0i8FhVSfJVMp18wFA");
	//props.setProperty(TwitterSource.TOKEN, "1333678850664906752-oR0ZX2Cadglk4q5iDjRqxllgvvl92c");
//	props.setProperty(TwitterSource.TOKEN_SECRET, "MgIX56Osf98DAigCeqR8dVYNRCyEbqwEqQQb2TbHhgw5n");
	Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "flink_consumer1");

DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer<>("flink-demo", new SimpleStringSchema(), properties));

DataStream<Tuple2<String,Integer>> Tweets=messageStream.map(new LocationCount())
		.keyBy(0)
		.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
		.sum(1);
Tweets.print();
env.execute("Twitter Location Count");
}
public static  class LocationCount implements MapFunction<String,Tuple2<String,Integer>>{

/**
 * 
 */
private static final long serialVersionUID = 1L;	
private transient ObjectMapper jsonParser;
public Tuple2<String, Integer> map(String value) throws Exception {
	if(jsonParser==null) {
		jsonParser=new ObjectMapper();
	}
   JsonNode jsonNode=jsonParser.readValue(value, JsonNode.class);
   String location=jsonNode.has("user") && jsonNode.get("user").has("location") ?(jsonNode.get("user").get("location")).toString():"unknown";
 return new Tuple2<String,Integer>(location,1);
}
}
}