package start;

import java.util.Properties;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;


public class ReadFromKafka {

	public static void main(String[] args) throws Exception {
		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// parse user parameters
		Properties properties = new Properties();
	    properties.setProperty("bootstrap.servers", "localhost:9092");
	    properties.setProperty("group.id", "flink_consumer");

	DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer<>("twitter_tweets", new SimpleStringSchema(), properties));

		// print() will write the contents of the stream to the TaskManager's standard out stream
		// the rebelance call is causing a repartitioning of the data so that all machines
		// see the messages (for example in cases when "num kafka partitions" < "num flink operators"
		messageStream.map(new MapFunction<String, String>() {
			private static final long serialVersionUID = -6867736771747690202L;

			@Override
			public String map(String value) throws Exception {
				return "readind tweets from Kafka to Flink: " + value;
			}
		}).rebalance().print();

		env.execute();
	}
}

