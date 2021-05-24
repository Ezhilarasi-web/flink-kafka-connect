package start;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class WordCount {
@SuppressWarnings({ "deprecation"})
public static void main(String[] args) throws Exception{
	DataStream<String> dataStream = null;
	final StreamExecutionEnvironment env =
			StreamExecutionEnvironment.getExecutionEnvironment();
	final ParameterTool params=ParameterTool.fromArgs(args);
	env.getConfig().setGlobalJobParameters(params);	
	if(params.has("input")) {
		dataStream = env.readTextFile(params.get("input"));
		
	}
	else if(params.has("host")&&params.has("port")) {
		dataStream=env.socketTextStream(params.get("host"),Integer.parseInt(params.get("port")));
		}
	else {
		System.out.println("Use --host and --port to specify socket");
		System.out.println("Use --input to specify file input");
}
if(dataStream==null) {
	System.exit(1);
	return;
}
dataStream.print();
dataStream.writeAsText(params.get("output"),FileSystem.WriteMode.OVERWRITE);
env.execute("Read and write");
}
}
