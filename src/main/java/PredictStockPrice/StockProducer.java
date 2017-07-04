package PredictStockPrice;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
        
public class StockProducer {
    // Set the stream and topic to publish to.
    public static String topic;
    
        
    // Declare a new producer
    public static KafkaProducer<String, JsonNode> producer;
        
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException, TimeoutException {
        // check command-line args
        if(args.length != 5) {
            System.err.println("usage: StockProducer <broker-socket> <input-file> <stock-symbol> <output-topic> <sleep-time>");
            System.err.println("eg: StockProducer localhost:9092 /user/user01/LAB2/orcl.csv orcl prices 1000");
            System.exit(1);
        }
        
        // initialize variables
        String brokerSocket = args[0];
        String inputFile = args[1];
        String stockSymbol = args[2];
        String outputTopic = args[3];
        long sleepTime = Long.parseLong(args[4]);
        
        
        // configure the producer
        configureProducer(brokerSocket);
        
        // TODO create a buffered file reader for the input file
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        String line = br.readLine();
        // TODO loop through all lines in input file
        while(line!=null){
        	// TODO filter out "bad" records
        	if(!Character.isLetter(line.charAt(0))){
				// TODO create an ObjectNode to store data in
                ObjectNode value = JsonNodeFactory.instance.objectNode();
                // TODO parse out the fields from the line and create key-value pairs in ObjectNode
                String [] lineValues=line.split(",");
                value.put("timestamp",lineValues[0]); 
                value.put("open",lineValues[1]); 
                value.put("high",lineValues[2]); 
                value.put("low", lineValues[3]); 
                value.put("close", lineValues[4]); 
                value.put("volume", lineValues[5]); 
                
                // TODO produce the record
                ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(outputTopic, stockSymbol,value);
                producer.send(rec);
                //test to check the record that is sent
                //System.out.println("Sent message  " + line);
                
                // TODO sleep the thread
                //hint: Sleep thread sleep and duration sleep from spark
                Thread.sleep(sleepTime);
			}
			line=br.readLine();
        }
        // TODO close buffered reader
        br.close();
        // TODO close producer
        producer.close();
    }

    public static void configureProducer(String brokerSocket) {
        Properties props = new Properties();
        props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("bootstrap.servers", brokerSocket);
        producer = new KafkaProducer<String, JsonNode>(props);
    }
}
