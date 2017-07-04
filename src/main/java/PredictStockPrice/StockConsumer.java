package PredictStockPrice;

import org.apache.kafka.clients.consumer.*;


import java.io.IOException;
import java.util.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class StockConsumer {

    // Declare a new consumer
    public static KafkaConsumer<String, JsonNode> consumer;

    public static void main(String[] args) throws IOException, InterruptedException {
        // check command-line arguments
        if(args.length != 5) {
            System.err.println("usage: StockConsumer <broker-socket> <input-topic> <stock-symbol> <group-id> <threshold-%>");
            System.err.println("e.g.: StockConsumer localhost:9092 stats orcl mycg 0.5");
            System.exit(1);
        }
        
        // initialize varaibles
        String brokerSocket = args[0];
        String inputTopic = args[1];
        String stockSymbol = args[2];
        String groupId = args[3];
        double thresholdPercentage = Double.parseDouble(args[4]);
        
        long pollTimeOut = 1000;
        double currentAggregatedStatistic=0.0, previousAggregatedStatistic=0.0;

        
        // configure consumer
        configureConsumer(brokerSocket, groupId);
        
        // TODO subscribe to the topic
        List<String> topics = new ArrayList<String>();
        topics.add(inputTopic);
        consumer.subscribe(topics);
        boolean firstValue = true;
        // TODO loop infinitely -- pulling messages out every pollTimeOut ms
        while(true){
        	// TODO iterate through message batch
        	ConsumerRecords<String, JsonNode> consumerRecords = consumer.poll(pollTimeOut);
        	Iterator<ConsumerRecord<String, JsonNode>> iterator = consumerRecords.iterator();
        	double sumHigh = 0, sumLow = 0, sumOpen = 0, sumClose = 0, sumVolume=0, lastClose=0;
            int counter = 0;
            String lastTimeStamp = null;
            // TODO create a ConsumerRecord from message
            while(iterator.hasNext()){
        		ConsumerRecord<String, JsonNode> record = iterator.next();
        	// TODO pull out statistics from message
        		JsonNode node = record.value();
        		sumHigh += node.get("meanHigh").asDouble();
        		sumLow += node.get("meanLow").asDouble();
        		sumOpen += node.get("meanOpen").asDouble();
        		sumClose += node.get("meanClose").asDouble();
        		sumVolume += node.get("meanVolume").asDouble();
        		lastTimeStamp = node.get("lastTimestamp").asText();
        		lastClose = node.get("lastClose").asDouble();
        		counter++;
        		//check to see how many records have been fetched
        		//System.out.println("Data recevied; current Counter : "+counter);
        	}
        	// TODO calculate batch statistics meanHigh, meanLow, meanOpen, meanClose, meanVolume
        	double meanHigh = 0, meanLow = 0, meanOpen = 0, meanClose = 0, meanVolume = 0;
        	if(counter!=0){
            	meanHigh=sumHigh/counter;
                meanLow=sumLow/counter;
                meanOpen=sumOpen/counter;
                meanClose=sumClose/counter;
                meanVolume=sumVolume/counter;
                // TODO calculate currentAggregatedStatistic and compare to previousAggregatedStatistic
                currentAggregatedStatistic = (meanVolume * (meanHigh + meanLow + meanOpen + meanClose)) / 4.0;
                if(firstValue){
                	firstValue = false;
                	previousAggregatedStatistic = currentAggregatedStatistic;
                	continue;
                }
                else{
                	// TODO determine if delta percentage is greater than threshold 
                	double deltaPercentage = 0.0;
                	deltaPercentage = (currentAggregatedStatistic-previousAggregatedStatistic)/(100*meanVolume);
                	previousAggregatedStatistic = currentAggregatedStatistic;
                	String outputString = lastTimeStamp+","+stockSymbol+","+lastClose+","+deltaPercentage;
                    // TODO print output to screen
                	//lastTimestamp,stockSymbol,lastClose,deltaPercentage,position
                	if(deltaPercentage>0 && deltaPercentage>thresholdPercentage){
                		outputString+=",sell";
                	}
                	else{
                		if(deltaPercentage<0 && Math.abs(deltaPercentage)>thresholdPercentage){
                			outputString+=",buy";
                    		
                		}
                		else{
                			outputString+=",hold";
                		}
                	}
                	System.out.println(outputString);
                }
        	}
        	//introducing Thread.sleep helps in fetching records more than one instead of fetching just one record
        	//Thread.sleep(10000);
        }
    }

    public static void configureConsumer(String brokerSocket, String groupId) {
        Properties props = new Properties();
        props.put("value.deserializer", "org.apache.kafka.connect.json.JsonDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("bootstrap.servers", brokerSocket);
        props.put("group.id", groupId);
        props.put("auto.commit.enable", true);

        consumer = new KafkaConsumer<String, JsonNode>(props);
    }
}

