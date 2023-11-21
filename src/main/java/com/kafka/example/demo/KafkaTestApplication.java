package com.kafka.example.demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class KafkaTestApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaTestApplication.class, args);
		
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams");
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
//		StreamsBuilder builder = new StreamsBuilder();
		
		// flatten the kafka stream sentence into a list of words with same key
		
//		KStream<String, String> stream = builder.stream("FlatMapInputTopic");
//		
//		KStream<String, String> altered = stream
//				
//				.flatMapValues(v -> Arrays.asList(v.split(" ")))
//				.selectKey((k, v)-> v.substring(0, 2));
//		altered.to("FlatMapOutputTopic", Produced.with(Serdes.String(), Serdes.String()));
//		KafkaStreams streams = new KafkaStreams(builder.build(), config);
//		streams.start();
//		System.out.println(streams.toString());
		 
		
		
		// wordcount of a sentence 
		
//		KStream<String, String> wordCountInput =  builder.stream("kafka-stream-input-topic");
//		
//		KTable<String, Long> wordsCount = wordCountInput.mapValues(value -> value.toLowerCase())
//		.flatMapValues(value -> Arrays.asList(value.split(" ")))
//		.groupBy((key,value) -> value)
//		.count(Materialized.as("word-counts"));
//		
//		wordsCount.toStream().to("kafka-stream-output-topic", Produced.with(Serdes.String(), Serdes.Long()));
//		KafkaStreams stream = new KafkaStreams(builder.build(), config);
//		stream.start();
//	
//		System.out.println(stream.toString());
		
		
		
		StreamsBuilder streamsBuilder = new StreamsBuilder();
//		
//	
//        // Build the Topology
        streamsBuilder.<String, String>stream("kafka-stream-input-topic")
                .flatMapValues((key, value) ->
                        Arrays.asList(value.toLowerCase()
                            .split(" ")))
                .groupBy((key, value) -> value)
                .count(Materialized.with(Serdes.String(), Serdes.Long()))
                .toStream()
                .to("kafka-stream-output-topic", Produced.with(Serdes.String(), Serdes.Long()));
        // Create the Kafka Streams Application
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), config);
        // Start the application
        kafkaStreams.start();

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
	}

}
