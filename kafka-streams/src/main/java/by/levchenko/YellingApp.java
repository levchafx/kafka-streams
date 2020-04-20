package by.levchenko;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class YellingApp {
	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yelling_app_id");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

		StreamsConfig streamsConfig = new StreamsConfig(props);

		Serde<String> stringSerde = Serdes.String();

		StreamsBuilder builder = new StreamsBuilder();

		KStream<String, String> simpleFirstStream = builder.stream("src-topic",
				Consumed.with(stringSerde, stringSerde));

		KStream<String, String> upperCasedStream = simpleFirstStream
				.map((key, value) -> KeyValue.pair(key, value.toUpperCase()));

		upperCasedStream.to("out-topic", Produced.with(stringSerde, stringSerde));
		upperCasedStream.print(Printed.<String, String>toSysOut().withLabel("Yelling App"));

		KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
		kafkaStreams.start();
		Thread.sleep(35000);
		kafkaStreams.close();

	}
}
