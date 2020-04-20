package by.levchenko;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public class Runner {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-app");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();

		builder.<String, String>stream("app_events").flatMapValues(value -> Arrays.asList(value.split(" ")))
				.to("app_aggregated_events");

		final Topology topology = builder.build();
		final KafkaStreams streams = new KafkaStreams(topology, props);
		final CountDownLatch latch = new CountDownLatch(1);

//		KTable<Windowed<String>, Long> sessionizedAggregatedStream = streams
//				.windowedBy(SessionWindows.with(TimeUnit.MINUTES.toMillis(5)).aggregate(() -> 0L,
//						(aggKey, newValue, aggValue) -> aggValue + newValue,
//						(aggKey, leftAggValue, rightAggValue) -> leftAggValue + rightAggValue,
//						Materialized
//								.<String, Long, SessionStore<Bytes, byte[]>>as("sessionized-aggregated-stream-store")
//								.withValueSerde(Serdes.Long())));

		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			System.exit(1);
		}
		System.exit(0);
	}
}
