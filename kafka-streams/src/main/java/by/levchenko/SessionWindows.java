package by.levchenko;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.SessionStore;

public class SessionWindows {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-app");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		// props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
		// Serdes.String().getClass());
		StreamsBuilder builder = new StreamsBuilder();

		ArrayListSerde<String> valueSerde = new ArrayListSerde<>(Serdes.String());
		builder.stream("app_events", Consumed.with(Serdes.String(), Serdes.String())).groupByKey()
				.windowedBy(org.apache.kafka.streams.kstream.SessionWindows.with(Duration.ofMinutes(1)))
				.aggregate(() -> new ArrayList<String>(), (aggKey, newValue, aggValue) -> {
					(aggValue).add(newValue);
					return aggValue;
				}, (aggKey, leftAggValue, rightAggValue) -> {
					leftAggValue.addAll(rightAggValue);
					return leftAggValue;
				}, Materialized.<String, ArrayList<String>, SessionStore<Bytes, byte[]>>as(
						"sessionized-aggregated-stream-store").withValueSerde(valueSerde))
				.suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded())).toStream()
				.selectKey((key, value) -> key).to("app_aggregated_events",
						Produced.keySerde(WindowedSerdes.timeWindowedSerdeFrom(String.class)).valueSerde(valueSerde));

		;
		final Topology topology = builder.build();
		final KafkaStreams streams = new KafkaStreams(topology, props);
		final CountDownLatch latch = new CountDownLatch(1);
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
