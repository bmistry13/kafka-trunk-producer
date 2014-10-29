
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaConsumerWithDelay {
	
	private static final Logger LOG = LoggerFactory
			.getLogger(KafkaConsumerWithDelay.class);

	private final String topic;
	private final String zkConnect;
	private final String groupId ;
	private int numberOfConsumer = 1;

	private boolean close;

	private ConsumerConnector consumerConnector = null;

	public static final String DYNAMIC_SOURCE = "KafkaDynamicSource_";

	private String mainSourceName;

	/**
	 * Synchronized List so it should be good only need for Dynamic Sources..
	 * 
	 */

	private ConsumerIterator<byte[], byte[]> mainIterator;

	public KafkaConsumerWithDelay(String zk, String groupId, String topic, int consumerThreads, String mainSourceName ) {


		if (zk == null || (zk.trim()).length() == 0) {
			throw new IllegalArgumentException("zkConnect is null or empty");
		}

		if (groupId == null || (groupId.trim()).length() == 0) {
			throw new IllegalArgumentException("Group ID is null or empty");
		}

		if (topic == null || (topic.trim()).length() == 0) {
			throw new IllegalArgumentException("topic is null or empty");
		}
		this.zkConnect = zk;
		this.groupId = groupId;
		this.topic = topic;		
		numberOfConsumer = consumerThreads;
		this.mainSourceName = mainSourceName;

		init();
		LOG.info("Kakfa Main Soruce Started");
	}

	//@Override
	public boolean hasNext() {
		int retry = 2;
		while (retry > 0) {
			try {
				// this hasNext is blocking call..
				boolean result = !close && mainIterator.hasNext();
				LOG.info("called hasNext() Result " + result);
				return result;
			} catch (IllegalStateException exp) {
				mainIterator.resetState();
				retry--;
			}
		}
		return false;
	}

	//@Override
	public byte[] getNextDataPair() {

		int retry = 2;
		while (retry > 0) {
			try {
				byte[] msg = mainIterator.next().message();
				if (msg != null) {
					return msg;
				}
			} catch (IllegalStateException exp) {
				mainIterator.resetState();
				retry--;
			}
		}
		return "MAIN SORUCE NO MSG".getBytes();
	}

	@SuppressWarnings("all")
	private void init() {
		LOG.info("init called");
		// +1 for the schedule thread pool (heart beat thread)
		// executor = Executors.newFixedThreadPool(numberOfConsumer +1);

		consumerConnector = Consumer
				.createJavaConsumerConnector(getConsumerConfig());
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, numberOfConsumer);
		Map<String, List<KafkaStream<byte[], byte[]>>> topicStreamMap = consumerConnector
				.createMessageStreams(topicCountMap);

		List<KafkaStream<byte[], byte[]>> streams = Collections
				.synchronizedList(topicStreamMap.get(topic));


		Iterator<KafkaStream<byte[], byte[]>> iterator = streams.iterator();
		// remove the head first list for this source...rest are for the Dynamic
		// Souce...
		mainIterator = iterator.next().iterator();

		List<ConsumerIterator<byte[], byte[]>> iteratorList = new ArrayList<ConsumerIterator<byte[], byte[]>>(
				streams.size());
		// now rest of the iterator must be registered now..
		while (iterator.hasNext()) {
			iteratorList.add(iterator.next().iterator());
		}

		KafkaStreamRegistory.registerStream(mainSourceName, iteratorList);

	}

	// TODO PUT THIS IN AS CONFUGURATION...
	private ConsumerConfig getConsumerConfig() {
		Properties props = new Properties();
		props.put("zookeeper.connect", zkConnect);
		props.put("group.id", groupId);
		props.put("consumer.timeout.ms", "-1");
		props.put("zookeeper.session.timeout.ms", "10000");
		props.put("zookeeper.sync.time.ms", "6000");
		props.put("auto.commit.interval.ms", "2000");
		props.put("rebalance.max.retries", "8");
		props.put("auto.offset.reset", "largest");
		props.put("fetch.message.max.bytes", "2097152");
		props.put("socket.receive.buffer.bytes", "2097152");
		props.put("auto.commit.enable", "true");
		props.put("queued.max.message.chunks", "10000");
		return new ConsumerConfig(props);
	}

	public void closeSource() {
		close = true;
		if (consumerConnector != null) {
			consumerConnector.shutdown();
			// if(executor != null){
			// executor.shutdown();
			// }
		}
	}

   static final class KafkaStreamRegistory {

			private static final Map<String, List<ConsumerIterator<byte[], byte[]>>> MAIN_STEAM_DYNAMIC_STEAM_REG =
					new HashMap<String, List<ConsumerIterator<byte[], byte[]>>>();

//			private static final Map<String, List<KafkaStream<byte[], byte[]>>> KAKFA_MUPD_SOURCE_REG =
//					new HashMap<String, List<KafkaStream<byte[], byte[]>>>();	
			
			private KafkaStreamRegistory(){
				
			}
			
			public static final synchronized void registerStream(String mainSteam, List<ConsumerIterator<byte[], byte[]>> dynamic){
				List<ConsumerIterator<byte[], byte[]>> newDynamic = MAIN_STEAM_DYNAMIC_STEAM_REG.get(mainSteam);
				if(newDynamic != null){
					new IllegalStateException(mainSteam + " already registred with this application.  "
							+ "Please check configuraiton...");
				}
				MAIN_STEAM_DYNAMIC_STEAM_REG.put(mainSteam, dynamic);
			}
			

			
			public static final synchronized ConsumerIterator<byte[], byte[]> getDynamicStream(String mainSteam){
				List<ConsumerIterator<byte[], byte[]>> newDynamic = MAIN_STEAM_DYNAMIC_STEAM_REG.get(mainSteam);
				if(newDynamic == null){
					new IllegalStateException(mainSteam + " is NOT registred with this application. Kafka Source.."
							+ "Please check configuraiton...");
				}
				if(newDynamic.size() == 0){
					new IllegalStateException(mainSteam + " there is no Dynamic Stream left. Kafka Source.."
							+ "Please check configuraiton...");			
				}
				// remove the return the sream....
				return newDynamic.iterator().next();
			}	
   }
	
	public static void doWorkBeforeReadingSteam(){
		
		// do some work....
		try {
			Thread.sleep(60000);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}	
	}

	static class KafkaWithDynamicSoruce {
		private final ConsumerIterator<byte[], byte[]> iterator;
		private boolean close = false;
		private static final Logger LOG = LoggerFactory
				.getLogger(KafkaWithDynamicSoruce.class);

		public KafkaWithDynamicSoruce(String mainSource) {
	
			iterator = KafkaStreamRegistory.getDynamicStream(mainSource);
			LOG.info("Kakfa Dynamic Soruce Started Source # " + mainSource);
		}

		public boolean hasNext() {
			LOG.info("called of  hasNext() :");
			int retry = 3;
			while(retry > 0){
				try{
					// this hasNext is blocking call..
					boolean result = !close && iterator.hasNext();
					//LOG.info("hasNext() :" + result);
					return result;
				}catch(IllegalStateException exp){
					iterator.resetState();
					LOG.error("GOT Illegale arg trying to recover....", exp);
					retry--;
				}
			}
			return false;
		}

		public byte[] getNextDataPair() {
			int retry = 3;
			while (retry > 0) {
				try {
//					Mupd8DataPair ret = new Mupd8DataPair();
					byte[] msg = iterator.next().message();

					//LOG.info("KafkaData Returned :" + new String(msg));
					return msg;
				} catch (IllegalStateException exp) {
					iterator.resetState();
					retry--;
				}
			}
			return "DYNAMIC SORUCE NO MSG".getBytes();
		}	
	}
   
	
	public static void main(String[] test) {

		final String mainSoruce = "Kafka";
		/**
		 * SET YOUR ZK HERE 
		 * 
		 */
		String zk = "ZK HERE";
		String groupid =  "mytest." + Math.random();
		/**
		 * SET YOUR TOPIC HERE...
		 */
		String topicname = "MY.TOPIC HERE";
		/**
		 * SET TOTAL NUMBER OF PARTITION OR DYNAMIC SOURCE TO CREATE with main...
		 * 
		 */
		int totalPartition = 32;
		
		KafkaConsumerWithDelay source = new KafkaConsumerWithDelay(
				zk,groupid,
				topicname,totalPartition,mainSoruce);

		ExecutorService service = Executors.newFixedThreadPool(totalPartition);
		for (int i = 1; i < totalPartition; i++) {
			service.execute(new Runnable() {
				@Override
				public void run() {
					
					/**
					 * THIS Delay is causing the IllegalStateException exception...
					 */
					doWorkBeforeReadingSteam();
					KafkaWithDynamicSoruce soruce = new KafkaWithDynamicSoruce(mainSoruce);
					while (true) {
						if (soruce.hasNext()) {
							System.out.println("Dynamic GOT IT "
									+ new String(
											soruce.getNextDataPair()));
						}
					}
				}
			});
		}
		while (true) {
			if (source.hasNext()) {
				System.out.println("Main GOT IT "
						+ new String(source.getNextDataPair()));
			}
		}

	}

}
