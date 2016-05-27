package net.ibogner.kinesis;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.metrics.AwsSdkMetrics;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class KinesisStressExample {

    private static final Logger logger = LoggerFactory.getLogger(KinesisStressExample.class);

    private volatile boolean keepGoing = true;

    private final String streamName;
    private final Integer numThreads;
    private final Integer numRecordsPerPut;
    private final Long numMessagesToProduce;

    private final Random random = new Random();
    private final AtomicLong msgsProduced = new AtomicLong(0);
    private final ExecutorService threadPool;
    private final AmazonKinesisClient kinesisClient;

    public KinesisStressExample(String streamName, Integer numThreads, Integer numRecordsPerPut, Long numMessagesToProduce, String credentialsProfileName) {
        this.streamName = streamName;
        this.numThreads = numThreads;
        this.numRecordsPerPut = numRecordsPerPut;
        this.numMessagesToProduce = numMessagesToProduce;


        final ClientConfiguration clientConfiguration = new ClientConfiguration();
        final AWSCredentialsProvider awsCredentialsProvider = getCredentialsProvider(credentialsProfileName);

        kinesisClient = new AmazonKinesisClient(awsCredentialsProvider, clientConfiguration, new RequestMetricCollector() {
            @Override
            public void collectMetrics(Request<?> request, Response<?> response) {

            }
        });
        kinesisClient.setRegion(Region.getRegion(Regions.US_WEST_2));
        AwsSdkMetrics.enableDefaultMetrics();
        threadPool = Executors.newFixedThreadPool(numThreads);
    }

    public void startProducers() {
        for (int threadNum = 0; threadNum < numThreads; ++threadNum) {
            threadPool.submit((Runnable) () -> produce());
        }
    }

    public void awaitCompletion() throws InterruptedException {
        while (msgsProduced.get() < numMessagesToProduce) {
            Thread.sleep(1000);
        }
        keepGoing = false;
        MoreExecutors.shutdownAndAwaitTermination(threadPool, 10, TimeUnit.SECONDS);
    }

    private void produce() {
        logger.info("starting producer");
        while (keepGoing) {
            final PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
            for (int msgNum = 0; msgNum < numRecordsPerPut; ++msgNum) {
                msgsProduced.incrementAndGet();

                final ByteBuffer randomByteBuffer = buildRandomByteBuffer();

                putRecordsRequest.withStreamName(streamName)
                                 .withRecords(new PutRecordsRequestEntry().withData(randomByteBuffer)
                                                                          .withPartitionKey(Integer.toString(random.nextInt())));
            }
            logger.info("Sending to {}", streamName);
            kinesisClient.putRecords(putRecordsRequest);
            logger.info("Sent {} to {}", msgsProduced.get(), streamName);
        }
    }

    /**
     * Build up a ByteBuffer that's roughly 10KB
     */
    private ByteBuffer buildRandomByteBuffer() {
        final String tenKBRandomString = Strings.repeat(UUID.randomUUID().toString(), 277);
        return ByteBuffer.wrap(tenKBRandomString.getBytes(Charsets.UTF_8));
    }

    private AWSCredentialsProvider getCredentialsProvider(String profileName) {
        return new AWSCredentialsProviderChain(new ProfileCredentialsProvider(profileName),
                                               new InstanceProfileCredentialsProvider());
    }

    /**
     *  Usage: [className] [streamName] [numberOfProducerThreads] [numberOfRecordsPerRequest] [numberOfTotalRecordsToPut] [optionalProfileName]
     */
    public static void main(String[] args) throws InterruptedException {
        checkArgument(args.length >= 4, "Incorrect number of arguments");

        final String streamName = checkNotNull(args[0], "You must provide a stream name");
        final String numProducers = checkNotNull(args[1], "You must define the number of producers");
        final String numRecordsPerPut = checkNotNull(args[2], "You must define how many records to put per request");
        final String numRecordsToPut = checkNotNull(args[3], "You must define how many records to write during the test");
        final Optional<String> profileName = args.length > 4 ? Optional.of(args[4]) : Optional.empty();

        final KinesisStressExample example = new KinesisStressExample(streamName, Integer.valueOf(numProducers), Integer.valueOf(numRecordsPerPut), Long.valueOf(numRecordsToPut), profileName.orElse(null));
        example.startProducers();
        example.awaitCompletion();
        logger.info("Test run complete");
    }

}
