package net.ibogner.kinesis;


import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordProcessor implements IRecordProcessor {
    private static final Logger logger = LoggerFactory.getLogger(RecordProcessor.class);

    private String shardId;

    @Override
    public void initialize(InitializationInput initializationInput) {
        shardId = initializationInput.getShardId();
    }

    public void processRecords(ProcessRecordsInput processRecordsInput) {
        logger.info("processing record...");
        this.checkpoint(processRecordsInput.getCheckpointer());
    }

    public void shutdown(ShutdownInput shutdownInput) {
        ShutdownReason shutdownReason = shutdownInput.getShutdownReason();
        logger.info("Shutting down record processor for shard id {} with shutdown reason {}", this.shardId, shutdownReason);
        if(shutdownReason == ShutdownReason.TERMINATE) {
            this.checkpoint(shutdownInput.getCheckpointer());
        }

    }

    protected void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        try {
            checkpointer.checkpoint();
        } catch (Exception var3) {
            logger.error("Failed to checkpoint", var3);
        }

    }
}
