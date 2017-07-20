package com.metamx.tranquility.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.metamx.tranquility.kinesis.writer.TranquilityEventWriter;

/**
 * Created by gaurav on 7/5/17.
 */
public class KinesisRecordProcessorFactory implements IRecordProcessorFactory {

    private TranquilityEventWriter tranquilityEventWriter;
    public KinesisRecordProcessorFactory(TranquilityEventWriter tranquilityEventWriter){
        this.tranquilityEventWriter = tranquilityEventWriter;

    }
    @Override
    public IRecordProcessor createProcessor() {
        return new KinesisRecordProcessor(tranquilityEventWriter);
    }
}
