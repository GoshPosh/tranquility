/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.metamx.tranquility.kinesis;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.common.logger.Logger;
import com.metamx.tranquility.config.DataSourceConfig;
import com.metamx.tranquility.kinesis.model.MessageCounters;
import com.metamx.tranquility.kinesis.model.PropertiesBasedKinesisConfig;
import com.metamx.tranquility.kinesis.writer.TranquilityEventWriter;
import io.druid.concurrent.Execs;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Spawns a number of threads to read messages from Kafka topics and write them by calling
 * WriterController.getWriter(topic).send(). Will periodically call WriterController.flushAll() and when this completes
 * will call ConsumerConnector.commitOffsets() to save the last written offset to ZooKeeper. This implementation
 * guarantees that any events in Kafka will be read at least once even in case of a failure condition but does not
 * guarantee that duplication will not occur.
 */
public class KinesisConsumer
{
  private static final Logger log = new Logger(KinesisConsumer.class);

  private final ExecutorService consumerExec;
  private final AtomicBoolean shutdown = new AtomicBoolean();

  // prevents reading the next event from Kafka while events are being flushed and offset is being committed to ZK
  private final ReentrantReadWriteLock commitLock = new ReentrantReadWriteLock();

  private static AWSCredentialsProvider credentialsProvider;
  private final int numThreads;
  private final String appName;
  private final String streamName;
  private final int commitMillis;
  private final InitialPositionInStream initialPosition;
  private final TranquilityEventWriter eventWriter;
  private List<Worker> workers;

  private Map<String, MessageCounters> previousMessageCounters = new HashMap<>();

  public KinesisConsumer(
      final PropertiesBasedKinesisConfig globalConfig,
      final Properties kafkaProperties,
      final Map<String, DataSourceConfig<PropertiesBasedKinesisConfig>> dataSourceConfigs,
      final TranquilityEventWriter eventWriter
  )
  {
    credentialsProvider = new DefaultAWSCredentialsProviderChain();

    try {
      credentialsProvider.getCredentials();
    } catch (Exception e) {
      throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
              + "Please make sure that your credentials file is at the correct "
              + "location (~/.aws/credentials), and is in valid format.", e);
    }

    int defaultNumThreads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
    this.numThreads = globalConfig.getConsumerNumThreads() > 0
                      ? globalConfig.getConsumerNumThreads()
                      : defaultNumThreads;

    this.commitMillis = globalConfig.getCommitPeriodMillis();
    this.eventWriter = eventWriter;
    this.appName = globalConfig.getKinesisAppName();
    this.streamName = globalConfig.getKinesisStreamName();
    this.initialPosition = globalConfig.getInitialStreamPositionJava();
    this.consumerExec = Execs.multiThreaded(numThreads, "KinesisConsumer-%d");
    workers =new ArrayList<>();
  }

  public void start()
  {
    startConsumers();
  }


  private void startConsumers()
  {

    for (int i=0;i<numThreads; i++) {
      consumerExec.execute(
          new Runnable()
          {
            Worker worker;
            @Override
            public void run()
            {
              try {

                String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();

                KinesisClientLibConfiguration kinesisClientLibConfiguration =
                        new KinesisClientLibConfiguration(appName,
                                streamName,
                                credentialsProvider,
                                workerId);
                kinesisClientLibConfiguration.withInitialPositionInStream(initialPosition);

                IRecordProcessorFactory recordProcessorFactory = new KinesisRecordProcessorFactory(eventWriter);


                worker = new Worker.Builder()
                        .recordProcessorFactory(recordProcessorFactory)
                        .config(kinesisClientLibConfiguration)
                        .build();



                workers.add(worker);
                worker.run();
              }
              catch (Throwable e) {
                log.error(e, "Exception: ");
                throw Throwables.propagate(e);
              }
              finally {
                worker.shutdown();
              }
            }
          }
      );
    }
  }

  public void stop()
  {
    if (shutdown.compareAndSet(false, true)) {
      log.info("Shutting down - attempting to flush buffers and commit final offsets");

      try {
        eventWriter.stop();

        for (Iterator<Worker> workerIterator = workers.iterator(); workerIterator.hasNext();) {
          workerIterator.next().shutdown();
        }
      }
      catch (Exception e) {
        Thread.currentThread().interrupt();
        Throwables.propagate(e);
      }

      log.info("Finished clean shutdown.");
    }
  }

  public void join() throws InterruptedException
  {
    consumerExec.shutdown();
    consumerExec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    ;
  }


}
