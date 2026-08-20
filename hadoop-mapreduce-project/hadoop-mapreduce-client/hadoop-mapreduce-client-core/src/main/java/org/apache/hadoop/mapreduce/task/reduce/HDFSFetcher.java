/**
 * modified version of Fetcher to Retrieve map output from HDFS
 * instead of remotly from each map host.
 */
package org.apache.hadoop.mapreduce.task.reduce;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.crypto.SecretKey;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.IndexRecord;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputFile;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.DistributedSpillRecord;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.security.IntermediateEncryptedStream;
import org.apache.hadoop.mapreduce.security.SecureShuffleUtils;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.SubjectInheritingThread;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@VisibleForTesting
public class HDFSFetcher<K, V> extends Fetcher<K, V> {


  private static final Logger LOG = LoggerFactory.getLogger(HDFSFetcher.class);
  private static final String JOB_OUTPUT_DIR = "output";
  private static final String MAP_OUTPUT_FILENAME_STRING = "file.out";
  private static TaskAttemptID[] EMPTY_ATTEMPT_ID_ARRAY = new TaskAttemptID[0];

  // assume configured to $localdir/usercache/$user/appcache/$appId
  // private DistributedDirAllocator distDirAlloc = new DistributedDirAllocator(MRConfig.LOCAL_DIR);
  private JobConf job;
  private final boolean fetchRetryEnabled;

  @VisibleForTesting
  HDFSFetcher(JobConf job, TaskAttemptID reduceId, ShuffleSchedulerImpl<K, V> scheduler, MergeManager<K, V> merger,
              Reporter reporter, ShuffleClientMetrics metrics, ExceptionReporter exceptionReporter,
              SecretKey shuffleKey) {
    super(job, reduceId, scheduler, merger, reporter, metrics, exceptionReporter, shuffleKey);
    this.job = job;

    boolean shuffleFetchEnabledDefault = job.getBoolean(
        YarnConfiguration.NM_RECOVERY_ENABLED,
        YarnConfiguration.DEFAULT_NM_RECOVERY_ENABLED);
    this.fetchRetryEnabled = job.getBoolean(
        MRJobConfig.SHUFFLE_FETCH_RETRY_ENABLED,
        shuffleFetchEnabledDefault);

    setName("hdfs-fetcher#" + id);
    setDaemon(true);
  }

  @VisibleForTesting
  @Override
  protected void copyFromHost(MapHost host) throws IOException {
    // Get completed maps on 'host'
    List<TaskAttemptID> maps = scheduler.getMapsForHost(host);

    // Sanity check to catch hosts with only 'OBSOLETE' maps,
    // especially at the tail of large jobs
    if (maps.size() == 0) {
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("HDFSFetcher " + id + " going to fetch from " + host + " for: " + maps);
    }

    // List of maps to be fetched yet
    Set<TaskAttemptID> remaining = new HashSet<TaskAttemptID>(maps);
    try {
      // Loop through available map-outputs and fetch them
      // On any error, faildTasks is not null and we exit
      // after putting back the remaining maps to the
      // yet_to_be_fetched list and marking the failed tasks.
      TaskAttemptID[] failedTasks = null;
      while (!remaining.isEmpty() && failedTasks == null) {
        try {
          failedTasks = copyMapOutputFromHDFS(host, remaining, fetchRetryEnabled);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      if (failedTasks != null && failedTasks.length > 0) {
        LOG.warn("copyMapOutput failed for tasks " + Arrays.toString(failedTasks));
        scheduler.hostFailed(host.getHostName());
        for (TaskAttemptID left : failedTasks) {
          scheduler.copyFailed(left, host, true, false);
        }
      }

      // Sanity check
      if (failedTasks == null && !remaining.isEmpty()) {
        throw new IOException("server didn't return all expected map outputs: " + remaining.size() + " left.");
      }
    } finally {
      for (TaskAttemptID left : remaining) {
        scheduler.putBackKnownMapOutput(host, left);
      }
    }
  }

  private TaskAttemptID[] copyMapOutputFromHDFS(MapHost host, Set<TaskAttemptID> remaining,
                                        boolean canRetry) throws IOException {
    MapOutput<K, V> mapOutput = null;
    TaskAttemptID mapId = null;

    Path mapOutputFileName = getMapOutputPath(remaining.iterator().next());
    Path indexFileName = mapOutputFileName.suffix(".index");

    long decompressedLength = -1;
    long compressedLength = -1;


    try {
      long startTime = Time.monotonicNow();

      // Read its index to determine the location of our split
      // and its size.
      DistributedSpillRecord dsr = new DistributedSpillRecord(indexFileName, job);
      IndexRecord ir = dsr.getIndex(reduce);

      compressedLength = ir.partLength;
      decompressedLength = ir.rawLength;

      compressedLength -= CryptoUtils.cryptoPadding(job);
      decompressedLength -= CryptoUtils.cryptoPadding(job);

      // Get the location for the map output - either in-memory or on-disk
      try {
        mapOutput = merger.reserve(mapId, decompressedLength, id);
      } catch (IOException ioe) {
        // kill this reduce attempt
        scheduler.reportLocalError(ioe);
        return EMPTY_ATTEMPT_ID_ARRAY;
      }

      // Check if we can shuffle *now* ...
      if (mapOutput == null) {
        LOG.info("hdfs-fetcher#" + id + " - MergeManager returned status WAIT ...");
        // Not an error but wait to process data.
        return EMPTY_ATTEMPT_ID_ARRAY;
      }
      FileSystem defaultFS = null;
      FSDataInputStream inStream = null;
      try {
        defaultFS = FileSystem.get(job);
        inStream = defaultFS.open(mapOutputFileName);
      } catch (IOException e) {
        LOG.warn("Path error : ", mapOutputFileName.toString());
        // Don't know which one was bad, so consider all of them as bad
        return remaining.toArray(new TaskAttemptID[remaining.size()]);
      }

      try {
        inStream.seek(ir.startOffset);
        inStream = IntermediateEncryptedStream.wrapIfNecessary(job, inStream,
                mapOutputFileName);
        mapOutput.shuffle(host, inStream, compressedLength, decompressedLength, metrics, reporter);
      } catch (java.lang.InternalError | Exception e) {
        LOG.warn("Failed to shuffle for hdfs-fetcher#"+id, e);
        throw new IOException(e);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("header: " + mapId + ", len: " + compressedLength + ", decomp len: " + decompressedLength);
      }

      // The codec for lz0,lz4,snappy,bz2,etc. throw java.lang.InternalError
      // on decompression failures. Catching and re-throwing as IOException
      // to allow fetch failure logic to be processed
      try {
        // Go!
        LOG.info("hdfs-fetcher#" + id + " about to shuffle output of map " + mapOutput.getMapId() + " decomp: " +
                 decompressedLength + " len: " + compressedLength + " to " + mapOutput.getDescription());
        mapOutput.shuffle(host, inStream, compressedLength, decompressedLength, metrics, reporter);
      } catch (java.lang.InternalError | Exception e) {
        LOG.warn("Failed to shuffle for hdfs-fetcher#" + id, e);
        throw new IOException(e);
      }

      // Inform the shuffle scheduler
      long endTime = Time.monotonicNow();

      scheduler.copySucceeded(mapId, host, compressedLength, startTime, endTime, mapOutput);
      // Note successful shuffle
      remaining.remove(mapId);
      metrics.successFetch();
      return null;
    } catch (IOException ioe) {
      if (mapOutput != null) {
        mapOutput.abort();
      }

      if (mapId == null || mapOutput == null) {
        LOG.warn("hdfs-fetcher#" + id + " failed to read map header" + mapId + " decomp: " + decompressedLength + ", " +
                     compressedLength, ioe);
        if (mapId == null) {
          return remaining.toArray(new TaskAttemptID[remaining.size()]);
        } else {
          return new TaskAttemptID[] {mapId};
        }
      }

      LOG.warn("Failed to shuffle output of " + mapId + " from HDFS : map host = " + host.getHostName(), ioe);

      // Inform the shuffle-scheduler
      metrics.failedFetch();
      return new TaskAttemptID[] {mapId};
    }
  }

  private Path getMapOutputPath(TaskAttemptID attemptID) {
    Path attemptOutputDir = new Path(JOB_OUTPUT_DIR, attemptID.toString()); // -> output/attempt_XXXXXXXXXXXXX_XXXX_m_XXXXXX_X
    Path mapOutputDir = new Path(job.get(MRConfig.LOCAL_DIR), attemptOutputDir); // -> $localdir/usercache/$user/appcache/$appId/output/attempt_XXXXXXXXXXXXX_XXXX_m_XXXXXX_X
    return new Path(mapOutputDir, MAP_OUTPUT_FILENAME_STRING);
  }
}
