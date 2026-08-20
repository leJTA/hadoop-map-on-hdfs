/**
 * Modified version of SpillRecord for files stored on HDFS
 */
package org.apache.hadoop.mapred;

import static org.apache.hadoop.mapred.MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SecureIOUtils;
import org.apache.hadoop.util.PureJavaCrc32;

@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class DistributedSpillRecord {

  /** Backing store */
  private final ByteBuffer buf;
  /** View of backing storage as longs */
  private final LongBuffer entries;

  public DistributedSpillRecord(int numPartitions) {
    buf = ByteBuffer.allocate(
        numPartitions * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH);
    entries = buf.asLongBuffer();
  }

  public DistributedSpillRecord(Path indexFileName, JobConf job) throws IOException {
    this(indexFileName, job, null);
  }

  public DistributedSpillRecord(Path indexFileName, JobConf job, String expectedIndexOwner)
    throws IOException {
    this(indexFileName, job, new PureJavaCrc32(), expectedIndexOwner);
  }

  public DistributedSpillRecord(Path indexFileName, JobConf job, Checksum crc,
                     String expectedIndexOwner)
      throws IOException {

    final FileSystem fs = FileSystem.get(job);
    final FSDataInputStream in = fs.open(indexFileName);
    try {
      final long length = fs.getFileStatus(indexFileName).getLen();
      final int partitions = (int) length / MAP_OUTPUT_INDEX_RECORD_LENGTH;
      final int size = partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH;
      buf = ByteBuffer.allocate(size);
      if (crc != null) {
        crc.reset();
        CheckedInputStream chk = new CheckedInputStream(in, crc);
        IOUtils.readFully(chk, buf.array(), 0, size);

        if (chk.getChecksum().getValue() != in.readLong()) {
          throw new ChecksumException("Checksum error reading spill index: " +
                                indexFileName, -1);
        }
      } else {
        IOUtils.readFully(in, buf.array(), 0, size);
      }
      entries = buf.asLongBuffer();
    } finally {
      in.close();
    }
  }

  /**
   * Return number of IndexRecord entries in this spill.
   */
  public int size() {
    return entries.capacity() / (MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8);
  }

  /**
   * Get spill offsets for given partition.
   */
  public IndexRecord getIndex(int partition) {
    final int pos = partition * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
    return new IndexRecord(entries.get(pos), entries.get(pos + 1),
                           entries.get(pos + 2));
  }

  /**
   * Set spill offsets for given partition.
   */
  public void putIndex(IndexRecord rec, int partition) {
    final int pos = partition * MapTask.MAP_OUTPUT_INDEX_RECORD_LENGTH / 8;
    entries.put(pos, rec.startOffset);
    entries.put(pos + 1, rec.rawLength);
    entries.put(pos + 2, rec.partLength);
  }

  /**
   * Write this spill record to the location provided.
   */
  public void writeToFile(Path loc, JobConf job)
      throws IOException {
    writeToFile(loc, job, new PureJavaCrc32());
  }

  public void writeToFile(Path loc, JobConf job, Checksum crc)
      throws IOException {
    final FileSystem fs = FileSystem.get(job);
    CheckedOutputStream chk = null;
    final FSDataOutputStream out = fs.create(loc);
    try {
      if (crc != null) {
        crc.reset();
        chk = new CheckedOutputStream(out, crc);
        chk.write(buf.array());
        out.writeLong(chk.getChecksum().getValue());
      } else {
        out.write(buf.array());
      }
    } finally {
      if (chk != null) {
        chk.close();
      } else {
        out.close();
      }
    }
  }

}
