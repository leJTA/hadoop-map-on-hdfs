/**
 * A modified version of LocalDirAllocator to store files and folders
 * on the default filesystem (usually HDFS) rather than the local filesystem.
 *
 */

package org.apache.hadoop.fs;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.util.*;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.net.NetUtils.getHostname;

@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class NonLocalDirAllocator {

  static final String E_NO_SPACE_AVAILABLE =
      "No space available in any of the local directories";

  //A Map from the config item names like "mapred.local.dir"
  //to the instance of the AllocatorPerContext. This
  //is a static object to make sure there exists exactly one instance per JVM
  private static Map <String, AllocatorPerContext> contexts =
                 new TreeMap<String, AllocatorPerContext>();
  private String contextCfgItemName;

  /** Used when size of file to be allocated is unknown. */
  public static final int SIZE_UNKNOWN = -1;

  /**
   * Create an allocator object.
   * @param contextCfgItemName contextCfgItemName.
   */
  public NonLocalDirAllocator(String contextCfgItemName) {
    this.contextCfgItemName = contextCfgItemName;
  }

  /** This method must be used to obtain the dir allocation context for a
   * particular value of the context name. The context name must be an item
   * defined in the Configuration object for which we want to control the
   * dir allocations (e.g., <code>mapred.local.dir</code>). The method will
   * create a context for that name if it doesn't already exist.
   */
  private AllocatorPerContext obtainContext(String contextCfgItemName) {
    synchronized (contexts) {
      AllocatorPerContext l = contexts.get(contextCfgItemName);
      if (l == null) {
        contexts.put(contextCfgItemName,
                    (l = new AllocatorPerContext(contextCfgItemName))
        );
      }
      return l;
    }
  }

  /** Get a path from the local FS. This method should be used if the size of
   *  the file is not known apriori. We go round-robin over the set of disks
   *  (via the configured dirs) and return the first complete path where
   *  we could create the parent directory of the passed path.
   *  @param pathStr the requested path (this will be created on the first
   *  available disk)
   *  @param conf the Configuration object
   *  @return the complete path to the file on a local disk
   *  @throws IOException raised on errors performing I/O.
   */
  public Path getLocalPathForWrite(String pathStr,
      Configuration conf) throws IOException {
    return getLocalPathForWrite(pathStr, SIZE_UNKNOWN, conf);
  }

  /** Get a path from the local FS. Pass size as
   *  SIZE_UNKNOWN if not known apriori. We
   *  round-robin over the set of disks (via the configured dirs) and return
   *  the first complete path which has enough space
   *  @param pathStr the requested path (this will be created on the first
   *  available disk)
   *  @param size the size of the file that is going to be written
   *  @param conf the Configuration object
   *  @return the complete path to the file on a local disk
   *  @throws IOException raised on errors performing I/O.
   */
  public Path getLocalPathForWrite(String pathStr, long size,
      Configuration conf) throws IOException {
    return getLocalPathForWrite(pathStr, size, conf, true);
  }

  /** Get a path from the local FS. Pass size as
   *  SIZE_UNKNOWN if not known apriori. We
   *  round-robin over the set of disks (via the configured dirs) and return
   *  the first complete path which has enough space
   *  @param pathStr the requested path (this will be created on the first
   *  available disk)
   *  @param size the size of the file that is going to be written
   *  @param conf the Configuration object
   *  @param checkWrite ensure that the path is writable
   *  @return the complete path to the file on a local disk
   *  @throws IOException raised on errors performing I/O.
   */
  public Path getLocalPathForWrite(String pathStr, long size,
                                   Configuration conf,
                                   boolean checkWrite) throws IOException {
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.getLocalPathForWrite(pathStr, size, conf, checkWrite);
  }

  /** Get a path from the local FS for reading. We search through all the
   *  configured dirs for the file's existence and return the complete
   *  path to the file when we find one
   *  @param pathStr the requested file (this will be searched)
   *  @param conf the Configuration object
   *  @return the complete path to the file on a local disk
   *  @throws IOException raised on errors performing I/O.
   */
  public Path getLocalPathToRead(String pathStr,
      Configuration conf) throws IOException {
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.getLocalPathToRead(pathStr, conf);
  }

  /**
   * Get all of the paths that currently exist in the working directories.
   * @param pathStr the path underneath the roots
   * @param conf the configuration to look up the roots in
   * @return all of the paths that exist under any of the roots
   * @throws IOException raised on errors performing I/O.
   */
  public Iterable<Path> getAllLocalPathsToRead(String pathStr,
                                               Configuration conf
                                               ) throws IOException {
    AllocatorPerContext context;
    synchronized (this) {
      context = obtainContext(contextCfgItemName);
    }
    return context.getAllLocalPathsToRead(pathStr, conf);
  }

  /** Creates a temporary file in the local FS. Pass size as -1 if not known
   *  apriori. We round-robin over the set of disks (via the configured dirs)
   *  and select the first complete path which has enough space. A file is
   *  created on this directory. The file is guaranteed to go away when the
   *  JVM exits.
   *  @param pathStr prefix for the temporary file
   *  @param size the size of the file that is going to be written
   *  @param conf the Configuration object
   *  @return a unique temporary file
   *  @throws IOException raised on errors performing I/O.
   */
  public File createTmpFileForWrite(String pathStr, long size,
      Configuration conf) throws IOException {
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.createTmpFileForWrite(pathStr, size, conf);
  }

  /**
   * Method to check whether a context is valid.
   * @param contextCfgItemName contextCfgItemName.
   * @return true/false
   */
  public static boolean isContextValid(String contextCfgItemName) {
    synchronized (contexts) {
      return contexts.containsKey(contextCfgItemName);
    }
  }

  /**
   * Removes the context from the context config items.
   *
   * @param contextCfgItemName contextCfgItemName.
   */
  @Deprecated
  @InterfaceAudience.LimitedPrivate({"MapReduce"})
  public static void removeContext(String contextCfgItemName) {
    synchronized (contexts) {
      contexts.remove(contextCfgItemName);
    }
  }

  /**
   *  We search through all the configured dirs for the file's existence
   *  and return true when we find.
   *  @param pathStr the requested file (this will be searched)
   *  @param conf the Configuration object
   *  @return true if files exist. false otherwise
   */
  public boolean ifExists(String pathStr, Configuration conf) {
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.ifExists(pathStr, conf);
  }

  /**
   * Get the current directory index for the given configuration item.
   * @return the current directory index for the given configuration item.
   */
  int getCurrentDirectoryIndex() {
    AllocatorPerContext context = obtainContext(contextCfgItemName);
    return context.getCurrentDirectoryIndex();
  }

  private static class AllocatorPerContext {

    private static final Logger LOG =
        LoggerFactory.getLogger(AllocatorPerContext.class);

    private Random dirIndexRandomizer = new Random();
    private String contextCfgItemName;

    // NOTE: the context must be accessed via a local reference as it
    //       may be updated at any time to reference a different context
    private AtomicReference<Context> currentContext;

    private static class Context {
      private AtomicInteger dirNumLastAccessed = new AtomicInteger(0);
      private FileSystem defaultFS;
      // private DF[] dirDF;
      private Path[] localDirs;
      private String savedLocalDirs;

      public int getAndIncrDirNumLastAccessed() {
        return getAndIncrDirNumLastAccessed(1);
      }

      public int getAndIncrDirNumLastAccessed(int delta) {
        if (localDirs.length < 2 || delta == 0) {
          return dirNumLastAccessed.get();
        }
        int oldval, newval;
        do {
          oldval = dirNumLastAccessed.get();
          newval = (oldval + delta) % localDirs.length;
        } while (!dirNumLastAccessed.compareAndSet(oldval, newval));
        return oldval;
      }
    }

    public AllocatorPerContext(String contextCfgItemName) {
      this.contextCfgItemName = contextCfgItemName;
      this.currentContext = new AtomicReference<Context>(new Context());
    }

    /** This method gets called everytime before any read/write to make sure
     * that any change to localDirs is reflected immediately.
     */
    private Context confChanged(Configuration conf)
        throws IOException {
      Context ctx = currentContext.get();
      String newLocalDirs = conf.get(contextCfgItemName);
      if (null == newLocalDirs) {
        throw new IOException(contextCfgItemName + " not configured");
      }
      if (!newLocalDirs.equals(ctx.savedLocalDirs)) {
        ctx = new Context();
        String[] dirStrings = StringUtils.getTrimmedStrings(newLocalDirs);
        ctx.defaultFS = FileSystem.get(conf);
        int numDirs = dirStrings.length;
        ArrayList<Path> dirs = new ArrayList<Path>(numDirs);
        // ArrayList<DF> dfList = new ArrayList<DF>(numDirs);
        for (int i = 0; i < numDirs; i++) {
          try {
            // filter problematic directories
            Path tmpDir = Path.getPathWithoutSchemeAndAuthority(new Path(dirStrings[i]));
            if(ctx.defaultFS.mkdirs(tmpDir) || ctx.defaultFS.exists(tmpDir)) {
              dirs.add(tmpDir);
            }
            else {
              LOG.warn("Failed to create " + dirStrings[i]);
            }
          } catch (IOException ie) {
            LOG.warn("Failed to create " + dirStrings[i] + ": " +
                ie.getMessage() + "\n", ie);
          } //ignore
        }
        ctx.localDirs = dirs.toArray(new Path[dirs.size()]);
        // ctx.dirDF = dfList.toArray(new DF[dirs.size()]);
        ctx.savedLocalDirs = newLocalDirs;

        if (dirs.size() > 0) {
          // randomize the first disk picked in the round-robin selection
          ctx.dirNumLastAccessed.set(dirIndexRandomizer.nextInt(dirs.size()));
        }

        currentContext.set(ctx);
      }

      return ctx;
    }

    private Path createPath(Path dir, String path,
        boolean checkWrite) throws IOException {
      Path file = new Path(dir, path);
      return file;
    }

    /**
     * Get the current directory index.
     * @return the current directory index.
     */
    int getCurrentDirectoryIndex() {
      return currentContext.get().dirNumLastAccessed.get();
    }

    /**
     * Format a string, log at debug and append it to the history as a new line.
     *
     * @param history history to fill in
     * @param fmt format string
     * @param args varags
     */
    private void note(StringBuilder history, String fmt, Object... args) {
      try {
        final String s = String.format(fmt, args);
        history.append(s).append("\n");
        LOG.debug(s);
      } catch (Exception e) {
        // some resilience in case the format string is wrong
        LOG.debug(fmt, e);
      }
    }

    /** Get a path from the local FS. If size is known, we go
     *  round-robin over the set of disks (via the configured dirs) and return
     *  the first complete path which has enough space.
     *
     *  If size is not known, use roulette selection -- pick directories
     *  with probability proportional to their available space.
     */
    public Path getLocalPathForWrite(String pathStr, long size,
        Configuration conf, boolean checkWrite) throws IOException {

      // history is built up and logged at error if the alloc
      StringBuilder history = new StringBuilder();

      note(history, "Searching for a directory for file \"%s\", size = %,d; checkWrite=%s",
          pathStr, size, checkWrite);
      Context ctx = confChanged(conf);
      int numDirs = ctx.localDirs.length;
      int numDirsSearched = 0;
      // Max capacity in any directory
      long maxCapacity = 0;
      String errorText = null;
      IOException diskException = null;
      //remove the leading slash from the path (to make sure that the uri
      //resolution results in a valid path on the dir being checked)
      if (pathStr.startsWith("/")) {
        pathStr = pathStr.substring(1);
      }
      Path returnPath = null;

      final int dirCount = ctx.localDirs.length;
      long[] availableOnDisk = new long[dirCount];
      long totalAvailable = 0;

      StringBuilder pathNames = new StringBuilder();

      //build the "roulette wheel"
      for (int i =0; i < dirCount; ++i) {
        final Path target = ctx.localDirs[i];
        // attempt to recreate the dir so that getAvailable() is valid
        // if it fails, getAvailable() will return 0, so the dir will
        // be declared unavailable.
        // return value is logged at debug to keep spotbugs quiet.
        final String name = target.toString(); // JTA : target is already a HDFS absolute path
        pathNames.append(" ").append(name);
        final Path dirPath = new Path(name);

        // existence probe with directory recreation
        if (!ctx.defaultFS.exists(dirPath)) {
          LOG.debug("Creating buffer dir {}", name);
          if (ctx.defaultFS.mkdirs(dirPath)) {
            note(history, "Created buffer dir %s", name);
          } else {
            note(history, "Failed to create buffer dir %s", name);
          }
        }

        // path already existed or the mkdir call had an outcome
        // make sure the path is present and a dir, and if so add its availability
        if (ctx.defaultFS.getFileLinkStatus(dirPath).isDirectory()) {
          final long available = ctx.defaultFS.getStatus().getCapacity(); // JTA
          availableOnDisk[i] = available;
          note(history, "%,d bytes available under path %s",  available, name);
          totalAvailable += availableOnDisk[i];
        } else {
          note(history, "%s does not exist/is not a directory", name);
        }
      }

      note(history, "Directory count is %d; total available capacity is %,d",
          dirCount, totalAvailable);

      if (size == SIZE_UNKNOWN) {
        //do roulette selection: pick dir with probability
        // proportional to available size
        note(history, "Size not specified, so picking directories at random.");

        if (totalAvailable == 0) {
          // log error and history
          String newErrorText = E_NO_SPACE_AVAILABLE + pathNames
              + " on host" + getHostname();
          LOG.error(newErrorText);
          LOG.error(history.toString());
          // then raise the exception
          throw new DiskErrorException(newErrorText);
        }

        // Keep rolling the wheel till we get a valid path
        Random r = new java.util.Random();
        while (numDirsSearched < numDirs && returnPath == null) {
          long randomPosition = (r.nextLong() >>> 1) % totalAvailable;
          int dir = 0;
          while (randomPosition > availableOnDisk[dir]) {
            randomPosition -= availableOnDisk[dir];
            dir++;
          }
          ctx.dirNumLastAccessed.set(dir);
          final Path localDir = ctx.localDirs[dir];
          returnPath = createPath(localDir, pathStr, checkWrite);
          if (returnPath == null) {
            totalAvailable -= availableOnDisk[dir];
            availableOnDisk[dir] = 0; // skip this disk
            numDirsSearched++;
            note(history, "No capacity in %s", localDir);
          } else {
            note(history, "Allocated file %s in %s", returnPath, localDir);
          }
        }
      } else {
        note(history, "Requested file size is %,d; searching for a suitable directory",
            size);
        // Start linear search with random increment if possible
        int randomInc = 1;
        if (numDirs > 2) {
          randomInc += dirIndexRandomizer.nextInt(numDirs - 1);
        }
        int dirNum = ctx.getAndIncrDirNumLastAccessed(randomInc);
        while (numDirsSearched < numDirs) {
          // ctx.dirDF[dirNum].getAvailable();
          long capacity = ctx.defaultFS.getStatus().getCapacity(); // JTA

          if (capacity > maxCapacity) {
            maxCapacity = capacity;
          }
          if (capacity > size) {
            final Path localDir = ctx.localDirs[dirNum];
            try {
              returnPath = createPath(localDir, pathStr, checkWrite);
            } catch (IOException e) {
              errorText = e.getMessage();
              diskException = e;
              note(history, "Exception while creating path %s: %s", localDir, errorText);
              LOG.debug("DiskException caught for dir {}", localDir, e);
            }
            if (returnPath != null) {
              // success
              ctx.getAndIncrDirNumLastAccessed(numDirsSearched);
              note(history, "Allocated file %s in %s", returnPath, localDir);
              break;
            } else {
              note(history, "No capacity in %s", localDir);
            }
          }
          dirNum++;
          dirNum = dirNum % numDirs;
          numDirsSearched++;
        }
      }
      if (returnPath != null) {
        return returnPath;
      }

      //no path found
      String hostname = getHostname();
      String newErrorText = "Could not find any valid local directory for "
          + pathStr + " with requested size " + size
          + " on host " + hostname
          + " as the max capacity in any directory"
          + " (" + pathNames + " )"
          + " is " + maxCapacity;
      if (errorText != null) {
        newErrorText = newErrorText + " due to " + errorText;
      }
      LOG.error(newErrorText);
      LOG.error(history.toString());
      throw new DiskErrorException(newErrorText, diskException);
    }

    /** Creates a file on the local FS. Pass size as
     * {@link LocalDirAllocator.SIZE_UNKNOWN} if not known apriori. We
     *  round-robin over the set of disks (via the configured dirs) and return
     *  a file on the first path which has enough space. The file is guaranteed
     *  to go away when the JVM exits.
     */
    public File createTmpFileForWrite(String pathStr, long size,
        Configuration conf) throws IOException {

      Context ctx = confChanged(conf); // JTA

      // find an appropriate directory
      LOG.warn("!!!!!!!!!! pathStr = " + pathStr + " !!!!!!!!!!");
      Path path = getLocalPathForWrite(pathStr, size, conf, true);
      File dir = new File(path.getParent().toUri().getPath());
      String prefix = path.getName();

      // create a temp file on this directory
      FileSystem.create(ctx.defaultFS, path, null); // JTA
      File result = File.createTempFile(prefix, null, dir);
      result.deleteOnExit();
      return result;
    }

    /** Get a path from the local FS for reading. We search through all the
     *  configured dirs for the file's existence and return the complete
     *  path to the file when we find one
     */
    public Path getLocalPathToRead(String pathStr,
        Configuration conf) throws IOException {
      Context ctx = confChanged(conf);
      int numDirs = ctx.localDirs.length;
      int numDirsSearched = 0;
      //remove the leading slash from the path (to make sure that the uri
      //resolution results in a valid path on the dir being checked)
      if (pathStr.startsWith("/")) {
        pathStr = pathStr.substring(1);
      }
      while (numDirsSearched < numDirs) {
        Path file = new Path(ctx.localDirs[numDirsSearched], pathStr);
        if (ctx.defaultFS.exists(file)) {
          return file;
        }
        numDirsSearched++;
      }

      //no path found
      throw new DiskErrorException ("Could not find " + pathStr +" in any of" +
      " the configured local directories");
    }

    private static class PathIterator implements Iterator<Path>, Iterable<Path> {
      private final FileSystem fs;
      private final String pathStr;
      private int i = 0;
      private final Path[] rootDirs;
      private Path next = null;

      private PathIterator(FileSystem fs, String pathStr, Path[] rootDirs)
          throws IOException {
        this.fs = fs;
        this.pathStr = pathStr;
        this.rootDirs = rootDirs;
        advance();
      }

      @Override
      public boolean hasNext() {
        return next != null;
      }

      private void advance() throws IOException {
        while (i < rootDirs.length) {
          next = new Path(rootDirs[i++], pathStr);
          if (fs.exists(next)) {
            return;
          }
        }
        next = null;
      }

      @Override
      public Path next() {
        final Path result = next;
        try {
          advance();
        } catch (IOException ie) {
          throw new RuntimeException("Can't check existence of " + next, ie);
        }
        if (result == null) {
          throw new NoSuchElementException();
        }
        return result;
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("read only iterator");
      }

      @Override
      public Iterator<Path> iterator() {
        return this;
      }
    }

    /**
     * Get all of the paths that currently exist in the working directories.
     * @param pathStr the path underneath the roots
     * @param conf the configuration to look up the roots in
     * @return all of the paths that exist under any of the roots
     * @throws IOException
     */
    Iterable<Path> getAllLocalPathsToRead(String pathStr,
        Configuration conf) throws IOException {
      Context ctx = confChanged(conf);
      if (pathStr.startsWith("/")) {
        pathStr = pathStr.substring(1);
      }
      return new PathIterator(ctx.defaultFS, pathStr, ctx.localDirs);
    }

    /** We search through all the configured dirs for the file's existence
     *  and return true when we find one
     */
    public boolean ifExists(String pathStr, Configuration conf) {
      Context ctx = currentContext.get();
      try {
        int numDirs = ctx.localDirs.length;
        int numDirsSearched = 0;
        //remove the leading slash from the path (to make sure that the uri
        //resolution results in a valid path on the dir being checked)
        if (pathStr.startsWith("/")) {
          pathStr = pathStr.substring(1);
        }
        while (numDirsSearched < numDirs) {
          Path file = new Path(ctx.localDirs[numDirsSearched], pathStr);
          if (ctx.defaultFS.exists(file)) {
            return true;
          }
          numDirsSearched++;
        }
      } catch (IOException e) {
        // IGNORE and try again
      }
      return false;
    }
  }
}
