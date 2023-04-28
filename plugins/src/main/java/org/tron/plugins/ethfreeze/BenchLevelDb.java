package org.tron.plugins.ethfreeze;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.tron.common.utils.ByteUtil;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.exception.BadItemException;
import org.tron.plugins.utils.DBUtils;
import org.tron.plugins.utils.db.LevelDBIterator;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Slf4j(topic = "DB")
@CommandLine.Command(name = "benchleveldb",
    description = "Benchmark level db's delay of reading one block.")
public class BenchLevelDb implements Callable<Integer> {

  @Option(names = {"-m", "--min"},
      description = "min block num")
  public long minBlockNum;

  @Option(names = {"-M", "--max"},
      description = "max block num")
  public long maxBlockNum;

  @Option(names = {"-c", "--count"},
      description = "test times")
  public int testTimes;

  String dbDir = "output-directory"; //jar包当前平级目录

  public BenchLevelDb() {
  }

  public BenchLevelDb(long minBlockNum, long maxBlockNum, int testTimes) throws IOException {
    this.minBlockNum = minBlockNum;
    this.maxBlockNum = maxBlockNum;
    this.testTimes = testTimes;
  }

  public void bench() throws IOException, BadItemException {
    long start = System.currentTimeMillis();

    Path path = Paths.get(dbDir, "database", "block");

    DB db = DBUtils.newLevelDb(path);
    DBIterator dbIterator = db.iterator();
    LevelDBIterator levelDBIterator = new LevelDBIterator(dbIterator);
    levelDBIterator.seekToFirst();

    Random random = new Random();
    int count = 0;
    while (count < testTimes) {
      int blockNum = random.nextInt((int) (maxBlockNum - minBlockNum));
      byte[] blockBytes = ByteUtil.longTo32Bytes(blockNum);
      levelDBIterator.seek(blockBytes);
      if (!levelDBIterator.valid()) {
        logger.error("Cannot find block {}", blockNum);
      }
      levelDBIterator.getKey();
      byte[] data = levelDBIterator.getValue();

      BlockCapsule blockCapsule = new BlockCapsule(data);

      count += 1;

      if (count % 10000 == 0) {
        long end = System.currentTimeMillis();
        logger.info("read block count {}, cost {} ms", count, end - start);
      }
    }
  }

  @Override
  public Integer call() throws Exception {
    bench();
    return 0;
  }

  public static void main(String[] args) throws IOException, BadItemException {
    BenchLevelDb benchLevelDb = new BenchLevelDb(50536001, 50601536, 1_000_000);
    benchLevelDb.bench();
  }
}
