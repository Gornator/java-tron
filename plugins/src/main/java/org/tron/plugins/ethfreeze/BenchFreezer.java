package org.tron.plugins.ethfreeze;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.utils.ByteUtil;
import org.xerial.snappy.Snappy;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Slf4j(topic = "DB")
@CommandLine.Command(name = "benchfreezer",
    description = "Benchmark freezer's delay of reading one block.")
public class BenchFreezer implements Callable<Integer> {

  @Option(names = {"-m", "--min"},
      description = "min block num")
  public long minBlockNum;

  @Option(names = {"-M", "--max"},
      description = "max block num")
  public long maxBlockNum;

  @Option(names = {"-c", "--count"},
      description = "test times")
  public int testTimes;

  private Map<Integer, RandomAccessFile> index2Input = new HashMap<>();
  private byte[] cIdx;

  private long readIndexCost = 0;
  private long seekCost = 0;
  private long readDataCost = 0;
  private long unCompressCost = 0;

  private void readIndex() throws IOException {
    File indexFile = new File("ancient/block.cidx");
    FileInputStream in = new FileInputStream(indexFile);

    ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
    byte[] temp = new byte[1024];
    int size;
    while ((size = in.read(temp)) != -1) {
      out.write(temp, 0, size);
    }
    in.close();
    cIdx = out.toByteArray();
    out.close();
  }

  public BenchFreezer() throws IOException {
    readIndex();
  }

  public BenchFreezer(long minBlockNum, long maxBlockNum, int testTimes) throws IOException {
    this.minBlockNum = minBlockNum;
    this.maxBlockNum = maxBlockNum;
    this.testTimes = testTimes;
    readIndex();
  }

  private int[] getFileAndOffset(byte[] index) {
    //get file number
    byte[] numberByte = new byte[2];
    System.arraycopy(index, 0, numberByte, 0, 2);
    int fileNum = ByteUtil.byteArrayToInt(numberByte);

    //get start
    byte[] offsetByte = new byte[4];
    System.arraycopy(index, 2, offsetByte, 0, 4);
    int offset = ByteUtil.byteArrayToInt(offsetByte);

    return new int[] {fileNum, offset};
  }

  public byte[] readBlockNum(long blockNum) throws IOException {
    long t1 = System.currentTimeMillis();
    int start = (int) (blockNum - minBlockNum) * 6;
    int middle = (int) (blockNum - minBlockNum + 1) * 6;

    byte[] cur = new byte[6];
    byte[] next = null;
    System.arraycopy(cIdx, start, cur, 0, 6);
    if (middle < cIdx.length) {
      next = new byte[6];
      System.arraycopy(cIdx, middle, next, 0, 6);
    }

    int[] curData = getFileAndOffset(cur);

    int fileNum = curData[0];
    int offset = curData[1];
    //logger.info("block number {}, file number {}, offset {}", blockNum, fileNum, offset);
    int size = 0;
    boolean readToEnd = false;

    //get read size. if no next or next' block != cur, read end
    if (next != null) {
      int[] nextData = getFileAndOffset(next);
      if (curData[0] == nextData[0]) {
        size = nextData[1] - curData[1];
      } else {
        readToEnd = true;
      }
    } else {
      readToEnd = true;
    }

    RandomAccessFile randomAccessFile = index2Input.get(fileNum);
    if (randomAccessFile == null) {
      File dataFile = new File(String.format("ancient/block.%04d.cdat", fileNum));
      randomAccessFile = new RandomAccessFile(dataFile, "r");
      index2Input.put(fileNum, randomAccessFile);
    }
    readIndexCost += (System.currentTimeMillis() - t1);

    long t2 = System.currentTimeMillis();
    randomAccessFile.seek(offset);
    seekCost += (System.currentTimeMillis() - t2);

    long t3 = System.currentTimeMillis();
    if (readToEnd) {
      size = (int) (randomAccessFile.length() - offset);
    }
    byte[] data = new byte[size];
    randomAccessFile.readFully(data);
    readDataCost += (System.currentTimeMillis() - t3);

    long t4 = System.currentTimeMillis();
    byte[] unCompressData = Snappy.uncompress(data);
    unCompressCost += (System.currentTimeMillis() - t4);

    //try {
    //  BlockCapsule blockCapsule = new BlockCapsule(unCompressData);
    //  if (blockCapsule.getNum() != blockNum) {
    //    logger.error("Parse error, expect block {}, real block {}", blockNum,
    //        blockCapsule.getNum());
    //  }
    //} catch (BadItemException e) {
    //  throw new RuntimeException(e);
    //}
    return unCompressData;
  }


  public void bench() throws IOException {
    long start = System.currentTimeMillis();
    //for (long i = benchMarkFreezer.minBlockNum; i < benchMarkFreezer.maxBlockNum; i++) {
    //  benchMarkFreezer.readBlockNum(i);
    //}
    Random random = new Random();
    int count = 0;
    long lastTime = start;
    while (count < testTimes) {
      int offset = random.nextInt((int) (maxBlockNum - minBlockNum));
      readBlockNum(minBlockNum + offset);
      count += 1;

      if (count % 10000 == 0) {
        long end = System.currentTimeMillis();
        logger.info("read block count {}, delta {} ms, readIndexCost {} ms, seekCost {} ms,"
                + " readDataCost {} ms, unCompressCost {} ms, cost {} ms",
            count, end - lastTime, readIndexCost, seekCost, readDataCost, unCompressCost,
            end - start);
        readIndexCost = 0;
        seekCost = 0;
        readDataCost = 0;
        unCompressCost = 0;
        lastTime = end;
      }
    }
  }

  @Override
  public Integer call() throws Exception {
    bench();
    return 0;
  }

  public static void main(String[] args) throws IOException {
    BenchFreezer benchMarkFreezer = new BenchFreezer(50536001, 50601536, 1_000_000);
    benchMarkFreezer.bench();
  }
}
