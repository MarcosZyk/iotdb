package org.apache.iotdb.db.metadata.mtree.store.disk.schemafile;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.SchemaPageOverflowException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;


/**
 * TODO: reliability design TBD
 *
 * This class is mainly aimed to manage space all over the file.
 *
 * This class is meant to open a .pmt(Persistent MTree) file, and maintains the header of the file.
 * It Loads or writes a page length bytes at once, with an 32 bits int to index a page inside a file.
 * Use SlottedFile to manipulate segment(sp) inside a page(an array of bytes).
 * */
public class SchemaFile implements ISchemaFile{

  private static final Logger logger = LoggerFactory.getLogger(SchemaFile.class);

  public static int FILE_HEADER_SIZE = 256;  // size of file header in bytes
  public static String FILE_FOLDER = "./pst/";  // folder to store .pmt files

  public static int PAGE_LENGTH = 16 * 1024;  // 16 kib for default
  public static short PAGE_HEADER_SIZE = 16;
  public static int PAGE_CACHE_SIZE = 48;  // size of page cache
  public static int ROOT_INDEX = 0;  // index of header page
  public static int INDEX_LENGTH = 4;  // 32 bit for page pointer, maximum .pmt file as 2^(32+14) bytes, 64 TiB

  public static short SEG_OFF_DIG = 2; // byte length of short, which is the type of index of segment inside a page
  public static short SEG_MAX_SIZ = (short)(16*1024 - SchemaFile.PAGE_HEADER_SIZE - SEG_OFF_DIG);
  public static short[] SEG_SIZE_LST = {1024, 2*1024, 4*1024, 8*1024, SEG_MAX_SIZ};
  public static int SEG_INDEX_DIGIT = 16;  // for type short
  public static long SEG_INDEX_MASK = 0xffffL;  // to translate address

  // attributes for this schema file
  String filePath;
  String storageGroupName;

  ByteBuffer headerContent;
  int lastPageIndex;  // last page index of the file, boundary to grow

  // work as a naive cache for page instance
  Map<Integer, ISchemaPage> pageInstCache;
  ISchemaPage rootPage;

  // attributes inside current page

  FileChannel channel;

  public SchemaFile(String sgName) throws IOException, MetadataException {
    this(sgName, false);
  }

  public SchemaFile(String sgName, boolean override) throws IOException, MetadataException {
    this.storageGroupName = sgName;
    filePath = SchemaFile.FILE_FOLDER + sgName + ".pmt";

    pageInstCache = new LinkedHashMap<>(PAGE_CACHE_SIZE, 1, true);

    File pmtFile = new File(filePath);

    if (pmtFile.exists() && override) {
      Files.delete(Paths.get(pmtFile.toURI()));
    }

    if (!pmtFile.exists() || !pmtFile.isFile()) {
      File folder = new File(SchemaFile.FILE_FOLDER);
      folder.mkdirs();
      pmtFile.createNewFile();
    }

    channel = new RandomAccessFile(pmtFile, "rw").getChannel();
    headerContent = ByteBuffer.allocate(SchemaFile.FILE_HEADER_SIZE);
    initFileHeader();
  }

  // region Interface Implementation

  @Override
  public void writeMNode(IMNode node) throws MetadataException, IOException {
    int pageIndex;
    short curSegIdx;
    ISchemaPage curPage;

    // Get corresponding page instance, segment id
    long curSegAddr = node.getChildren().getSegment().getSegmentAddress();
    if (curSegAddr < 0) {
      if (node.getParent() == null) {
        // root node
        curPage = getRootPage();
        pageIndex = curPage.getPageIndex();
        curSegIdx = 0;
      } else {
        throw new MetadataException("Cannot store a node without segment address except for root.");
      }
    } else {
      pageIndex = SchemaFile.getPageIndex(curSegAddr);
      curSegIdx = SchemaFile.getSegIndex(curSegAddr);
      curPage = getPageInstance(pageIndex);
    }

    // Write new child
    for(Map.Entry<String, IMNode> entry:
        node.getChildren().getSegment().getNewChildBuffer().entrySet()) {

      // Translate node.child into buffer and Pre-Allocate segment for internal child.
      IMNode child = entry.getValue();
      if (!child.isMeasurement()) {
        if (child.getChildren().getSegment().getSegmentAddress() < 0) {
          short estSegSize = estimateSegmentSize(child);
          long glbIndex = preAllocateSegment(estSegSize);
          child.getChildren().getSegment().setSegmentAddress(glbIndex);
        } else {
          // new child with a valid segment address, weird
          throw new MetadataException("A child in newChildBuffer shall not have segmentAddress.");
        }
      }

      ByteBuffer childBuffer = RecordUtils.node2Buffer(child);

      // Write and Handle Overflow
      // (inside curPage.write)
      // write to curSeg, return 0 if succeeded, positive if next page, exception if no next and no spare
      // (inside this method)
      // get next segment if existed, retry till succeeded or exception
      // allocate new page if exception, link or transplant by original segment size, write buffer to new segment
      // throw exception if record larger than one page
      try {
        long npAddress = curPage.write(curSegIdx, entry.getKey(), childBuffer);
        while (npAddress > 0) {
          // get next page and retry
          pageIndex = SchemaFile.getPageIndex(npAddress);
          curSegIdx = SchemaFile.getSegIndex(npAddress);
          curPage = getPageInstance(pageIndex);
          npAddress = curPage.write(curSegIdx, entry.getKey(), childBuffer);
        }

      } catch (SchemaPageOverflowException e) {
        // there is no more next page, need allocate new page
        short newSegSize = SchemaFile.reEstimateSegSize(curPage.getSegmentSize(curSegIdx));
        ISchemaPage newPage = getMinApplicablePageInMem(newSegSize);

        if (newSegSize == curPage.getSegmentSize(curSegIdx)) {
          // segment on multi pages
          short newSegId = newPage.allocNewSegment(newSegSize);
          long newSegAddr = SchemaFile.getGlobalIndex(newPage.getPageIndex(), newSegId);

          // note that it doesn't modify address of node nor parental record
          newPage.setPrevSegAddress(newSegId, curSegAddr);
          curPage.setNextSegAddress(curSegIdx, newSegAddr);

          curSegAddr = newSegAddr;
        } else {
          // segment on a single new page
          curSegAddr = newPage.transplantSegment(curPage, curSegIdx, newSegSize);

          curPage.deleteSegment(curSegIdx);

          curSegIdx = SchemaFile.getSegIndex(curSegAddr);
          node.getChildren().getSegment().setSegmentAddress(curSegAddr);
          updateParentalRecord(node.getParent(), node.getName(), curSegAddr);
        }
        curPage = newPage;
        curPage.write(curSegIdx, entry.getKey(), childBuffer);
      }
    }

    // Write updated child
    for(Map.Entry<String, IMNode> entry:
        node.getChildren().getSegment().getUpdatedChildBuffer().entrySet()) {
      // Translate child into reocrd
      IMNode child = entry.getValue();
      ByteBuffer childBuffer = RecordUtils.node2Buffer(child);

      // Get segment actually contains the record
      long actualSegAddr = getTargetSegmentAddress(curSegAddr, entry.getKey());
      curPage = getPageInstance(getPageIndex(actualSegAddr));
      curSegIdx = getSegIndex(actualSegAddr);

      try {
        // if current segment has no more space for new record, it will re-allocate segment, if failed, throw exception
        curPage.update(curSegIdx, entry.getKey(), childBuffer);

      } catch (SchemaPageOverflowException e) {
        short curSegSize = curPage.getSegmentSize(curSegIdx);
        long existedSegAddr = getApplicableLinkedSegments(curPage, curSegIdx, entry.getKey(), childBuffer);
        if (existedSegAddr >= 0) {
          // get another existed segment
          curPage = getPageInstance(getPageIndex(existedSegAddr));
          curSegIdx = getSegIndex(existedSegAddr);
        } else {
          // no next segment to write
          short newSegSize = reEstimateSegSize(curSegSize);
          ISchemaPage newPage = getMinApplicablePageInMem(newSegSize);
          long newSegAddr = newPage.transplantSegment(curPage, curSegIdx, newSegSize);
          short newSegId = getSegIndex(newSegAddr);

          // and new seg to list for multi-page segment, modify parental record for single segment
          if (curSegSize == SEG_MAX_SIZ) {
            // allocate new page and expand max-segment-lists
            long nextSegAddr = curPage.getNextSegAddress(curSegIdx);

            // link new seg as the next to the cur, prev to the original next if existed
            if (nextSegAddr >= 0) {
              ISchemaPage nextPage = getPageInstance(getPageIndex(nextSegAddr));

              newPage.setNextSegAddress(newSegId, nextSegAddr);
              nextPage.setPrevSegAddress(getSegIndex(newSegAddr), newSegAddr);
            }

            curPage.setNextSegAddress(curSegIdx, newSegAddr);
            newPage.setPrevSegAddress(newSegId, curSegAddr);

          } else {
            // modify parental record and node
            curPage.deleteSegment(curSegIdx);
            node.getChildren().getSegment().setSegmentAddress(newSegAddr);
            updateParentalRecord(node.getParent(), node.getName(), newSegAddr);
          }

          curPage = newPage;
          curSegAddr = newSegAddr;
          curSegIdx = newSegId;
        }

        if (existedSegAddr >= 0 || curSegSize == SEG_MAX_SIZ) {
          // remove from original segment
          getPageInstance(getPageIndex(actualSegAddr)).removeRecord(getSegIndex(actualSegAddr), entry.getKey());
          // flush updated record into another linked segment, or a fresh full-page segment
          curPage.write(curSegIdx, entry.getKey(), childBuffer);
        } else {
          // updated on an extended segment
          curPage.update(curSegIdx, entry.getKey(), childBuffer);
        }
      }
    }
  }

  @Override
  public void delete(IMNode node) throws IOException, MetadataException{
    long recSegAddr = node.getParent() == null ? ROOT_INDEX : node.getParent().getChildren().getSegment().getSegmentAddress();
    recSegAddr = getTargetSegmentAddress(recSegAddr, node.getName());
    getPageInstance(getPageIndex(recSegAddr)).removeRecord(getSegIndex(recSegAddr), node.getName());

    if (!node.isMeasurement()) {
      long delSegAddr = node.getChildren().getSegment().getSegmentAddress();
      getPageInstance(getPageIndex(delSegAddr)).deleteSegment(getSegIndex(delSegAddr));
    }
  }

  @Override
  public IMNode getChildNode(IMNode parent, String childName) throws MetadataException, IOException{
    if (parent.isMeasurement()
    || parent.getChildren().getSegment().getSegmentAddress() < 0) {
      throw new MetadataException(String.format("Node [] has no child in schema file.", parent.getFullPath()));
    }

    int pageIdx = getPageIndex(parent.getChildren().getSegment().getSegmentAddress());
    ISchemaPage page = getPageInstance(pageIdx);
    return page.read(getSegIndex(parent.getChildren().getSegment().getSegmentAddress()), childName);
  }

  @Override
  public Iterator<IMNode> getChildren(IMNode parent) throws MetadataException, IOException{
    if (parent.isMeasurement()
        || parent.getChildren().getSegment().getSegmentAddress() < 0) {
      throw new MetadataException(String.format("Node [] has no child in schema file.", parent.getFullPath()));
    }

    int pageIdx = getPageIndex(parent.getChildren().getSegment().getSegmentAddress());
    short segId = getSegIndex(parent.getChildren().getSegment().getSegmentAddress());
    ISchemaPage page = getPageInstance(pageIdx);

    return new Iterator<IMNode>() {
      long nextSeg = page.getNextSegAddress(segId);
      Queue<IMNode> children = page.getChildren(segId);
      @Override
      public boolean hasNext() {
        if (children.size() == 0) {
          // actually, 0 can never be nextSeg forever
          if (nextSeg < 0) {
            return false;
          }
          try {
            ISchemaPage newPage = getPageInstance(getPageIndex(nextSeg));
            children.addAll(newPage.getChildren(getSegIndex(nextSeg)));
            nextSeg = newPage.getNextSegAddress(getSegIndex(nextSeg));
            return true;
          } catch (IOException | MetadataException e) {
            return false;
          }
        }
        return true;
      }

      @Override
      public IMNode next(){
        return children.poll();
      }
    };
  }

  @Override
  public void close() throws IOException {
    updateHeader();
    for (Map.Entry<Integer, ISchemaPage> entry: pageInstCache.entrySet()) {
      flushPageToFile(entry.getValue());
    }
    channel.close();
  }

  public String inspect() throws MetadataException, IOException {
    StringBuilder builder = new StringBuilder(String.format(
        "==================\n" +
            "==================\n" +
            "SchemaFile inspect: %s, totalPages:%d\n", storageGroupName, lastPageIndex));
    int cnt = 0;
    while (cnt <= lastPageIndex) {
      ISchemaPage page = getPageInstance(cnt);
      builder.append(String.format("----------\n%s\n", page.inspect()));
      cnt ++;
    }
    return builder.toString();
  }
  // endregion

  // region File Operations

  /**
   * File Header Structure:
   * 1 int (4 bytes): last page index
   * var length: root(SG) node info
   *    a. var length string (less than 200 bytes): path to root(SG) node
   *    b. fixed length buffer (13 bytes): internal or entity node buffer
   *    ... (Expected to extend for optimization) ...
   * */
  private void initFileHeader() throws IOException, MetadataException {
    if (channel.size() == 0) {
      // new schema file
      lastPageIndex = 0;
      ReadWriteIOUtils.write(lastPageIndex, headerContent);
      ReadWriteIOUtils.write(storageGroupName, headerContent);
      initRootPage();
    } else {
      channel.read(headerContent);
      headerContent.clear();
      lastPageIndex = ReadWriteIOUtils.readInt(headerContent);
      storageGroupName = ReadWriteIOUtils.readString(headerContent);
      rootPage = getPageInstance(0);
    }
  }

  private void updateHeader() throws IOException{
    headerContent.clear();

    ReadWriteIOUtils.write(lastPageIndex, headerContent);
    ReadWriteIOUtils.write(storageGroupName, headerContent);

    headerContent.clear();
    channel.write(headerContent, 0);
    channel.force(true);
  }

  private void initRootPage() throws IOException, MetadataException{
    if (rootPage == null) {
      rootPage = SchemaPage.initPage(ByteBuffer.allocate(PAGE_LENGTH), 0);
      rootPage.allocNewSegment(SEG_MAX_SIZ);

      lastPageIndex = 0;

      pageInstCache.put(rootPage.getPageIndex(), rootPage);
    }
  }


  // endregion
  // region Segment Address Operation

  private long getTargetSegmentAddress(long curSegAddr, String recKey) throws IOException, MetadataException {
    ISchemaPage curPage = getPageInstance(getPageIndex(curSegAddr));
    short curSegId = getSegIndex(curSegAddr);

    if (curPage.hasRecordKeyInSegment(recKey, curSegId)) {
      return curSegAddr;
    }

    long nextSegAddr = curPage.getNextSegAddress(curSegId);
    while (nextSegAddr >= 0) {
      ISchemaPage pivotPage = getPageInstance(getPageIndex(nextSegAddr));
      short pSegId = getSegIndex(nextSegAddr);
      if (pivotPage.hasRecordKeyInSegment(recKey, pSegId)) {
        return nextSegAddr;
      }
      nextSegAddr = pivotPage.getNextSegAddress(pSegId);
    }

    long prevSegAddr = curPage.getPrevSegAddress(curSegId);
    while (prevSegAddr >= 0) {
      ISchemaPage pivotPage = getPageInstance(getPageIndex(prevSegAddr));
      short pSegId = getSegIndex(prevSegAddr);
      if (pivotPage.hasRecordKeyInSegment(recKey, pSegId)) {
        return prevSegAddr;
      }
      prevSegAddr = pivotPage.getPrevSegAddress(pSegId);
    }
    return -1;
  }


  /**
   * To find a segment applicable for inserting (key, buffer), among pages linked to curPage.curSeg
   * @param curPage
   * @param curSegId
   * @param key
   * @param buffer
   * @return
   */
  private long getApplicableLinkedSegments(ISchemaPage curPage, short curSegId, String key, ByteBuffer buffer) throws IOException, MetadataException{
    if (curPage.getSegmentSize(curSegId) < SEG_MAX_SIZ) {
      // only max segment can be linked
      return -1;
    }

    short totalSize = (short)(key.getBytes().length + buffer.capacity() + 4 + 2);
    long nextSegAddr = curPage.getNextSegAddress(curSegId);
    while (nextSegAddr >= 0) {
      ISchemaPage nextPage = getPageInstance(getPageIndex(nextSegAddr));
      if (nextPage.isSegmentCapableFor(getSegIndex(nextSegAddr), totalSize)) {
        return nextSegAddr;
      }
      nextSegAddr = nextPage.getNextSegAddress(getSegIndex(nextSegAddr));
    }

    long prevSegAddr = curPage.getPrevSegAddress(curSegId);
    while (prevSegAddr >= 0) {
      ISchemaPage prevPage = getPageInstance(getPageIndex(prevSegAddr));
      if (prevPage.isSegmentCapableFor(getSegIndex(prevSegAddr), totalSize)) {
        return prevSegAddr;
      }
      prevSegAddr = prevPage.getPrevSegAddress(getSegIndex(prevSegAddr));
    }
    return -1;
  }

  private long preAllocateSegment(short size) throws IOException, MetadataException {
    ISchemaPage page = getMinApplicablePageInMem(size);
    return SchemaFile.getGlobalIndex(page.getPageIndex(), page.allocNewSegment(size));
  }

  // endregion

  // region Schema Page Operations

  private ISchemaPage getRootPage() {
    return rootPage;
  }

  /**
   * This method checks with cached page container, get a minimum applicable page for allocation.
   * @param size size of segment
   * @return
   */
  private ISchemaPage getMinApplicablePageInMem(short size) throws IOException {
    for (Map.Entry<Integer, ISchemaPage> entry: pageInstCache.entrySet()) {
      if (entry.getValue().isCapableForSize(size)) {
        return entry.getValue();
      }
    }
    return allocateNewPage();
  }

  /**
   * Get from cache, or load from file.
   * @param pageIdx target page index
   * @return an existed page
   */
  private ISchemaPage getPageInstance(int pageIdx) throws IOException, MetadataException {
    if (pageIdx > lastPageIndex) {
      throw new MetadataException(String.format("Page index %d out of range.", pageIdx));
    }

    if (pageInstCache.containsKey(pageIdx)) {
      return pageInstCache.get(pageIdx);
    }

    ByteBuffer newBuf = ByteBuffer.allocate(PAGE_LENGTH);

    loadFromFile(newBuf, pageIdx);
    addPageToCache(pageIdx, SchemaPage.loadPage(newBuf, pageIdx));
    return pageInstCache.get(pageIdx);
  }

  private ISchemaPage allocateNewPage() throws IOException{
    lastPageIndex += 1;
    ISchemaPage newPage = SchemaPage.initPage(ByteBuffer.allocate(PAGE_LENGTH), lastPageIndex);

    addPageToCache(newPage.getPageIndex(), newPage);

    return newPage;
  }

  private void addPageToCache(int pageIndex, ISchemaPage page) throws IOException {
    pageInstCache.put(pageIndex, page);
    if (pageInstCache.size() >= PAGE_CACHE_SIZE -1) {
      int removeCnt = (int)( 0.2 * PAGE_CACHE_SIZE );
      for (Map.Entry<Integer, ISchemaPage> entry: pageInstCache.entrySet()) {
        removeCnt --;
        entry.getValue().syncPageBuffer();
        flushPageToFile(entry.getValue());

        // release object
        pageInstCache.remove(entry.getKey());
        if (removeCnt == 0) {
          return;
        }
      }
    }
  }

  // endregion


  // region Utilities

  public static long getGlobalIndex(int pageIndex, short segIndex) {
    return ((pageIndex << SchemaFile.SEG_INDEX_DIGIT) | (segIndex & SchemaFile.SEG_INDEX_MASK));
  }

  public static int getPageIndex(long globalIndex) {
    return ((int) globalIndex >>> SchemaFile.SEG_INDEX_DIGIT);
  }

  public static short getSegIndex(long globalIndex) {
    return (short) (globalIndex & SchemaFile.SEG_INDEX_MASK);
  }

  // estimate for segment re-allocation
  static short reEstimateSegSize(int oldSize) {
    for (short size : SEG_SIZE_LST) {
      if (oldSize < size) {
        return size;
      }
    }
    return SEG_MAX_SIZ;
  }

  private void updateParentalRecord(IMNode parent, String key, long newSegAddr) throws IOException, MetadataException {
    long parSegAddr = parent.getParent() == null ? ROOT_INDEX : parent.getChildren().getSegment().getSegmentAddress();
    parSegAddr = getTargetSegmentAddress(parSegAddr, key);
    ISchemaPage page = getPageInstance(getPageIndex(parSegAddr));
    ((SchemaPage)page).updateRecordSegAddr(getSegIndex(parSegAddr), key, newSegAddr);
  }

  /**
   * Estimate segment size for pre-allocate
   * @param node
   * @return
   */
  private static short estimateSegmentSize(IMNode node) {
    int childNum = node.getChildren().size();
    int tier = SchemaFile.SEG_SIZE_LST.length;
    if (childNum > 300) {
      return SchemaFile.SEG_SIZE_LST[tier - 1];
    } else if (childNum > 150) {
      return SchemaFile.SEG_SIZE_LST[tier - 2];
    } else if (childNum > 75) {
      return SchemaFile.SEG_SIZE_LST[tier - 3];
    } else if (childNum > 40) {
      return SchemaFile.SEG_SIZE_LST[tier - 4];
    } else if (childNum > 20) {
      return SchemaFile.SEG_SIZE_LST[tier - 5];
    }
    // for childNum < 20, count for actually
    int totalSize = Segment.SEG_HEADER_SIZE;
    for (IMNode child : node.getChildren().values()) {
      totalSize += child.getName().getBytes().length;
      totalSize += 2 + 4;  // for record offset, length of string key
      if (child.isMeasurement()) {
        totalSize += child.getAsMeasurementMNode().getAlias().getBytes().length + 4;
        totalSize += 24;  // slightly larger than actually size
      } else {
        totalSize += 14;  // slightly larger
      }
    }
    return (short)totalSize;
  }

  private long getPageAddress(int pageIndex) {
    return pageIndex * PAGE_LENGTH + FILE_HEADER_SIZE;
  }

  private int loadFromFile(ByteBuffer dst, int pageIndex) throws IOException{
    dst.clear();
    return channel.read(dst, getPageAddress(pageIndex));
  }

  private void flushPageToFile(ISchemaPage src) throws IOException{
    src.syncPageBuffer();

    ByteBuffer srcBuf = ByteBuffer.allocate(SchemaFile.PAGE_LENGTH);
    src.getPageBuffer(srcBuf);
    srcBuf.clear();
    channel.write(srcBuf, getPageAddress(src.getPageIndex()));
  }

  @TestOnly
  public SchemaPage getPage(int index) throws IOException, MetadataException{
    return (SchemaPage) getPageInstance(index);
  }

  // endregion
}
