/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.metadata.schemafile.ColossalRecordException;
import org.apache.iotdb.db.exception.metadata.schemafile.RecordDuplicatedException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.nio.ByteBuffer;

import static org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.schemafile.SchemaFileConfig.SEG_HEADER_SIZE;

/**
 * Segments store [String]keys with bytebuffer records, which can be serialized IMNode, bytes of
 * alias string or any other attributes need to store with hierarchy structure.
 *
 * @param <R> type to construct from bytebuffer
 */
public abstract class Segment<R> implements ISegment<ByteBuffer, R> {

  // members load from buffer
  protected final ByteBuffer buffer;
  protected short length, freeAddr, recordNum, pairLength;
  protected boolean delFlag, aliasFlag;
  protected long prevSegAddress, nextSegAddress;

  // todo remove
  // reconstruct from key-address pair buffer
  // protected List<Pair<String, Short>> keyAddressList;

  // assess monotonic
  protected String penuKey = null, lastKey = null;

  /**
   * Init Segment with a buffer, which contains all information about this segment
   *
   * <p>For a page no more than 16 kib, a signed short is enough to index all bytes inside a
   * segment.
   *
   * <p><b>Segment Structure:</b>
   * <li>(25 byte: header)
   * <li>1 short: length, segment length
   * <li>1 short: freeAddr, start offset of records
   * <li>1 short: recordNum, amount of records in this segment
   * <li>1 short: pairLength, length of key-address in bytes
   * <li>1 long (8 bytes): prevSegIndex, previous segment index
   * <li>1 long (8 bytes): nextSegIndex, next segment index
   * <li>1 bit: delFlag, delete flag
   * <li>1 bit: aliasFlag, whether alias existed<br>
   *     (--- checksum, parent record address, max/min record key may be contained further ---)
   * <li><s>var length: key-address pairs, begin at 25 bytes offset, length of pairLength </s><br>
   * <li>var length: ORDERED record offset<br>
   *     ... empty space ...
   * <li>var length: (record key, record body) * recordNum
   */
  protected Segment(ByteBuffer buffer, boolean override) {
    this.buffer = buffer;

    this.buffer.clear();
    if (override) {
      // blank segment
      length = (short) buffer.capacity();
      freeAddr = (short) buffer.capacity();
      recordNum = 0;
      pairLength = 0;

      // these two address need to be initiated as same as in childrenContainer
      prevSegAddress = -1;
      nextSegAddress = -1;
      delFlag = false;
      aliasFlag = false;

    } else {
      length = ReadWriteIOUtils.readShort(buffer);
      freeAddr = ReadWriteIOUtils.readShort(buffer);
      recordNum = ReadWriteIOUtils.readShort(buffer);
      pairLength = ReadWriteIOUtils.readShort(buffer);

      prevSegAddress = ReadWriteIOUtils.readLong(buffer);
      nextSegAddress = ReadWriteIOUtils.readLong(buffer);
      byte flags = ReadWriteIOUtils.readByte(buffer);
      delFlag = (0x80 & flags) != 0;
      aliasFlag = (0x40 & flags) != 0;
    }
  }

  @Override
  public boolean hasRecordKey(String key) {
    return binarySearchOnKeys(key) > -1;
  }

  @Override
  public boolean hasRecordAlias(String alias) {
    return false;
  }

  @Override
  public synchronized void syncBuffer() {
    ByteBuffer prefBuffer = ByteBuffer.allocate(SEG_HEADER_SIZE);

    ReadWriteIOUtils.write(length, prefBuffer);
    ReadWriteIOUtils.write(freeAddr, prefBuffer);
    ReadWriteIOUtils.write(recordNum, prefBuffer);
    ReadWriteIOUtils.write(pairLength, prefBuffer);
    ReadWriteIOUtils.write(prevSegAddress, prefBuffer);
    ReadWriteIOUtils.write(nextSegAddress, prefBuffer);
    ReadWriteIOUtils.write(getFlag(), prefBuffer);

    prefBuffer.clear();
    this.buffer.clear();
    this.buffer.put(prefBuffer);
  }

  private byte getFlag() {
    byte flags = delFlag ? (byte) 0x80 : (byte) 0x00;
    flags = (byte) (aliasFlag ? flags | 0x40 : flags | 0x00);
    return flags;
  }

  @Override
  public short size() {
    return length;
  }

  @Override
  public short getSpareSize() {
    return (short) (freeAddr - pairLength - SEG_HEADER_SIZE);
  }

  @Override
  public void delete() {
    this.delFlag = true;
    this.buffer.clear();
    this.buffer.position(SchemaFileConfig.SEG_HEADER_SIZE - 1);
    ReadWriteIOUtils.write(getFlag(), this.buffer);
  }

  @Override
  public long getNextSegAddress() {
    return nextSegAddress;
  }

  @Override
  public void setNextSegAddress(long nextSegAddress) {
    this.nextSegAddress = nextSegAddress;
  }

  @Override
  public void extendsTo(ByteBuffer newBuffer) throws MetadataException {
    short sizeGap = (short) (newBuffer.capacity() - length);

    if (sizeGap < 0) {
      throw new MetadataException("Leaf Segment cannot extend to a smaller buffer.");
    }

    this.buffer.clear();
    newBuffer.clear();

    if (sizeGap == 0) {
      this.syncBuffer();
      this.buffer.clear();
      newBuffer.put(this.buffer);
      this.buffer.clear();
      newBuffer.clear();
      return;
    }

    ReadWriteIOUtils.write((short) newBuffer.capacity(), newBuffer);
    ReadWriteIOUtils.write((short) (freeAddr + sizeGap), newBuffer);
    ReadWriteIOUtils.write(recordNum, newBuffer);
    ReadWriteIOUtils.write(pairLength, newBuffer);
    ReadWriteIOUtils.write(prevSegAddress, newBuffer);
    ReadWriteIOUtils.write(nextSegAddress, newBuffer);
    ReadWriteIOUtils.write(getFlag(), newBuffer);

    newBuffer.position(SchemaFileConfig.SEG_HEADER_SIZE);
    this.buffer.position(SchemaFileConfig.SEG_HEADER_SIZE);
    for (int i = 0; i < recordNum; i++) {
      newBuffer.putShort((short) (this.buffer.getShort() + sizeGap));
    }

    this.buffer.clear();
    this.buffer.position(freeAddr);
    this.buffer.limit(length);
    newBuffer.position(freeAddr + sizeGap);
    newBuffer.put(this.buffer);
    newBuffer.clear();
    this.buffer.clear();
  }

  @Override
  public ByteBuffer resetBuffer(int ptr) {
    freeAddr = (short) this.buffer.capacity();
    recordNum = 0;
    pairLength = 0;
    prevSegAddress = -1;
    nextSegAddress = -1;
    delFlag = false;
    aliasFlag = false;
    syncBuffer();
    this.buffer.clear();
    return this.buffer.slice();
  }

  @Override
  public String splitByKey(
      String key, ByteBuffer recBuf, ByteBuffer dstBuffer, boolean inclineSplit)
      throws MetadataException {

    if (this.buffer.capacity() != dstBuffer.capacity()) {
      throw new MetadataException("Segments only splits with same capacity.");
    }

    if (recordNum == 0) {
      throw new MetadataException("Segment can not be split with no records.");
    }

    if (key == null && recordNum == 1) {
      throw new MetadataException("Segment can not be split with only one record.");
    }

    // notice that key can be null here
    boolean monotonic =
        penuKey != null
            && key != null
            && lastKey != null
            && inclineSplit
            && (key.compareTo(lastKey)) * (lastKey.compareTo(penuKey)) > 0;

    int n = recordNum;

    // actual index of key just smaller than the insert, -2 for null key
    int pos = key != null ? binaryInsertOnKeys(key) - 1 : -2;

    int sp; // virtual index to split
    if (monotonic) {
      // new entry into part with more space
      sp = key.compareTo(lastKey) > 0 ? Math.max(pos + 1, n / 2) : Math.min(pos + 2, n / 2);
    } else {
      sp = n / 2;
    }

    // little different from InternalSegment, only the front edge key can not split
    sp = sp <= 0 ? 1 : sp;

    short recordLeft = this.recordNum;
    // prepare header for dstBuffer
    short length = this.length,
        freeAddr = (short) dstBuffer.capacity(),
        recordNum = 0,
        pairLength = 0;
    long prevSegAddress = this.prevSegAddress, nextSegAddress = this.nextSegAddress;

    int recSize, keySize;
    String mKey, sKey = null;
    ByteBuffer srcBuf;
    int aix; // aix for actual index on keyAddressList
    n = key == null ? n - 1 : n; // null key
    // TODO: implement bulk split further
    for (int ix = sp; ix <= n; ix++) {
      if (ix == pos + 1) {
        // migrate newly insert
        srcBuf = recBuf;
        recSize = recBuf.capacity();
        mKey = key;
        keySize = 4 + mKey.getBytes().length;

        recBuf.clear();
      } else {
        srcBuf = this.buffer;
        // pos equals -2 if key is null
        aix = (ix > pos) && (pos != -2) ? ix - 1 : ix;
        mKey = getKeyByIndex(aix);
        keySize = 4 + mKey.getBytes().length;

        // prepare on this.buffer
        this.buffer.clear();
        this.buffer.position(getOffsetByIndex(aix) + keySize);
        recSize = getRecordLength();
        this.buffer.limit(this.buffer.position() + recSize);

        recordLeft--;
      }

      if (ix == sp) {
        // search key is the first key in split segment
        sKey = mKey;
      }

      freeAddr -= recSize + keySize;
      dstBuffer.position(freeAddr);
      ReadWriteIOUtils.write(mKey, dstBuffer);
      dstBuffer.put(srcBuf);

      dstBuffer.position(SchemaFileConfig.SEG_HEADER_SIZE + pairLength);
      ReadWriteIOUtils.write(freeAddr, dstBuffer);

      recordNum++;
      pairLength += 2;
    }

    // compact and update status
    this.recordNum = recordLeft;
    compactRecords();
    if (sp > pos + 1 && key != null) {
      // new insert shall be in this
      if (insertRecord(key, recBuf) < 0) {
        throw new ColossalRecordException(key, recBuf.capacity());
      }
    }

    // flush dstBuffer header
    dstBuffer.clear();
    ReadWriteIOUtils.write(length, dstBuffer);
    ReadWriteIOUtils.write(freeAddr, dstBuffer);
    ReadWriteIOUtils.write(recordNum, dstBuffer);
    ReadWriteIOUtils.write(pairLength, dstBuffer);
    ReadWriteIOUtils.write(prevSegAddress, dstBuffer);
    ReadWriteIOUtils.write(nextSegAddress, dstBuffer);
    // FIXME flag of split page is not always the same
    ReadWriteIOUtils.write(getFlag(), dstBuffer);

    penuKey = null;
    lastKey = null;
    return sKey;
  }

  protected void compactRecords() {
    // compact by existed item on keyAddressList
    ByteBuffer tempBuf = ByteBuffer.allocate(this.buffer.capacity() - this.freeAddr);
    int accSiz = 0;
    short[] newOffsets = new short[recordNum];

    String migKey;
    short migOffset;
    int recLen;
    for (int i = 0; i < recordNum; i++) {
      migOffset = getOffsetByIndex(i);
      migKey = getKeyByOffset(migOffset);
      this.buffer.position(migOffset + 4 + migKey.getBytes().length);
      recLen = getRecordLength();

      this.buffer.clear();
      this.buffer.position(migOffset);
      this.buffer.limit(migOffset + 4 + migKey.getBytes().length + recLen);

      accSiz += this.buffer.remaining();

      newOffsets[i] = (short) (this.buffer.capacity() - accSiz);
      tempBuf.position(tempBuf.capacity() - accSiz);
      tempBuf.put(this.buffer);
    }

    tempBuf.clear();
    tempBuf.position(tempBuf.capacity() - accSiz);
    this.freeAddr = (short) (this.buffer.capacity() - accSiz);

    this.buffer.clear();
    this.buffer.position(SEG_HEADER_SIZE);
    this.buffer.asShortBuffer().put(newOffsets);

    this.buffer.position(this.freeAddr);
    this.buffer.put(tempBuf);

    this.syncBuffer();
  }

  /** Assuming that buffer has been set position well, record length depends on implementation. */
  protected abstract short getRecordLength();

  // region Record Index Access
  // todo abstract with same name method within Internal

  protected short getOffsetByIndex(int index) {
    if (index < 0 || index >= recordNum) {
      throw new IndexOutOfBoundsException();
    }
    synchronized (this.buffer) {
      this.buffer.limit(this.buffer.capacity());
      this.buffer.position(SEG_HEADER_SIZE + index * SchemaFileConfig.SEG_OFF_DIG);
      return ReadWriteIOUtils.readShort(this.buffer);
    }
  }

  protected String getKeyByOffset(short offset) {
    synchronized (this.buffer) {
      this.buffer.limit(this.buffer.capacity());
      this.buffer.position(offset);
      return ReadWriteIOUtils.readString(this.buffer);
    }
  }

  private String getKeyByIndex(int index) {
    if (index < 0 || index >= recordNum) {
      throw new IndexOutOfBoundsException();
    }
    synchronized (this.buffer) {
      this.buffer
          .limit(this.buffer.capacity())
          .position(SEG_HEADER_SIZE + index * SchemaFileConfig.SEG_OFF_DIG);
      this.buffer.position(ReadWriteIOUtils.readShort(this.buffer));
      return ReadWriteIOUtils.readString(this.buffer);
    }
  }

  /**
   * @param key
   * @return -1 if not existed, otherwise correspondent position of target offset
   */
  protected int binarySearchOnKeys(String key) {
    int head = 0;
    int tail = recordNum - 1;
    if (tail < 0
        || key.compareTo(getKeyByIndex(head)) < 0
        || key.compareTo(getKeyByIndex(tail)) > 0) {
      return -1;
    }

    if (key.compareTo(getKeyByIndex(head)) == 0) {
      return head;
    }
    if (key.compareTo(getKeyByIndex(tail)) == 0) {
      return tail;
    }

    int pivot = (head + tail) / 2;
    while (key.compareTo(getKeyByIndex(pivot)) != 0) {
      if (head == tail || pivot == head || pivot == tail) {
        return -1;
      }
      if (key.compareTo(getKeyByIndex(pivot)) < 0) {
        tail = pivot;
      } else if (key.compareTo(getKeyByIndex(pivot)) > 0) {
        head = pivot;
      }
      pivot = (head + tail) / 2;
    }
    return pivot;
  }

  protected int binaryInsertOnKeys(String key) throws RecordDuplicatedException {
    if (recordNum == 0) {
      return 0;
    }

    int tarIdx = 0;
    int head = 0;
    int tail = recordNum - 1;

    if (getKeyByIndex(head).compareTo(key) == 0 || getKeyByIndex(tail).compareTo(key) == 0) {
      throw new RecordDuplicatedException(key);
    }

    if (key.compareTo(getKeyByIndex(head)) < 0) {
      return 0;
    }

    if (key.compareTo(getKeyByIndex(tail)) > 0) {
      return recordNum;
    }

    int pivot;
    while (head != tail) {
      pivot = (head + tail) / 2;
      // notice pivot always smaller than list.size()-1
      if (getKeyByIndex(pivot).compareTo(key) == 0
          || getKeyByIndex(pivot + 1).compareTo(key) == 0) {
        throw new RecordDuplicatedException(key);
      }

      if (getKeyByIndex(pivot).compareTo(key) < 0 && getKeyByIndex(pivot + 1).compareTo(key) > 0) {
        return pivot + 1;
      }

      if (pivot == head || pivot == tail) {
        if (getKeyByIndex(head).compareTo(key) > 0) {
          return head;
        }
        if (getKeyByIndex(tail).compareTo(key) < 0) {
          return tail + 1;
        }
      }

      // impossible for pivot.cmp > 0 and (pivot+1).cmp < 0
      if (getKeyByIndex(pivot).compareTo(key) > 0) {
        tail = pivot;
      }

      if (getKeyByIndex(pivot + 1).compareTo(key) < 0) {
        head = pivot;
      }
    }
    return tarIdx;
  }

  // endregion
}
