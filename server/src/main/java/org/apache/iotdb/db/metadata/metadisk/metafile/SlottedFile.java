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
package org.apache.iotdb.db.metadata.metadisk.metafile;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class SlottedFile implements ISlottedFileAccess {

  private final RandomAccessFile file;
  private final FileChannel channel;

  private final int headerLength;

  private volatile long currentBlockPosition;
  private volatile boolean isModified = false;
  private final byte[] blockData;
  private final int blockScale;
  private final int blockSize;
  private final long blockMask;

  public SlottedFile(String filepath, int headerLength, int blockSize) throws IOException {
    File metaFile = new File(filepath);
    file = new RandomAccessFile(metaFile, "rw");
    channel = file.getChannel();

    this.headerLength = headerLength;

    blockScale = (int) Math.ceil(Math.log(blockSize) / Math.log(2));
    this.blockSize = 1 << blockScale;
    blockMask = 0xffffffffffffffffL << blockScale;
    blockData = new byte[this.blockSize];

    currentBlockPosition = headerLength;
    fileRead(currentBlockPosition, ByteBuffer.wrap(blockData));
  }

  @Override
  public long getFileLength() throws IOException {
    return file.length();
  }

  @Override
  public int getHeaderLength() {
    return headerLength;
  }

  @Override
  public int getBlockSize() {
    return blockSize;
  }

  @Override
  public synchronized ByteBuffer readHeader() throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(headerLength);
    fileRead(0, buffer);
    buffer.flip();
    return buffer;
  }

  @Override
  public void writeHeader(ByteBuffer buffer) throws IOException {
    if (buffer.limit() - buffer.position() != headerLength) {
      throw new IOException("wrong format header");
    }
    fileWrite(0, buffer);
  }

  @Override
  public ByteBuffer readBytes(long position, int length) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(length);
    readBytes(position, buffer);
    return buffer;
  }

  @Override
  public void readBytes(long position, ByteBuffer byteBuffer) throws IOException {
    synchronized (blockData) {
      ByteBuffer blockBuffer = ByteBuffer.wrap(blockData);

      // read data from blockBuffer to buffer
      updateDataBlock(position, blockBuffer);
      blockBuffer.limit(
          blockBuffer.position() + Math.min(byteBuffer.remaining(), blockBuffer.remaining()));
      byteBuffer.put(blockBuffer);

      // case the target data exist in several block
      while (byteBuffer.hasRemaining()) {
        // read next block from file
        updateDataBlock(currentBlockPosition + blockSize, blockBuffer);

        // read the rest target data to buffer
        blockBuffer.limit(Math.min(byteBuffer.remaining(), blockSize));
        byteBuffer.put(blockBuffer);
      }
      byteBuffer.flip();
    }
  }

  @Override
  public void writeBytes(long position, ByteBuffer byteBuffer) throws IOException {
    synchronized (blockData) {
      ByteBuffer blockBuffer = ByteBuffer.wrap(blockData);

      updateDataBlock(position, blockBuffer);
      int limit = byteBuffer.limit();
      byteBuffer.limit(
          byteBuffer.position() + Math.min(byteBuffer.remaining(), blockBuffer.remaining()));
      blockBuffer.put(byteBuffer);
      isModified = true;
      byteBuffer.limit(limit);

      // case the target data exist in several block
      while (byteBuffer.hasRemaining()) {
        // read next block from file
        updateDataBlock(currentBlockPosition + blockSize, blockBuffer);

        // read the rest target data to buffer
        byteBuffer.limit(byteBuffer.position() + Math.min(byteBuffer.remaining(), blockSize));
        blockBuffer.put(byteBuffer);
        isModified = true;
        byteBuffer.limit(limit);
      }
      blockBuffer.position(0);
      blockBuffer.limit(blockSize);
    }
  }

  private void updateDataBlock(long position, ByteBuffer blockBuffer) throws IOException {
    if (position < currentBlockPosition || position >= currentBlockPosition + blockSize) {

      if (isModified) {
        blockBuffer.position(0);
        blockBuffer.limit(blockSize);
        fileWrite(currentBlockPosition, blockBuffer);
        isModified = false;
      }

      blockBuffer.position(0);
      blockBuffer.limit(blockSize);
      currentBlockPosition = ((position - headerLength) & blockMask) + headerLength;
      fileRead(currentBlockPosition, blockBuffer);
    }

    blockBuffer.position((int) (position - currentBlockPosition));
    blockBuffer.limit(blockSize);
  }

  private void fileRead(long position, ByteBuffer byteBuffer) throws IOException {
    channel.position(position);
    channel.read(byteBuffer);
  }

  private void fileWrite(long position, ByteBuffer byteBuffer) throws IOException {
    channel.position(position);
    while (byteBuffer.hasRemaining()) {
      channel.write(byteBuffer);
    }
  }

  @Override
  public void sync() throws IOException {
    synchronized (blockData) {
      if (isModified) {
        fileWrite(currentBlockPosition, ByteBuffer.wrap(blockData));
        isModified = false;
      }
    }
    channel.force(true);
  }

  @Override
  public void close() throws IOException {
    sync();
    channel.force(true);
    channel.close();
    file.close();
  }
}
