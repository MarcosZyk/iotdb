package org.apache.iotdb.db.metadata.metafile;


import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class MeasurementFile {

    private static final int HEADER_LENGTH=64;
    private static final int MEASUREMENT_LENGTH=128;

    private final SlottedFileAccess fileAccess;

    private int headerLength;
    private short measurementLength;
    private long firstFreePosition;
    private int measurementCount;

    private final List<Long> freePosition = new LinkedList<>();

    public MeasurementFile(String filepath) throws IOException {
        File metaFile = new File(filepath);
        boolean isNew = !metaFile.exists();
        fileAccess=new SlottedFile(filepath,HEADER_LENGTH,MEASUREMENT_LENGTH);

        if (isNew) {
            initMetaFileHeader();
        } else {
            readMetaFileHeader();
        }

    }

    private void initMetaFileHeader() throws IOException {
        headerLength = HEADER_LENGTH;
        measurementLength = MEASUREMENT_LENGTH;
        firstFreePosition = HEADER_LENGTH;
        measurementCount = 0;
        writeMetaFileHeader();
    }

    private void readMetaFileHeader() throws IOException {
        ByteBuffer buffer = fileAccess.readHeader();
        headerLength = buffer.get();
        measurementLength = buffer.getShort();
        firstFreePosition = buffer.getLong();
        measurementCount = buffer.getInt();
    }

    private void writeMetaFileHeader() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(headerLength);
        buffer.put((byte) headerLength);
        buffer.putShort(measurementLength);
        buffer.putLong(firstFreePosition);
        buffer.putInt(measurementCount);
        buffer.position(0);
        fileAccess.writeHeader(buffer);
    }

    public MeasurementMNode read(long position) throws IOException{
        ByteBuffer buffer = ByteBuffer.allocate(MEASUREMENT_LENGTH);

        fileAccess.readBytes(position, buffer);

        byte bitmap = buffer.get();
        if ((bitmap & 0x80) == 0) {
            throw new IOException("file corrupted");
        }

        MeasurementMNode measurementMNode= readMeasurementMNode(buffer);
        measurementMNode.setPosition(position);
        measurementMNode.setLoaded(true);
        measurementMNode.setModified(false);
        return measurementMNode;
    }

    private MeasurementMNode readMeasurementMNode(ByteBuffer dataBuffer) {
        String name=Util.readString(dataBuffer);
        String alias=Util.readString(dataBuffer);
        long offset=dataBuffer.getLong();
        byte type=dataBuffer.get();
        byte encoding=dataBuffer.get();
        byte compressor=dataBuffer.get();
        Map<String, String> props = new HashMap<>();
        String key, value;
        while (null != (key = Util.readString(dataBuffer))) {
            value = Util.readString(dataBuffer);
            props.put(key, value);
        }
        MeasurementMNode mNode = new MeasurementMNode(null,name, alias,
                TSDataType.deserialize(type), TSEncoding.deserialize(encoding), CompressionType.deserialize(compressor),props.size() == 0 ? null : props);
        mNode.setOffset(offset);
        return mNode;
    }

    public void write(MeasurementMNode measurementMNode) throws IOException{
        if (measurementMNode.getPosition() == -1) {
            measurementMNode.setPosition(getFreePos());
        }
        ByteBuffer byteBuffer= serializeMeasurementMNodeData(measurementMNode);
        fileAccess.writeBytes( measurementMNode.getPosition(),byteBuffer);
        measurementMNode.setModified(false);
    }

    private ByteBuffer serializeMeasurementMNodeData(MeasurementMNode measurementMNode) {
        ByteBuffer dataBuffer = ByteBuffer.allocate(MEASUREMENT_LENGTH);
        dataBuffer.put((byte)0x80);
        Util.writeString(dataBuffer, measurementMNode.getName());
        Util.writeString(dataBuffer, measurementMNode.getAlias());
        dataBuffer.putLong(measurementMNode.getOffset());
        MeasurementSchema schema=measurementMNode.getSchema();
        dataBuffer.put(schema.getType().serialize());
        dataBuffer.put(schema.getEncodingType().serialize());
        dataBuffer.put(schema.getCompressor().serialize());
        if (schema.getProps() != null) {
            for (Map.Entry<String, String> entry : schema.getProps().entrySet()) {
                Util.writeString(dataBuffer, entry.getKey());
                Util.writeString(dataBuffer, entry.getValue());
            }
        }
        dataBuffer.put((byte) 0);
        dataBuffer.flip();
        return dataBuffer;
    }

    public void remove(long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(7);
        fileAccess.readBytes(position, buffer);
        buffer.put((byte) (0));
        if (freePosition.size() == 0) {
            buffer.putLong(fileAccess.getFileLength());
        } else {
            buffer.putLong(freePosition.get(0));
        }
        buffer.flip();
        fileAccess.writeBytes(position,buffer);
        freePosition.add(0, position);
    }

    public long getFreePos() throws IOException {
        if (freePosition.size() != 0) {
            return freePosition.remove(0);
        }
        firstFreePosition += MEASUREMENT_LENGTH;
        return firstFreePosition - MEASUREMENT_LENGTH;
    }

    public void sync() throws IOException {
        writeMetaFileHeader();
        fileAccess.sync();
    }

    public void close() throws IOException {
        sync();
        fileAccess.close();
    }

}
