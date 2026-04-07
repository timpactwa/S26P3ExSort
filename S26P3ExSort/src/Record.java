import java.nio.ByteBuffer;

/**
 * Represents a single 8-byte record with a 4-byte key and 4-byte value.
 * Used as a wrapper for individual record operations.
 *
 * @author Brianna McDonald, Timothy Pactwa
 * @version Spring 2026
 */
public class Record {

    /** The key used for sorting comparisons. */
    private int key;
    /** The data value associated with this record. */
    private int value;

    /**
     * Create a new Record from an 8-byte array.
     * The first 4 bytes are the key, the second 4 are the value.
     *
     * @param data array of 8 bytes to parse
     */
    public Record(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        this.key = buffer.getInt();
        this.value = buffer.getInt();
    }

    /**
     * Returns the key field of this record.
     *
     * @return the key value
     */
    public int getKey() {
        return key;
    }

    /**
     * Returns the data value field of this record.
     *
     * @return the data value
     */
    public int getValue() {
        return value;
    }

    /**
     * Converts record back into an 8-byte array.
     *
     * @return byte array with key in first 4 bytes, value in last 4
     */
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(this.key);
        buffer.putInt(this.value);
        return buffer.array();
    }
}