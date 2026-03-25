// Record Class for Storing Data
// -------------------------------------------------------------------------
 
import java.nio.ByteBuffer;
 
/**
 * @author {Your Name Here}
 * @version Spring 2026
 */
public class Record
{
 
    private int key;
    private int value;
 
 
    // ----------------------------------------------------------
    /**
     * Create a new Record object.
     * @param data
     *  data array of bytes being turned into a record. The first 4 bytes
     * are the key and the second 4 bytes are the value.
     */
    public Record(byte[] data)
    {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        this.key = buffer.getInt();
        this.value = buffer.getInt();
    }
 
 
 // ----------------------------------------------------------
    /**
     * Accessor method for the key field
     * @return
     *  the key of the record
     */
    public int getKey()
    {
        return key;
    }
 
 
    // ----------------------------------------------------------
    /**
     * Accessor method for the value field
     * @return
     *  the value stored by the record
     */
    public int getValue()
    {
        return value;
    }
 
 
    // ----------------------------------------------------------
    /**
     * Converts the record back into an array of bytes
     * @return
     *  array of bytes stored by record
     */
    public byte[] toBytes()
    {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt(this.key);
        buffer.putInt(this.value);
        return buffer.array();
    }
}
