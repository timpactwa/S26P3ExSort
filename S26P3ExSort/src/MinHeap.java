/**
 * The MinHeap Implementation. 
 * 
 * A MinHeap structure is a heap, a tree connected to an 
 * array, which sorts its data values so that every internal 
 * node's value is smaller than that of its children. The 
 * smallest data value in the entire structure will always 
 * be at the root of the tree every time the minHeap gets 
 * reorganized post-insertion or post-deletion of a value.
 * 
 * @author Brianna McDonald, Tim Pactwa
 * @version Spring 2026
 */
public class MinHeap {

    /** size of each record in bytes (4 for key, 4 for value */
    private static final int RECORD_SIZE = 8;
    /** the working memory pool */
    private byte[] mem;
    /** byte offset in mem where the heap section starts */
    private int baseOffset;
    /** max number of records the heap can have */
    private int maxSize;
    /** current number of records in the heap */
    private int numHeapRecords;
    /** temp buffer variable for swapping records 
     * that can hold 1 record at a time (each record 
     * is 8 bytes aka RECORD_SIZE) */
    private byte[] swapTemp = new byte[RECORD_SIZE];

    /**
     * Constructor for creating a MinHeap
     * 
     * Makes a MinHeap over a section of the given byte array
     * and builds the heap from the data already in that area.
     *
     * @param memPool
     *            the working memory byte array
     * @param offset
     *            byte offset where heap data starts in memPool
     * @param heapSize
     *            number of records currently in the region
     * @param maxRecordCap
     *            maximum number of records the region can hold
     */
    MinHeap(byte[] memPool, int offset, int heapSize, int maxRecordCap) {
        // setting up variable values for the minHeap
        mem = memPool;
        baseOffset = offset;
        numHeapRecords = heapSize;
        maxSize = maxRecordCap;
        // creates the heap upon initialization of a MinHeap object
        buildHeap();
    }


    /**
     * Getter function that returns the current number 
     * of elements in the heap.
     *
     * @return current heap size
     */
    public int heapSize() {
        return numHeapRecords;
    }


    /**
     * Getter function that returns the maximum capacity of the heap.
     *
     * @return max capacity in records
     */
    public int capacity() {
        return maxSize;
    }


    /**
     * Returns true if current node is a leaf position.
     *
     * @param pos
     *            position to check
     * @return true if node is a leaf
     */
    public boolean isLeaf(int pos) {
        return (numHeapRecords / 2 <= pos) && (pos < numHeapRecords);
    }


    /**
     * Returns the position of the left child.
     *
     * @param pos
     *            parent position
     * @return left child position
     */
    public static int leftChild(int pos) {
        return 2 * pos + 1;
    }


    /**
     * Returns the position of the right child.
     *
     * @param pos
     *            parent position
     * @return right child position
     */
    public static int rightChild(int pos) {
        return 2 * pos + 2;
    }


    /**
     * Returns the position of the parent.
     *
     * @param pos
     *            child position
     * @return parent position
     */
    public static int parent(int pos) {
        return (pos - 1) / 2;
    }


    /**
     * Removes and returns the minimum record from the heap.
     * The returned record is a copy — the heap region in mem
     * is modified in place.
     *
     * @return the record with the smallest key (the minimum record in the heap)
     */
    public Record removeMin() {
        // copies min record data before it gets overwritten
        byte[] minData = new byte[RECORD_SIZE];
        System.arraycopy(mem, byteOffset(0), minData, 0, RECORD_SIZE);

        // updates num records in heap and reorganizes 
        // heap to make it a minHeap again
        numHeapRecords--;
        swap(0, numHeapRecords);
        siftDown(0);

        return new Record(minData);
    }


    /**
     * Removes the minimum record and returns the byte offset in mem
     * where the removed record is now.
     *
     * @return byte offset in mem of the removed record's data
     */
    public int removeMinOffset() {
        // updating num of heap records then reorganizes the heap to make it
        // a minHeap again and returns where the record's byte offset is. 
        numHeapRecords--;
        swap(0, numHeapRecords);
        siftDown(0);
        return byteOffset(numHeapRecords);
    }


    /**
     * Removes and returns the record at the specified position.
     *
     * @param pos
     *            position of element to remove
     * @return the Record that was at pos
     */
    public Record remove(int pos) {
        byte[] data = new byte[RECORD_SIZE];
        System.arraycopy(mem, byteOffset(pos), data, 0, RECORD_SIZE);

        numHeapRecords--;
        swap(pos, numHeapRecords);
        update(pos);

        return new Record(data);
    }


    /**
     * Inserts a new Record into the heap by writing its bytes
     * into the next open slot in the memory pool.
     *
     * @param rec
     *            the Record to insert
     */
    public void insert(Record rec) {
        byte[] data = rec.toBytes();
        System.arraycopy(data, 0, mem, byteOffset(numHeapRecords), RECORD_SIZE);
        // updates num records in heap and resorts the heap to 
        // make it a minHeap again now with the new value
        numHeapRecords++;
        siftUp(numHeapRecords - 1);
    }


    /**
     * Modifies the record at pos with new data and restores heap order.
     *
     * @param pos
     *            position to modify
     * @param newVal
     *            new Record value
     */
    public void modify(int pos, Record newVal) {
        byte[] data = newVal.toBytes();
        System.arraycopy(data, 0, mem, byteOffset(pos), RECORD_SIZE);
        update(pos);
    }

    /**
     * Helper method that computes the byte offset in 
     * mem for record at index pos.
     *
     * @param pos
     *            record index
     * @return byte offset in mem
     */
    private int byteOffset(int pos) {
        return baseOffset + pos * RECORD_SIZE;
    }


    /**
     * Helper method that reads the 4-byte integer 
     * key of the record at index pos.
     *
     * @param pos
     *            record index
     * @return the key value
     */
    private int getKey(int pos) {
        int off = byteOffset(pos);
        return ((mem[off] & 0xFF) << 24) | ((mem[off + 1] & 0xFF) << 16)
            | ((mem[off + 2] & 0xFF) << 8) | (mem[off + 3] & 0xFF);
    }


    /**
     * Helper method that returns true if the record 
     * at pos1 has a smaller key than pos2.
     *
     * @param pos1
     *            first position
     * @param pos2
     *            second position
     * @return true if key at pos1 < key at pos2
     */
    private boolean isLessThan(int pos1, int pos2) {
        return getKey(pos1) < getKey(pos2);
    }


    /**
     * Helper method that swaps two records in the memory pool.
     *
     * @param pos1
     *            first record index
     * @param pos2
     *            second record index
     */
    private void swap(int pos1, int pos2) {
        // finding the offsets of both positions in the memory pool
        int off1 = byteOffset(pos1);
        int off2 = byteOffset(pos2);
        // swapping the records with a temp holder variable
        System.arraycopy(mem, off1, swapTemp, 0, RECORD_SIZE);
        System.arraycopy(mem, off2, mem, off1, RECORD_SIZE);
        System.arraycopy(swapTemp, 0, mem, off2, RECORD_SIZE);
    }


    /**
     * Helper method that builds the heap from 
     * the existing data in the memory pool section.
     */
    private void buildHeap() {
        // minHeap sorting the data from the memory pool
        for (int i = parent(numHeapRecords - 1); i >= 0; i--) {
            siftDown(i);
        }
    }


    /**
     * Helper method that sifts element at pos 
     * down to restore min-heap property.
     *
     * @param pos
     *            starting position
     */
    private void siftDown(int pos) {
        while (!isLeaf(pos)) {
            int child = leftChild(pos);
            if (child >= numHeapRecords) {
                return;
            }
            if ((child + 1 < numHeapRecords) && isLessThan(child + 1, child)) {
                child = child + 1;
            }
            if (!isLessThan(child, pos)) {
                return;
            }
            swap(pos, child);
            pos = child;
        }
    }


    /**
     * Helper method that sifts element at pos 
     * up to restore min-heap property.
     *
     * @param pos
     *            starting position
     */
    private void siftUp(int pos) {
        while (pos > 0) {
            int par = parent(pos);
            if (isLessThan(par, pos)) {
                return;
            }
            swap(pos, par);
            pos = par;
        }
    }


    /**
     * Helper method that restores heap property 
     * after a value at pos has changed.
     *
     * @param pos
     *            position of changed element
     */
    private void update(int pos) {
        siftUp(pos);
        siftDown(pos);
    }
}
