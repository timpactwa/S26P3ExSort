/**
 * Min-heap that operates directly on a byte array (the working memory pool).
 * Records are 8 bytes each (4-byte key + 4-byte value) and are sorted
 * in-place within the provided byte array at a given offset.
 *
 * No separate Record[] is allocated — all data stays in the memory pool.
 *
 * @author Brianna McDonald
 * @version Spring 2026
 */
class MinHeap {

    /** Size of each record in bytes. */
    private static final int RECORD_SIZE = 8;
    /** Reference to the working memory pool. */
    private byte[] mem;
    /** Byte offset in mem where the heap region begins. */
    private int baseOffset;
    /** Maximum number of records the heap can hold. */
    private int maxSize;
    /** Current number of records in the heap. */
    private int n;
    /** Temporary buffer for swapping records (8 bytes). */
    private byte[] swapTemp = new byte[RECORD_SIZE];

    /**
     * Constructs a MinHeap over a region of the given byte array
     * and builds the heap from the existing data in that region.
     *
     * @param memPool
     *            the working memory byte array
     * @param offset
     *            byte offset where heap data begins in memPool
     * @param heapSize
     *            number of records currently in the region
     * @param max
     *            maximum number of records the region can hold
     */
    MinHeap(byte[] memPool, int offset, int heapSize, int max) {
        mem = memPool;
        baseOffset = offset;
        n = heapSize;
        maxSize = max;
        buildHeap();
    }


    /**
     * Returns the current number of elements in the heap.
     *
     * @return current heap size
     */
    public int heapSize() {
        return n;
    }


    /**
     * Returns the maximum capacity of the heap.
     *
     * @return max capacity in records
     */
    public int capacity() {
        return maxSize;
    }


    /**
     * Returns true if pos is a leaf position.
     *
     * @param pos
     *            position to check
     * @return true if pos is a leaf
     */
    public boolean isLeaf(int pos) {
        return (n / 2 <= pos) && (pos < n);
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
     * The returned Record is a copy — the heap region in mem
     * is modified in place.
     *
     * @return the Record with the smallest key
     */
    public Record removeMin() {
        // Copy min record data before it gets overwritten
        byte[] minData = new byte[RECORD_SIZE];
        System.arraycopy(mem, byteOffset(0), minData, 0, RECORD_SIZE);

        n--;
        swap(0, n);
        siftDown(0);

        return new Record(minData);
    }


    /**
     * Removes the minimum record and returns the byte offset in mem
     * where the removed record now resides. This avoids creating a
     * Record object, which is faster for bulk extraction.
     *
     * @return byte offset in mem of the removed record's data
     */
    public int removeMinOffset() {
        n--;
        swap(0, n);
        siftDown(0);
        return byteOffset(n);
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

        n--;
        swap(pos, n);
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
        System.arraycopy(data, 0, mem, byteOffset(n), RECORD_SIZE);
        n++;
        siftUp(n - 1);
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

// ----------------------------------------------------------
// Private helpers
// ----------------------------------------------------------


    /**
     * Computes the byte offset in mem for record at index pos.
     *
     * @param pos
     *            record index
     * @return byte offset in mem
     */
    private int byteOffset(int pos) {
        return baseOffset + pos * RECORD_SIZE;
    }


    /**
     * Reads the 4-byte integer key of the record at index pos.
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
     * Returns true if the record at pos1 has a smaller key than pos2.
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
     * Swaps two records in the memory pool.
     *
     * @param pos1
     *            first record index
     * @param pos2
     *            second record index
     */
    private void swap(int pos1, int pos2) {
        int off1 = byteOffset(pos1);
        int off2 = byteOffset(pos2);
        System.arraycopy(mem, off1, swapTemp, 0, RECORD_SIZE);
        System.arraycopy(mem, off2, mem, off1, RECORD_SIZE);
        System.arraycopy(swapTemp, 0, mem, off2, RECORD_SIZE);
    }


    /**
     * Builds the heap from the existing data in the memory region.
     */
    private void buildHeap() {
        for (int i = parent(n - 1); i >= 0; i--) {
            siftDown(i);
        }
    }


    /**
     * Sifts element at pos down to restore min-heap property.
     *
     * @param pos
     *            starting position
     */
    private void siftDown(int pos) {
        while (!isLeaf(pos)) {
            int child = leftChild(pos);
            if (child >= n) {
                return;
            }
            if ((child + 1 < n) && isLessThan(child + 1, child)) {
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
     * Sifts element at pos up to restore min-heap property.
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
     * Restores heap property after a value at pos has changed.
     *
     * @param pos
     *            position of changed element
     */
    private void update(int pos) {
        siftUp(pos);
        siftDown(pos);
    }
}
