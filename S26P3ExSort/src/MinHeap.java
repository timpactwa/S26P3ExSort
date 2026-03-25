/**
 * Min-heap implementation for sorting Record objects by key value.
 *
 * @author {Your Name Here}
 * @version Spring 2026
 */
class MinHeap {
    private Record[] heap; // Pointer to the heap array
    private int maxSize;   // Maximum size of the heap
    private int n;         // Number of elements currently in heap
 
    // ----------------------------------------------------------
    /**
     * Constructor supporting preloading of heap contents.
     * @param r        array of Records to heapify
     * @param heapSize number of valid records in the array
     * @param max      maximum capacity of the heap
     */
    MinHeap(Record[] r, int heapSize, int max) {
        heap = r;
        n = heapSize;
        maxSize = max;
        buildHeap();
    }
 
    // ----------------------------------------------------------
    /**
     * Returns the current number of elements in the heap.
     * @return current heap size
     */
    public int heapSize() { return n; }
 
    // ----------------------------------------------------------
    /**
     * Returns true if pos is a leaf position, false otherwise.
     * @param pos position to check
     * @return true if pos is a leaf
     */
    public boolean isLeaf(int pos) {
        return (n / 2 <= pos) && (pos < n);
    }
 
    // ----------------------------------------------------------
    /**
     * Returns the position of the left child of pos.
     * @param pos parent position
     * @return left child position
     */
    public static int leftChild(int pos) { return 2 * pos + 1; }
 
    // ----------------------------------------------------------
    /**
     * Returns the position of the right child of pos.
     * @param pos parent position
     * @return right child position
     */
    public static int rightChild(int pos) { return 2 * pos + 2; }
 
    // ----------------------------------------------------------
    /**
     * Returns the position of the parent of pos.
     * @param pos child position
     * @return parent position
     */
    public static int parent(int pos) { return (pos - 1) / 2; }
 
    // ----------------------------------------------------------
    /**
     * Inserts a new Record into the heap.
     * @param key the Record to insert
     */
    public void insert(Record key) {
        heap[n] = key;
        n++;
        siftUp(n - 1);
    }
 
    // ----------------------------------------------------------
    /**
     * Heapifies the contents of the heap array.
     */
    private void buildHeap() {
        for (int i = parent(n - 1); i >= 0; i--) {
            siftDown(i);
        }
    }
 
    // ----------------------------------------------------------
    /**
     * Moves an element down to its correct position.
     * @param pos position of element to sift down
     */
    private void siftDown(int pos) {
        while (!isLeaf(pos)) {
            int child = leftChild(pos);
            if ((child + 1 < n) && isLessThan(child + 1, child)) {
                child = child + 1; // child is now index with lesser value
            }
            if (!isLessThan(child, pos)) {
                return; // stop early
            }
            swap(pos, child);
            pos = child; // keep sifting down
        }
    }
 
    // ----------------------------------------------------------
    /**
     * Moves an element up to its correct position.
     * @param pos position of element to sift up
     */
    private void siftUp(int pos) {
        while (pos > 0) {
            int par = parent(pos);
            if (isLessThan(par, pos)) {
                return; // stop early
            }
            swap(pos, par);
            pos = par; // keep sifting up
        }
    }
 
    // ----------------------------------------------------------
    /**
     * Removes and returns the minimum value from the heap.
     * @return the Record with the smallest key
     */
    public Record removeMin() {
        n--;
        swap(0, n);
        siftDown(0);
        return heap[n];
    }
 
    // ----------------------------------------------------------
    /**
     * Removes and returns the element at the specified position.
     * @param pos position of element to remove
     * @return the Record at pos
     */
    public Record remove(int pos) {
        n--;
        swap(pos, n);
        update(pos);
        return heap[n];
    }
 
    // ----------------------------------------------------------
    /**
     * Modifies the value at the given position and restores heap order.
     * @param pos    position to modify
     * @param newVal new Record value
     */
    public void modify(int pos, Record newVal) {
        heap[pos] = newVal;
        update(pos);
    }
 
    // ----------------------------------------------------------
    /**
     * Restores heap property after a value at pos has changed.
     * @param pos position of changed element
     */
    private void update(int pos) {
        siftUp(pos);
        siftDown(pos);
    }
 
    // ----------------------------------------------------------
    /**
     * Swaps the elements at two positions in the heap.
     * @param pos1 first position
     * @param pos2 second position
     */
    private void swap(int pos1, int pos2) {
        Record temp = heap[pos1];
        heap[pos1] = heap[pos2];
        heap[pos2] = temp;
    }
 
    // ----------------------------------------------------------
    /**
     * Returns true if the Record at pos1 has a smaller key than pos2.
     * @param pos1 first position
     * @param pos2 second position
     * @return true if heap[pos1].key < heap[pos2].key
     */
    private boolean isLessThan(int pos1, int pos2) {
        return heap[pos1].getKey() < heap[pos2].getKey();
    }
}
