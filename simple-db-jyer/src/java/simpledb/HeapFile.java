package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	private File f;
	private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
        }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
    	byte[] data = new byte[BufferPool.getPageSize()];
    	try {
    		RandomAccessFile randFile = new RandomAccessFile(f, "r");
    		long start = (long) pid.getPageNumber() * (long) BufferPool.getPageSize();
    		randFile.seek(start);
    		randFile.read(data);
    		randFile.close();
    		return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), data);
    	} catch(IOException e) {
    		throw new IllegalArgumentException();
    	}
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	RandomAccessFile randFile = new RandomAccessFile(f, "rw");
    	long start = (long) page.getId().getPageNumber() * BufferPool.getPageSize();
    	randFile.seek(start);
    	randFile.write(page.getPageData());
    	randFile.close();
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) f.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	int numPage = numPages();
    	ArrayList<Page> list = new ArrayList<>();
    	for (int i = 0; i < numPage; i++) {
    		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i),
    				Permissions.READ_WRITE);
    		if (page.getNumEmptySlots() > 0) {
    			page.insertTuple(t);
    			list.add(page);
    			return list;
    		}
    		else
    			Database.getBufferPool().releasePage(tid, page.getId());
    	}
    	HeapPage page = new HeapPage(new HeapPageId(getId(), numPage),
    			HeapPage.createEmptyPageData());
    	page.insertTuple(t);
    	//numPages++;
    	//return insertTuple(tid, t);
    	writePage(page);
    	list.add(page);
    	return list;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	ArrayList<Page> list = new ArrayList<>();
    	HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, 
    			t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	page.deleteTuple(t);
    	list.add(page);
    	return list;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new HeapFileIterator(this, tid);
        // some code goes here

    }
    class HeapFileIterator extends AbstractDbFileIterator {
		
		private HeapFile f;
		private TransactionId tid;
		private Iterator<Tuple> currIt;
		private int currPageNo;
		
    	HeapFileIterator(HeapFile f, TransactionId tid) {
    		this.f = f;
    		this.tid = tid;
    		currPageNo = -1;
    	}
    	
    	protected Tuple readNext() throws DbException, TransactionAbortedException {
			if (currIt == null || !currIt.hasNext()) {
				if (currPageNo == -1 || currPageNo >= f.numPages()) {
					return null;
				}
				PageId pid = new HeapPageId(f.getId(), currPageNo);
				Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
				currIt = ((HeapPage) page).iterator();
				currPageNo++;
				return readNext();
			}
			return currIt.next();
		}
		public void open() {
			currPageNo = 0;
		}
		
		public void close() {
			super.close();
			currPageNo = -1;
			currIt = null;
		}
		
		public void rewind() throws DbException, TransactionAbortedException{
			close();
			open();
		}
    }

}

