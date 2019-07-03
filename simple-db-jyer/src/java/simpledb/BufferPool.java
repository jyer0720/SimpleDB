package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.lang.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    private ConcurrentHashMap<PageId, Page> pages;
    private int numPages;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    private class LockManager {
    	
    	private Map<PageId, Set<TransactionId>> pidToTid;
    	private Map<PageId, Permissions> pidToPerm;
    	public Map<TransactionId, Set<PageId>> tidToPid;
    	public Map<TransactionId, Set<TransactionId>> tidCycle;
    	
    	public LockManager() {
    		pidToTid = new HashMap<>();
    		pidToPerm = new HashMap<>();
    		tidToPid = new HashMap<>();
    		tidCycle = new HashMap<>();
    	}
    	
    	private synchronized boolean checkLock(TransactionId tid, PageId pid, Permissions perm) {
    		if (!pidToPerm.containsKey(pid))
    			return true;
    		if (pidToPerm.get(pid).equals(Permissions.READ_ONLY)) {
    			if (perm.equals(Permissions.READ_ONLY))
    				return true;
    			else {
    				// problem
    				return pidToTid.containsKey(pid) && 
    						pidToTid.get(pid).contains(tid) &&
    						pidToTid.get(pid).size() == 1;
    			}
    		}
    		else {
    			return pidToTid.get(pid).contains(tid);
    		}
    	}
    	
    	private synchronized boolean getLock(TransactionId tid, PageId pid, Permissions perm) 
    			throws TransactionAbortedException{
    		if (!checkLock(tid, pid, perm)) {
    			if (detectCycle(tid, new HashSet<>()))
    				throw new TransactionAbortedException();
    			if (!tidCycle.containsKey(tid))
    				tidCycle.put(tid, new HashSet<>());
    			for (TransactionId blockedTid : pidToTid.get(pid))
    				tidCycle.get(tid).add(blockedTid);
    			return false;
    		}
    		tidCycle.remove(tid);
    		pidToPerm.put(pid, perm);
    		if (!pidToTid.containsKey(pid))
    			pidToTid.put(pid, new HashSet<>());
    		pidToTid.get(pid).add(tid);
    		if (!tidToPid.containsKey(tid))
    			tidToPid.put(tid, new HashSet<>());
    		tidToPid.get(tid).add(pid);
    		return true;// not finished
    	}
    	
    	private boolean detectCycle(TransactionId tid, Set<TransactionId> visited) {
    		if (tid == null || !tidCycle.containsKey(tid)) return false;
    		if (visited.contains(tid)) return true;
    		visited.add(tid);
    		for (TransactionId childTid : tidCycle.get(tid)) {
    			if (detectCycle(childTid, visited)) return true;
    		}
    		return false;
    	}
    	
    	private synchronized boolean releaseLock(TransactionId tid, PageId pid) {
    		if (!pidToTid.containsKey(pid) || !pidToTid.get(pid).contains(tid)
    				|| !tidToPid.containsKey(tid) || !tidToPid.get(tid).contains(pid))
    			return false;
    		pidToTid.get(pid).remove(tid);
    		if (pidToTid.get(pid).isEmpty()) {
    			pidToTid.remove(pid);
    			pidToPerm.remove(pid);
    		}
    		tidToPid.get(tid).remove(pid);
    		if (tidToPid.get(tid).isEmpty())
    			tidToPid.remove(tid);
    		return true;
    	}
    	
    	private synchronized boolean holdsLock(TransactionId tid, PageId pid) {
    		return pidToTid.containsKey(pid) && pidToTid.get(pid).contains(tid);
    	}
    	
    	
    }
    
    private LockManager lm;
    public BufferPool(int numPages) {
    	this.numPages = numPages;
    	pages = new ConcurrentHashMap<>();
    	lm = new LockManager();
        // some code goes here
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	while (!lm.getLock(tid, pid, perm)) {
    	}
    	if (pages.containsKey(pid)) {
    		return pages.get(pid);
    	}
    	while (pages.size() >= numPages) {
    		evictPage();
    	}
    	Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
    	pages.put(pid, page);
    	return page;
        // some code goes here
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	lm.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lm.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	if (commit) {
    		//	System.out.println("commit");
    		//	flushPages(tid);
    		for (PageId pid : pages.keySet()) {
    			Page page = pages.get(pid);
    			if (page.isDirty() == tid) {
    				releasePage(tid, pid);
    				Database.getLogFile().logWrite(tid, page.getBeforeImage(), page);
    				page.setBeforeImage();
    			}
    		}
    		//Database.getLogFile().logWrite(tid, p.getBeforeImage(), p)
    	}
    	else {
    		Set<PageId> pids = new HashSet<>();
    		if (lm.tidToPid.containsKey(tid)) {
    			for (PageId pid : lm.tidToPid.get(tid)) {
    				pids.add(pid);
    			}
    			for (PageId pid : pids) {
    				pages.remove(pid);
    				lm.releaseLock(tid, pid);
    			}
    		}
    	}
    		
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	HeapFile f = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> dirtyPages = f.insertTuple(tid, t);
    	for (Page page : dirtyPages) {
    		page.markDirty(true, tid);
    		PageId pid = page.getId();
    		if (!pages.contains(pid))
    			pages.put(pid, page);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	HeapFile f = (HeapFile) Database.getCatalog().
    			getDatabaseFile(t.getRecordId().getPageId().getTableId());
    	ArrayList<Page> pages = f.deleteTuple(tid, t);
		for (Page page : pages) {
			page.markDirty(true, tid);
		}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	for (Page page : pages.values())
    		flushPage(page.getId());
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    	pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
    	Page page = pages.get(pid);
    	if (page != null) {
    		TransactionId tid = page.isDirty();
    		if (tid != null) {
    		    // append an update record to the log, with
    		    // a before-image and after-image.
    		    TransactionId dirtier = page.isDirty();
    		    if (dirtier != null){
    		      Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
    		      Database.getLogFile().force();
    		    }
    			Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
    		}
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	if (!lm.tidToPid.containsKey(tid)) return;
    	Set<PageId> pids = new HashSet<>();
    	for (PageId pid : lm.tidToPid.get(tid)) {
    		pids.add(pid);
    	}
    	for (PageId pid : pids) {
    		flushPage(pid);
    		if (pages.containsKey(pid)) {
    			Page page = pages.get(pid);
    			page.markDirty(false, null);
    			page.setBeforeImage();
    			lm.releaseLock(tid, pid);
    		}
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	for (PageId pid : pages.keySet()) {
    		if (pages.size() >= numPages) {
    			Page page = pages.get(pid);
    			if (page.isDirty() == null)
    				pages.remove(pid);
    			if (page.isDirty() != null) {
    				try {
						flushPage(pid);
						pages.remove(pid);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
    			}
    			//if (page.isDirty() == null)
    			//	pages.remove(pid);
    		}
    	}
    	if (pages.size() >= numPages)
    		throw new DbException("no bufferpool page can be evicted");
    }
    
    public int pageSize() {
    	return pages.size();
    }

}
