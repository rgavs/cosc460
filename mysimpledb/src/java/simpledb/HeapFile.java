package simpledb;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
	
	private File file;
	private TupleDesc tD;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tD = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
    	return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tD;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        InputStream input = null;
		byte[] data;
        try {
			input = new BufferedInputStream(new FileInputStream(file));
			data = new byte[BufferPool.PAGE_SIZE];
			if (pid.pageNumber()>this.numPages()){
				input.close();
				throw new RuntimeException("Page number out of bounds.");
			}
			input.skip(BufferPool.PAGE_SIZE*pid.pageNumber());
			input.read(data);
			input.close();
			return new HeapPage((HeapPageId) pid, data);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			e.printStackTrace();
		}
    	return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	RandomAccessFile raf = new RandomAccessFile(file, "rw");
        int pageNum = page.getId().pageNumber() * BufferPool.getPageSize();
        raf.skipBytes(pageNum);
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.floor(file.length()/BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	if (!tD.equals(t.getTupleDesc())){
    		throw new DbException("TupleDescs do not match.");
    	}
    	ArrayList<Page> returnArray = new ArrayList<Page>();
    	int i = 0;
    	for (; i < numPages(); i++){
    		HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
    		if (hp.getNumEmptySlots()!=0){
    			hp.insertTuple(t);
    			returnArray.add(hp);
    			return returnArray;
    		}
    	}
    	// Pages all full
    	HeapPage hp = new HeapPage(new HeapPageId(getId(), i), HeapPage.createEmptyPageData());    	
    	hp.insertTuple(t);
    	BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file, true));	
    	bos.write(hp.getPageData());
    	bos.flush();
    	bos.close();
    	writePage(hp);
    	returnArray.add(hp);
    	return returnArray;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	ArrayList<Page> returnArray = new ArrayList<Page>();
    	RecordId rid = t.getRecordId();
    	BufferPool b = Database.getBufferPool();
    	Page p = b.getPage(tid, rid.getPageId(), null);
    	HeapPage hp = (HeapPage) p;
    	hp.deleteTuple(t);
    	return returnArray; 
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        
	    class HeapFileIterator implements DbFileIterator{
	    	private HeapFile hf;
	    	private HeapPage curPage;
	    	private TransactionId tId;
			private Iterator<Tuple> tuples;
			int pagenum = -1;
			
			public HeapFileIterator(HeapFile heapfile, TransactionId tid){
				this.hf = heapfile;
				this.tId = tid;
			}
			@Override
			public void open() throws DbException, TransactionAbortedException{
				if (pagenum != -1){
					System.err.println("Iterator has already been opened.");
					throw new TransactionAbortedException();
				}
				curPage = (HeapPage) Database.getBufferPool().getPage(tId , new HeapPageId(hf.getId(),0), Permissions.READ_ONLY);
				tuples = curPage.iterator();
				pagenum = 0;
			}
			@Override
			public void close() {
				if (pagenum != -1){
					Database.getBufferPool().discardPage(curPage.getId());
				}
				pagenum = -1;
			}
			
			@Override
			public boolean hasNext(){
				if (pagenum != -1){
					if (pagenum < hf.numPages()-1){
						return true;
					}
					if ((pagenum == numPages()-1) && tuples.hasNext()){
						return true;
					}
				}
				return false;
			}
			
			@Override
			public Tuple next() throws DbException,
					TransactionAbortedException, NoSuchElementException {
				if (pagenum==-1) {
					throw new NoSuchElementException("Iterator not opened.");
				}
				if (hasNext()){
					if (tuples.hasNext()) {
						return tuples.next();
					}
					else{
						Database.getBufferPool().discardPage(curPage.getId());
						pagenum++;
						curPage = (HeapPage) Database.getBufferPool().getPage(tId, new HeapPageId(hf.getId(), pagenum), Permissions.READ_ONLY);
						tuples = curPage.iterator();
						if (tuples.hasNext()){
							return tuples.next();
						}
						else{
							throw new NoSuchElementException("There are no more tuples left.");
						}
					}
				}
				throw new NoSuchElementException("There are no more tuples left.");
			}
			
			/*
			 * Skips to next page
			 */
			
			@Override
			public void rewind() throws DbException,
					TransactionAbortedException {
				if (pagenum==-1){
					throw new TransactionAbortedException();
				}
				pagenum = 0;
				curPage = (HeapPage) Database.getBufferPool().getPage(tId, new HeapPageId(hf.getId(), pagenum), null);
				tuples = curPage.iterator();
			}
		}
	    return new HeapFileIterator(this,tid);
	}
}