package simpledb;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
        // some code goes here
        // not necessary for lab1
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
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
    	return new HeapFileIterator(this, tid);
    }
    
    class HeapFileIterator implements DbFileIterator{
		private TransactionId tId;
		private Iterator<Tuple> tuples;
		private int pagenum;
		private boolean open;
		private HeapFile hf;
		
		public HeapFileIterator(HeapFile hf, TransactionId tid){
			this.hf = hf;
			tId = tid;
			open = false;
			pagenum = 0;
		}

		public void open() throws DbException, TransactionAbortedException{
			try {
				tuples = ((HeapPage) Database.getBufferPool().getPage(tId, new HeapPageId(hf.getId(),pagenum), Permissions.READ_ONLY)).iterator();
			} catch (TransactionAbortedException | DbException e) {
				e.printStackTrace();
			}
			open = true;
		}
		public void close() { open = false; }

		public boolean hasNext(){
			if (!open){return false;}
			if (!tuples.hasNext() | (pagenum == numPages()-1)){
					return false;
			}
			return true;
		}
		
		public Tuple next() throws DbException,
				TransactionAbortedException, NoSuchElementException {
			if (! open) throw new NoSuchElementException("Iterator not opened.");
			if (! tuples.hasNext()) throw new NoSuchElementException("Next tuple does not exist.");
			if (tuples.hasNext()) return tuples.next();
			else{
				if (hasNextPage()){
					nextPage();
				}
			}
			return tuples.next();
		}
		
		/*
		 * Skips to next page
		 */
		public boolean hasNextPage(){
			return pagenum < (numPages()-1);
		}

		public void nextPage(){
			if (hasNextPage()){
				pagenum++;
				try {
					tuples = ((HeapPage)Database.getBufferPool().getPage(tId, new HeapPageId(hf.getId(),pagenum), Permissions.READ_ONLY)).iterator();
				} catch (TransactionAbortedException e) {
					e.printStackTrace();
				} catch (DbException e) {
					e.printStackTrace();
				}
			}
		}
		
		public void rewind() throws DbException,
				TransactionAbortedException {
			pagenum = 0;
			if (! open) throw new TransactionAbortedException();
			try {
				tuples = ((HeapPage) Database.getBufferPool().getPage(tId, new HeapPageId(hf.getId(), pagenum), Permissions.READ_ONLY)).iterator();
			} catch (TransactionAbortedException e) {
				e.printStackTrace();
			} catch (DbException e){
				e.printStackTrace();
			}
		}
	}
}