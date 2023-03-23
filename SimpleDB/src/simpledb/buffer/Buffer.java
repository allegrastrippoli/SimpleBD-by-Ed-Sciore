package simpledb.buffer;


import java.util.Objects;

import simpledb.file.*;
import simpledb.log.LogMgr;

/**
 * An individual buffer. A databuffer wraps a page 
 * and stores information about its status,
 * such as the associated disk block,
 * the number of times the buffer has been pinned,
 * whether its contents have been modified,
 * and if so, the id and lsn of the modifying transaction.
 * @author Edward Sciore
 */
public class Buffer {
	private FileMgr fm;
	private LogMgr lm;
	private Page contents;
	private BlockId blk = null; //  un riferimento al blocco che per ultimo e' stato caricato nella pagina
	private int pins = 0; // intero che indica il numero di pin su una pagina
	private int txnum = -1; // id of the transaction
	private int lsn = -1; // the LSN of the log record. A negative LSN value indicates that a log record was not necessary.
	private int timePin = -1; // istante in cui e' stato effettuato l’ultimo caricamento
	private int timeUnpin = -1; // istante in cui la pagina `e stata per l’ultima volta liberata

	public Buffer(FileMgr fm, LogMgr lm) {
		this.fm = fm;
		this.lm = lm;
		contents = new Page(fm.blockSize());
	}

	public Page contents() {
		return contents;
	}
	
	public int getPins() {
		return pins;
	}

	public void setPins(int pins) {
		this.pins = pins;
	}


	/**
	 * Returns a reference to the disk block
	 * allocated to the buffer.
	 * @return a reference to a disk block
	 */
	public BlockId block() {
		return blk;
	}

	public int getTimePin() {
		return timePin;
	}

	public void setTimePin(int timePin) {
		this.timePin = timePin;
	}

	public int getTimeUnpin() {
		return timeUnpin;
	}

	public void setTimeUnpin(int timeUnpin) {
		this.timeUnpin = timeUnpin;
	}

	public void setModified(int txnum, int lsn) {
		this.txnum = txnum;
		if (lsn >= 0)
			this.lsn = lsn;
	}

	/**
	 * Return true if the buffer is currently pinned
	 * (that is, if it has a nonzero pin count).
	 * @return true if the buffer is pinned
	 */
	public boolean isPinned() {
		return pins > 0;
	}

	public int modifyingTx() {
		return txnum;
	}

	/**
	 * Reads the contents of the specified block into
	 * the contents of the buffer.
	 * If the buffer was dirty, then its previous contents
	 * are first written to disk.
	 * @param b a reference to the data block
	 */
	void assignToBlock(BlockId b) {
		flush();
		blk = b;
		fm.read(blk, contents);
		pins = 0;
	}



	/**
	 * Write the buffer to its disk block if it is dirty.
	 */
	void flush() {
		if (txnum >= 0) {
			lm.flush(lsn);
			fm.write(blk, contents);
			txnum = -1;
		}
	}

	/**
	 * Increase the buffer's pin count.
	 */
	void pin(int opNumber) {
		pins++;
		this.timePin = opNumber;
	}

	/**
	 * Decrease the buffer's pin count.
	 */
	void unpin(int opNumber) {
		pins--;
		this.timeUnpin = opNumber;
	}
	
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Buffer other = (Buffer) obj;
		return Objects.equals(blk, other.blk);
	}
	
}