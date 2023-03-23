package simpledb.buffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import simpledb.file.*;
import simpledb.log.LogMgr;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
public class BufferMgr {
	private Buffer[] bufferpool;
	private int numAvailable;
	private static final long MAX_TIME = 10000; // 10 seconds
	private static final String strategy = "LRU"; // posso implementarlo anche con classe enum
	private static int opNumber = -1;
	private int clock = 0;


	public static int getOpNumber() {
		return opNumber;
	}

	public static void setOpNumber(int opNumber) {
		BufferMgr.opNumber = opNumber;
	}

	public Buffer[] getBufferpool() {
		return bufferpool;
	}

	public void setBufferpool(Buffer[] bufferpool) {
		this.bufferpool = bufferpool;
	}


	/**
	 * Creates a buffer manager having the specified number 
	 * of buffer slots.
	 * This constructor depends on a {@link FileMgr} and
	 * {@link simpledb.log.LogMgr LogMgr} object.
	 * @param numbuffs the number of buffer slots to allocate
	 */
	public BufferMgr(FileMgr fm, LogMgr lm, int numbuffs) {
		bufferpool = new Buffer[numbuffs];
		numAvailable = numbuffs;
		for (int i=0; i<numbuffs; i++)
			bufferpool[i] = new Buffer(fm, lm);
	}

	/**
	 * Returns the number of available (i.e. unpinned) buffers.
	 * @return the number of available buffers
	 */
	public synchronized int available() {
		return numAvailable;
	}

	/**
	 * Flushes the dirty buffers modified by the specified transaction.
	 * @param txnum the transaction's id number
	 */
	public synchronized void flushAll(int txnum) {
		for (Buffer buff : bufferpool)
			if (buff.modifyingTx() == txnum)
				buff.flush();
	}
	
	public synchronized void flushAll() {
		for (Buffer buff : bufferpool)
				buff.flush();
	}


	/**
	 * Unpins the specified data buffer. If its pin count
	 * goes to zero, then notify any waiting threads.
	 * @param buff the buffer to be unpinned
	 */
	public synchronized void unpin(Buffer buff) {

		buff.unpin(opNumber++);

		if (!buff.isPinned()) {
			numAvailable++;
			notifyAll();
		}
	}

	/**
	 * Pins a buffer to the specified block, potentially
	 * waiting until a buffer becomes available.
	 * If no buffer becomes available within a fixed 
	 * time period, then a {@link BufferAbortException} is thrown.
	 * @param blk a clock to a disk block
	 * @return the buffer pinned to that block
	 */
	public synchronized Buffer pin(BlockId blk) {
		try {
			long timestamp = System.currentTimeMillis();
			Buffer buff = tryToPin(blk);
			while (buff == null && !waitingTooLong(timestamp)) {
				wait(MAX_TIME);
				buff = tryToPin(blk);
			}
			if (buff == null)
				throw new BufferAbortException();
			return buff;
		}
		catch(InterruptedException e) {
			throw new BufferAbortException();
		}
	}  

	private boolean waitingTooLong(long starttime) {
		return System.currentTimeMillis() - starttime > MAX_TIME;
	}

	/**
	 * Tries to pin a buffer to the specified block. 
	 * If there is already a buffer assigned to that block
	 * then that buffer is used;  
	 * otherwise, an unpinned buffer from the pool is chosen.
	 * Returns a null value if there are no available buffers.
	 * @param blk a clock to a disk block
	 * @return the pinned buffer
	 */
	private Buffer tryToPin(BlockId blk) {
		Buffer buff = findExistingBuffer(blk);
		if (buff == null) {
			buff = chooseUnpinnedBuffer();
			if (buff == null)
				return null;
			buff.assignToBlock(blk);
		}
		if (!buff.isPinned())
			numAvailable--;
		buff.pin(opNumber++);

		return buff;
	}

	private Buffer findExistingBuffer(BlockId blk) {
		for (Buffer buff : bufferpool) {
			BlockId b = buff.block();
			if (b != null && b.equals(blk))
				return buff;
		}
		return null;
	}


	// Diverse strategie per sostituire le pagine

	private Buffer chooseUnpinnedBufferNaive() {
		for (Buffer buff : bufferpool)
			if (!buff.isPinned())
				return buff;
		return null;
	}

	private Buffer chooseUnpinnedBufferFIFO() {
		List<Buffer> buffers = new ArrayList<>(Arrays.asList(bufferpool));

		Collections.sort(buffers, new PinComparator());

		for (Buffer buff : buffers)
			if (!buff.isPinned())
				return buff;
		return null;
	}


	private Buffer chooseUnpinnedBufferLRU() {
		List<Buffer> buffers = new ArrayList<>(Arrays.asList(bufferpool));

		Collections.sort(buffers, new UnpinComparator());

		for (Buffer buff : buffers) 
			if (!buff.isPinned())
				return buff;
			
		return null;
	}


		private Buffer chooseUnpinnedBufferCLOCK() {
			
			int k = this.clock;
			for(int i = 0; i < bufferpool.length; i++) {
				if(!this.bufferpool[k].isPinned()) {
					this.clock = (k + 1) % bufferpool.length;
					return this.bufferpool[k];
				}
				k = (k + 1) % bufferpool.length;
			}
			return null;
		}


//	private Buffer chooseUnpinnedBufferCLOCK() {
//
//
//		for(int i = clock; i < bufferpool.length; i++) {
//			if(!bufferpool[i].isPinned()) {
//				this.clock = (clock + 1) % bufferpool.length;
//				return bufferpool[i];
//			}
//			this.clock = (clock + 1) % bufferpool.length;
//		}
//
//		if(clock != 0)
//			for(int i = 0; i < clock; i++) {
//				if(!bufferpool[i].isPinned()) {
//					this.clock = (clock + 1) % bufferpool.length;
//					return bufferpool[i];
//				}
//				this.clock = (clock + 1) % bufferpool.length;
//			}
//
//		return null;
//	}



	private Buffer chooseUnpinnedBuffer() {

		if(strategy.equals("NAIVE")) 
			return chooseUnpinnedBufferNaive();

		/* FIFO deve scorrere i vari oggetti Buffer,
		 * guardare il campo timePin
		 * l'oggetto che ha min(timePin) && !isPinned verra' selezionato 
		 * se nessun oggetto rispetta la condizione, il metodo ritorna null
		 * 
		 * quindi si puo' costruire una lista ordinata 
		 * l'ordinamento e' basato proprio sul timePin crescente */

		if(strategy.equals("FIFO")) 
			return chooseUnpinnedBufferFIFO();

		if(strategy.equals("LRU")) 
			return chooseUnpinnedBufferLRU();
		

		if(strategy.equals("CLOCK")) 
			return chooseUnpinnedBufferCLOCK();

		return null;

	}
}
