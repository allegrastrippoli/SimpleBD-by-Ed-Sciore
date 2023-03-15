package simpledb.file;

import java.util.HashMap;
import java.util.Map;

public class BlockStats {
	
	HashMap<String, Integer> writtenBlocksPerFile;
	
	HashMap<String, Integer> readBlocksPerFile;
	
	public BlockStats() {
		
		this.readBlocksPerFile = new HashMap<>();
		this.writtenBlocksPerFile = new HashMap<>();
	}
	
	public void logReadBlock(BlockId block) {
		
		String fileName = block.fileName();
		
		if(readBlocksPerFile.containsKey(fileName)) {
			Integer numRead = readBlocksPerFile.get(fileName);
			readBlocksPerFile.put(fileName, numRead+1);
		}
		else {
			readBlocksPerFile.put(fileName, 1);
		}
		
	}
	
	public void WrittenBlock(BlockId Block) {
		
		String fileName = Block.fileName();
		
		if(writtenBlocksPerFile.containsKey(fileName)) {
			Integer numWrite = writtenBlocksPerFile.get(fileName);
			writtenBlocksPerFile.put(fileName, numWrite+1);
		}
		else {
			writtenBlocksPerFile.put(fileName, 1);
		}
		
		
	}

	
	public void reset() {
		
		writtenBlocksPerFile.clear();
		
		readBlocksPerFile.clear();
		
	}
	
	@Override
	public String toString() {
		
		return "scrittura: " + writtenBlocksPerFile.toString() + "\n" + "lettura: " + readBlocksPerFile.toString();
		
	}
}
