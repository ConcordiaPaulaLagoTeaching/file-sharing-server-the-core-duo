package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;
import ca.concordia.filesystem.datastructures.FNode;

import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
public class FileSystemManager {

    private final int MAXFILES = 5;
    private final int MAXBLOCKS = 10;
    private  static FileSystemManager instance;
    private final RandomAccessFile disk;
     private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
    

    private static final int BLOCK_SIZE = 128; // Example block size
    private static final int FENTRY_SIZE = 15; // 11 (filename) + 2 (size) + 2 (firstBlock)
    private static final int FNODE_SIZE = 4;   // 2 (blockIndex) + 2 (next)

    private FEntry[] inodeTable; // Array of inodes
    private FNode[] dataBlocks; // Array of data blocks
    private boolean[] freeBlockList; // Bitmap for free blocks

    private final int metadataBlocks;
    private final int dataStartBlock;
    
    public static synchronized FileSystemManager getInstance(String filename, int totalSize) {
        if (instance == null) {
          try{
            instance = new FileSystemManager(filename, totalSize);
          } catch (IOException e) {
            throw new RuntimeException("Failed to initialize FileSystemManager", e);
        }
     
    }
       return instance;
   }
    public static synchronized void resetInstance() {
       if (instance != null) {
            try {
                instance.disk.close(); 
            } catch (IOException e) {
                System.err.println("Error closing disk file: " + e.getMessage());
            }
            instance = null;
        }
    }

    public FileSystemManager(String filename, int totalSize) throws IOException {
        this.disk = new RandomAccessFile(filename, "rw");
        this.inodeTable = new FEntry[MAXFILES];
        this.dataBlocks = new FNode[MAXBLOCKS];
        this.freeBlockList = new boolean[MAXBLOCKS];   
        
        this.metadataBlocks = (int) Math.ceil((double)(MAXFILES * FENTRY_SIZE + MAXBLOCKS * FNODE_SIZE) / BLOCK_SIZE);
        this.dataStartBlock = metadataBlocks;

        initializeMemoryStructures();
        // Initialize all blocks as free
        for (int i = 0; i < MAXBLOCKS; i++) {
            freeBlockList[i] = true;
        }

        // Format the file system
        if (disk.length() == 0) {
            formatFileSystem();
        } else {    
            // Load existing file system structures from disk
            loadFileSystem();
        }



    }
    private void initializeMemoryStructures() {
        for (int i = 0; i < MAXFILES; i++) {
            inodeTable[i] = new FEntry("", (short)0, (short)-1);
        }
        for (int i = 0; i < MAXBLOCKS; i++) {
            dataBlocks[i] = new FNode((short)-1);
            dataBlocks[i].setNext((short)-1);
        }
    }

    private void formatFileSystem() throws IOException {
       disk.seek(0); // move to the start of the file
        disk.setLength(0); // Clear existing content
       //FEntry initialization
       for (int i = 0; i < MAXFILES; i++) {
        disk.write(new byte[11]); // Assuming each FEntry takes 11 bytes
        disk.writeShort(0); // filesize
        disk.writeShort(-1); // firstBlock pointer  
       }

       //FNode initialization
       for (int i = 0; i < MAXBLOCKS; i++) {
        disk.writeShort(-1); // blockIndex
        disk.writeShort(-1); // next pointer
       }
        long current = disk.getFilePointer();
        long metadataEnd = (long) metadataBlocks * BLOCK_SIZE;
        if (current < metadataEnd) {
            disk.write(new byte[(int) (metadataEnd - current)]);
        }   

         byte[] zeroBlock = new byte[BLOCK_SIZE];
        for (int i = 0; i < MAXBLOCKS; i++) {
            disk.write(zeroBlock);
        }
        disk.getFD().sync();
    }

    private void loadFileSystem() throws IOException {
        disk.seek(0); // move to the start of the file
        // Load FEntry structures
        for (int i = 0; i < MAXFILES; i++) {
            byte[] nameBytes = new byte[11];
            disk.readFully(nameBytes);
            String filename = new String(nameBytes).trim();
            short filesize = disk.readShort();
            short firstBlock = disk.readShort();
            inodeTable[i] = new FEntry(filename, filesize, firstBlock);

        }   

        // Load FNode structures
        for (int i = 0; i < MAXBLOCKS; i++) {
            short blockIndex = disk.readShort();
            short next = disk.readShort();
            dataBlocks[i] = new FNode(blockIndex);
            dataBlocks[i].setNext(next);
        }

        // Reconstruct free block list
        Arrays.fill(freeBlockList, true); // Assume all blocks are free initially

        for (FEntry entry : inodeTable) {
            if (entry.getFirstBlock() != -1 &&entry != null) {
                int currentBlock = entry.getFirstBlock();
                while (currentBlock != -1 && currentBlock<dataBlocks.length) {      
                 
                FNode node = dataBlocks[currentBlock];
                if (node.getBlockIndex() != -1 &&node != null &&node.getBlockIndex()<freeBlockList.length) {
                    freeBlockList[node.getBlockIndex()] = false; // Mark block as used
                }
                currentBlock = node !=null ? node.getNext(): -1;
                 }
            }
        }


    }

    public void createFile(String fileName) throws Exception {
        
        if (fileName.length() > 11) {
            throw new Exception("Filename cannot be longer than 11 characters.");
        }
        readWriteLock.writeLock().lock();
        try {
            // Check if file already exists
            for (int i = 0; i < MAXFILES; i++) {
            FEntry entry = inodeTable[i];
             if (entry != null && entry.getFilename() != null) {
                System.out.println("Checking entry " + i + ": '" + entry.getFilename().trim() + "', firstBlock: " + entry.getFirstBlock());
                if (fileName.equals(entry.getFilename().trim())) {
                    throw new Exception("File already exists.");
                }
            }
        }

            // Find a free inode
            int freeInodeIndex = -1;
            for (int i = 0; i < MAXFILES; i++) {
                FEntry entry = inodeTable[i];
                if (entry != null && (entry.getFilename()==null || entry.getFilename().trim().isEmpty()||entry.getFirstBlock()==-1)) {
                    freeInodeIndex = i;
                    System.out.println("Found free inode at index: " + i);
                    break;
                }
            }
            if (freeInodeIndex == -1) {
                throw new Exception("Maximum file limit reached.");
            }

            // Create new FEntry
            inodeTable[freeInodeIndex].setFilename(fileName);
            inodeTable[freeInodeIndex].setFilesize((short)0);
            inodeTable[freeInodeIndex].setFirstBlock((short)-1);    


            writeFEntryToDisk(freeInodeIndex);
            disk.getFD().sync();
            System.out.println("Successfully created file: " + fileName + " at index: " + freeInodeIndex);

        } finally {
        readWriteLock.writeLock().unlock();
        }

        

    }

    public void deleteFile(String fileName) throws Exception {
       readWriteLock.writeLock().lock();
       try {
        int fileIndex = findFileIndex(fileName);
        if (fileIndex == -1) {  
            throw new Exception("File not found."); 
       } 

       FEntry entry = inodeTable[fileIndex];    

         // Free data blocks    
        int currentBlock = entry.getFirstBlock();
        while (currentBlock != -1 && currentBlock<dataBlocks.length) {
            FNode node =dataBlocks[currentBlock];
            if (node!=null){
            if (node.getBlockIndex() >= 0&& node.getBlockIndex() < freeBlockList.length) {
                long blockOffset = getDataBlockOffset(node.getBlockIndex());
                disk.seek(blockOffset);
                disk.write(new byte[BLOCK_SIZE]); // Clear block data   
                freeBlockList[node.getBlockIndex()] = true; // Mark block as free   

            }
            
            node.setBlockIndex(-1); 
            int nextBlock = node.getNext();
            node.setNext(-1);
            writeFNodeToDisk(currentBlock);
            currentBlock = nextBlock;
            } else {
             currentBlock = -1;
            }
       }
        entry.setFilename("");
        entry.setFilesize((short)0);
        entry.setFirstBlock((short)-1);
        writeFEntryToDisk(fileIndex);
         disk.getFD().sync();

       } finally {
        readWriteLock.writeLock().unlock();
       }

    }

    public void writeFile(String fileName, byte[] data) throws Exception {
    
        if (data.length > BLOCK_SIZE * MAXBLOCKS) {
            throw new Exception("Data size exceeds maximum file size.");
        }   
        readWriteLock.writeLock().lock();
        try {
            int fileIndex = findFileIndex(fileName);
            System.out.println("Looking for file: " + fileName);
            if (fileIndex == -1) {
                 System.out.println("Current files in system:");
            for (int i = 0; i < MAXFILES; i++) {
                FEntry entry = inodeTable[i];
                if (entry != null && entry.getFilename() != null) {
                    System.out.println("  Index " + i + ": '" + entry.getFilename().trim() + "', firstBlock: " + entry.getFirstBlock());
                }
            }
                throw new Exception("File not found.");
            }
            FEntry entry = inodeTable[fileIndex];

            int requiredBlocks = (data.length + BLOCK_SIZE - 1) / BLOCK_SIZE;
            if (countfreeBlocks() < requiredBlocks) {
                throw new Exception("Not enough free space.");
            }
            short originalFirstBlock = entry.getFirstBlock();
            try {
            freefileBlocks(entry);
            // Allocate new blocks
            int firstBlockIndex = allocatedBlocks(data);
            if (firstBlockIndex == -1) {
                throw new Exception("Failed to allocate blocks.");
            }

            //Update FEntry
            entry.setFirstBlock((short)firstBlockIndex);
            entry.setFilesize((short)data.length);
            writeFEntryToDisk(fileIndex);

            // Write data to blocks
            writecontentsToBlocks(firstBlockIndex, data);
            disk.getFD().sync();
            } catch (Exception e) {
                // Rollback on failure
                entry.setFirstBlock(originalFirstBlock);
                writeFEntryToDisk(fileIndex);
                throw e;
            }   

        } finally {
            readWriteLock.writeLock().unlock();    


        }


    }

    public byte[] readFile(String fileName) throws Exception {
        readWriteLock.readLock().lock();
        try {
            int fileIndex = findFileIndex(fileName);
            if (fileIndex == -1) {
                throw new Exception("File not found.");
            }
            FEntry entry = inodeTable[fileIndex];
            byte[] data = new byte[entry.getFilesize()];
            System.out.println("DEBUG: Reading file '" + fileName + "' with size: " + entry.getFilesize());
            System.out.println("DEBUG: First block: " + entry.getFirstBlock());
            int currentBlock = entry.getFirstBlock();
            int dataOffset = 0;

            while (currentBlock != -1 && dataOffset < data.length && currentBlock<dataBlocks.length) {
                FNode node = dataBlocks[currentBlock];
                if(node !=null && node.getBlockIndex()>=0 ) {
                    long blockOffset = getDataBlockOffset(node.getBlockIndex());
                    disk.seek(blockOffset); 
                    int bytesToRead = Math.min(BLOCK_SIZE, data.length - dataOffset);
                     System.out.println("DEBUG: Reading from block " + node.getBlockIndex() + ", offset " + dataOffset + ", bytes " + bytesToRead);
                    disk.readFully(data, dataOffset, bytesToRead);
                    dataOffset += bytesToRead;
                }

                currentBlock = node !=null ? node.getNext(): -1;
            }
            System.out.println("DEBUG: Read completed, total bytes: " + dataOffset);
            return data;
        } finally {
           readWriteLock.readLock().unlock();
        }
    }

    public String[] listFiles() {
        readWriteLock.readLock().lock();
        try {
            List<String> fileList = new ArrayList<>();
            for (FEntry entry : inodeTable) {
                if (entry != null && entry.getFilename()!=null && !entry.getFilename().trim().isEmpty()) {
                    fileList.add(entry.getFilename().trim());
                }
            }
            return fileList.toArray(new String[0]);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    private void writecontentsToBlocks(int firstBlockIndex, byte[] data) throws IOException {
        int currentBlock = firstBlockIndex;
        int dataOffset = 0;

        while (currentBlock != -1 && dataOffset < data.length && currentBlock<dataBlocks.length) {
            FNode node = dataBlocks[currentBlock];
            if (node != null && node.getBlockIndex()>=0) {
            long blockOffset = getDataBlockOffset(node.getBlockIndex());
            disk.seek(blockOffset);

            int bytesToWrite = Math.min(BLOCK_SIZE, data.length - dataOffset);
           
            disk.write(data, dataOffset, bytesToWrite);
            dataOffset += bytesToWrite;
            
        }else {
           System.out.println("DEBUG: Invalid node during write at currentBlock: " + currentBlock);
        }
        currentBlock = node != null ? node.getNext(): -1;
        }
        System.out.println("DEBUG: Write completed, total bytes written: " + dataOffset);
    }
    private int allocatedBlocks(byte[] data) throws IOException
    {
       int blocksneeded = (data.length + BLOCK_SIZE - 1) / BLOCK_SIZE;
            List<Integer> allocatedBlocks = new ArrayList<>();
            List<Integer> blockNodes = new ArrayList<>();
            System.out.println("DEBUG: Need " + blocksneeded + " blocks for data size " + data.length);

            for (int i = 0; i <MAXBLOCKS && blockNodes.size() < blocksneeded; i++) {
                if (dataBlocks[i].getBlockIndex()<0 && dataBlocks[i]!=null) {
                    blockNodes.add(i);
                }
            }
                    //find free data block
            for (int j = 0; j < freeBlockList.length && allocatedBlocks.size() < blocksneeded; j++) {
                        if (freeBlockList[j]) {
                            freeBlockList[j] = false; // Mark block as used
                            allocatedBlocks.add(j);
                            System.out.println("DEBUG: Allocated block " + j);
                        
                    

                }
            }
            if (blockNodes.size() < blocksneeded || allocatedBlocks.size() < blocksneeded) {
                System.out.println("DEBUG: Failed to allocate enough blocks. Needed: " + blocksneeded + ", Got: " + blockNodes.size());
                for (Integer block : allocatedBlocks) {
                    freeBlockList[block] = true; // Rollback
                }   
                return -1; // Not enough free blocks
            }   

            //Link FNodes
            for (int idx = 0; idx < blockNodes.size(); idx++) {
                int nodeIndex = blockNodes.get(idx);
                FNode node = dataBlocks[nodeIndex];
                node.setBlockIndex(allocatedBlocks.get(idx).shortValue());
                short nextPointer = (idx < blockNodes.size() - 1) ? blockNodes.get(idx + 1).shortValue() : -1;
                node.setNext(nextPointer);
                writeFNodeToDisk(nodeIndex);
                System.out.println("DEBUG: Linked FNode " + nodeIndex + " -> block " + allocatedBlocks.get(idx) + " -> next " + nextPointer);
            }
            System.out.println("DEBUG: First block index: " + blockNodes.get(0));
            return blockNodes.get(0); // Return first block index
    }
    private void writeFEntryToDisk(int index) throws IOException {
        long offset = (long) index * FENTRY_SIZE; // filename (11 bytes) + filesize (2 bytes) + firstBlock (2 bytes)
        FEntry entry = inodeTable[index];
        disk.seek(offset);
       String filename = entry.getFilename()!= null ? entry.getFilename() : "";
       byte[] nameBytes =Arrays.copyOf(filename.getBytes(), 11);
       disk.write(nameBytes);  
       disk.writeShort(entry.getFilesize());
       disk.writeShort(entry.getFirstBlock()); 


    }

    private void writeFNodeToDisk(int index) throws IOException {
        long offset = MAXFILES * FENTRY_SIZE + index * FNODE_SIZE; // After FEntry area
        FNode node = dataBlocks[index];
        disk.seek(offset);
        disk.writeShort(node.getBlockIndex());
        disk.writeShort(node.getNext());
    }

    private int findFileIndex(String fileName) {
         System.out.println("findFileIndex searching for: '" + fileName + "'");
        for (int i = 0; i < MAXFILES; i++) {
            FEntry entry = inodeTable[i];
            if (entry != null && entry.getFilename() != null) {
            String currentFilename = entry.getFilename().trim();
            System.out.println("  Checking index " + i + ": '" + currentFilename + "', firstBlock: " + entry.getFirstBlock());
            
            if (fileName.equals(currentFilename)) {
                return i;
            }

        }
        }
        return -1; // File not found
    }   

    private int countfreeBlocks() {
        int count = 0;
        for (boolean isFree : freeBlockList) {
            if (isFree) {
                count++;
            }
        }
        return count;
    }

    private long getDataBlockOffset(int blockIndex) {
        return (long) blockIndex * BLOCK_SIZE + (long) dataStartBlock * BLOCK_SIZE;
    }
    private void freefileBlocks(FEntry entry) throws IOException {
        int currentBlock = entry.getFirstBlock();
        while (currentBlock != -1 && currentBlock<dataBlocks.length ) {
            FNode node = dataBlocks[currentBlock];
            if (node != null) {  
            if (node.getBlockIndex() >=0 && node.getBlockIndex()<freeBlockList.length) {
                freeBlockList[node.getBlockIndex()] = true; // Mark block as free
                     // long blockOffset = (long) node.getBlockIndex() * BLOCK_SIZE;
                // disk.seek(blockOffset);
                // disk.write(new byte[BLOCK_SIZE]); // Clear block data   
            }
           
            
            node.setBlockIndex((short)-1); 
            int nextBlock = node.getNext(); 
            node.setNext(-1);
            writeFNodeToDisk(currentBlock); 
            currentBlock = nextBlock;
    
        } else {
            currentBlock = -1;
        }
    }
    }
}
