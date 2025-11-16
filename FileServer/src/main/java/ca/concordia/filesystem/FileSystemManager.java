package ca.concordia.filesystem;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import ca.concordia.filesystem.datastructures.FEntry;
import ca.concordia.filesystem.datastructures.FNode;
public class FileSystemManager {

    private final int MAXFILES = 5;
    private final int MAXBLOCKS = 10;
    private  static FileSystemManager instance;
    private final RandomAccessFile disk;
    private final ReentrantLock globalLock = new ReentrantLock();

    private static final int BLOCK_SIZE = 128; // Example block size

    private FEntry[] inodeTable; // Array of inodes
    private FNode[] dataBlocks; // Array of data blocks
    private boolean[] freeBlockList; // Bitmap for free blocks

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
    private FileSystemManager(String filename, int totalSize) throws IOException {
        this.disk = new RandomAccessFile(filename, "rw");
        this.inodeTable = new FEntry[MAXFILES];
        this.dataBlocks = new FNode[MAXBLOCKS];
        this.freeBlockList = new boolean[ totalSize / BLOCK_SIZE];    

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


    private void formatFileSystem() throws IOException {
       disk.seek(0); // move to the start of the file

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

    for (int i = 0; i < MAXFILES; i++) {
        inodeTable[i] = new FEntry();
       }

         for (int i = 0; i < MAXBLOCKS; i++) {
          dataBlocks[i] = new FNode();

         }
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
            if (entry.getFirstBlock() != -1) {
                int currentBlock = entry.getFirstBlock();
                while (currentBlock != -1) {
                 
                FNode node = dataBlocks[currentBlock];
                if (node.getBlockIndex() != -1) {
                    freeBlockList[node.getBlockIndex()] = false; // Mark block as used
                }
                currentBlock = node.getNext();
                 }
            }
        }


    }

    public void createFile(String fileName) throws Exception {
        // TODO
        if (fileName.length() > 11) {
            throw new Exception("Filename cannot be longer than 11 characters.");
        }
        globalLock.lock();
        try {
            // Check if file already exists
            for (FEntry entry : inodeTable) {
                if (entry != null && entry.getFilename().equals(fileName)) {
                    throw new Exception("File already exists.");
                }
            }

            // Find a free inode
            int freeInodeIndex = -1;
            for (int i = 0; i < MAXFILES; i++) {
                if (inodeTable[i] == null) {
                    freeInodeIndex = i;
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
           

        } finally {
            globalLock.unlock();
        }

        

    }

    public void deleteFile(String fileName) throws Exception {
       globalLock.lock();
       try {
        int fileIndex = findFileIndex(fileName);
        if (fileIndex == -1) {  
            throw new Exception("File not found."); 
       } 

       FEntry entry = inodeTable[fileIndex];    

         // Free data blocks    
        int currentBlock = entry.getFirstBlock();
        while (currentBlock != -1) {
            FNode node =dataBlocks[currentBlock];

            if (node.getBlockIndex() != -1) {
                long blockOffset = (long) node.getBlockIndex() * BLOCK_SIZE;
                disk.seek(blockOffset);
                disk.write(new byte[BLOCK_SIZE]); // Clear block data   
                freeBlockList[node.getBlockIndex()] = true; // Mark block as free   

            }

        node.setBlockIndex(-1); 
        int nextBlock = node.getNext();
        node.setNext(-1);
        writeFNodeToDisk(currentBlock);
        currentBlock = nextBlock;
        }
        entry.setFilename("");
        entry.setFilesize((short)0);
        entry.setFirstBlock((short)-1);
        writeFEntryToDisk(fileIndex);

       } finally {
        globalLock.unlock();
       }

    }

    public void writeFile(String fileName, byte[] data) throws Exception {
    
        if (data.length > BLOCK_SIZE * MAXBLOCKS) {
            throw new Exception("Data size exceeds maximum file size.");
        }   
        globalLock.lock();
        try {
            int fileIndex = findFileIndex(fileName);
            if (fileIndex == -1) {
                throw new Exception("File not found.");
            }
            FEntry entry = inodeTable[fileIndex];

            int requiredBlocks = (data.length + BLOCK_SIZE - 1) / BLOCK_SIZE;
            if (countfreeBlocks() < requiredBlocks) {
                throw new Exception("Not enough free space.");
            }

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
        } finally {
            globalLock.unlock();    


        }


    }

    public byte[] readFile(String fileName) throws Exception {
        globalLock.lock();
        try {
            int fileIndex = findFileIndex(fileName);
            if (fileIndex == -1) {
                throw new Exception("File not found.");
            }
            FEntry entry = inodeTable[fileIndex];
            byte[] data = new byte[entry.getFilesize()];

            int currentBlock = entry.getFirstBlock();
            int dataOffset = 0;

            while (currentBlock != -1 && dataOffset < data.length) {
                FNode node = dataBlocks[currentBlock];
                if(node.getBlockIndex() == -1) {
                    long blockOffset = (long) node.getBlockIndex() * BLOCK_SIZE;
                    disk.seek(blockOffset); 
                    int bytesToRead = Math.min(BLOCK_SIZE, data.length - dataOffset);
                    disk.readFully(data, dataOffset, bytesToRead);
                    dataOffset += bytesToRead;
                }

                currentBlock = node.getNext();
            }

            return data;
        } finally {
            globalLock.unlock();
        }
    }

    public String[] listFiles() {
        globalLock.lock();
        try {
            List<String> fileList = new ArrayList<>();
            for (FEntry entry : inodeTable) {
                if (entry != null && !entry.getFilename().isEmpty()) {
                    fileList.add(entry.getFilename());
                }
            }
            return fileList.toArray(new String[0]);
        } finally {
            globalLock.unlock();
        }
    }

    private void writecontentsToBlocks(int firstBlockIndex, byte[] data) throws IOException {
        int currentBlock = firstBlockIndex;
        int dataOffset = 0;

        while (currentBlock != -1 && dataOffset < data.length) {
            FNode node = dataBlocks[currentBlock];
            int blockIndex = node.getBlockIndex();
            long blockOffset = (long) blockIndex * BLOCK_SIZE;
            disk.seek(blockOffset);

            int bytesToWrite = Math.min(BLOCK_SIZE, data.length - dataOffset);
            disk.write(data, dataOffset, bytesToWrite);
            dataOffset += bytesToWrite;

            currentBlock = node.getNext();
        }
    }
    private int allocatedBlocks(byte[] data) throws IOException
    {
       int blocksneeded = (data.length + BLOCK_SIZE - 1) / BLOCK_SIZE;
            List<Integer> allocatedBlocks = new ArrayList<>();
            List<Integer> blockNodes = new ArrayList<>();

            for (int i = 0; i <MAXBLOCKS && allocatedBlocks.size() < blocksneeded; i++) {
                if (dataBlocks[i].getBlockIndex()==-1){
                    //find free data block
                    for (int j = 0; j < freeBlockList.length; j++) {
                        if (freeBlockList[j] && !allocatedBlocks.contains(j)) {
                            freeBlockList[j] = false; // Mark block as used
                            allocatedBlocks.add(j);
                            blockNodes.add(i);
                            break;
                        }
                    }

                }
            }
            if (blockNodes.size() < blocksneeded) {
                return -1; // Not enough free blocks
            }   

            //Link FNodes
            for (int i = 0; i < blockNodes.size(); i++) {
                int nodeIndex = blockNodes.get(i);
                FNode node = dataBlocks[nodeIndex];
                node.setBlockIndex(allocatedBlocks.get(i));
                node.setNext(i < blockNodes.size() - 1 ? blockNodes.get(i + 1) : -1);
                writeFNodeToDisk(nodeIndex);
            }
            return blockNodes.get(0); // Return first block index
    }
    private void writeFEntryToDisk(int index) throws IOException {
        long offset = index * (11 + 2 + 2); // filename (11 bytes) + filesize (2 bytes) + firstBlock (2 bytes)
        FEntry entry = inodeTable[index];
        disk.seek(offset);
       String filename = entry.getFilename();
       byte[] nameBytes =Arrays.copyOf(filename.getBytes(), 11);
       disk.write(nameBytes);  
       disk.writeShort(entry.getFilesize());
       disk.writeShort(entry.getFirstBlock()); 


    }

    private void writeFNodeToDisk(int index) throws IOException {
        long offset = MAXFILES * (11 + 2 + 2) + index * (2 + 2); // After FEntry area
        FNode node = dataBlocks[index];
        disk.seek(offset);
        disk.writeShort(node.getBlockIndex());
        disk.writeShort(node.getNext());
    }

    private int findFileIndex(String fileName) {
        for (int i = 0; i < MAXFILES; i++) {
            FEntry entry = inodeTable[i];
            if (entry != null && entry.getFilename().equals(fileName)) {
                return i;
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

    private void freefileBlocks(FEntry entry) throws IOException {
        int currentBlock = entry.getFirstBlock();
        while (currentBlock != -1) {
            FNode node = dataBlocks[currentBlock];
            if (node.getBlockIndex() != -1) {
                freeBlockList[node.getBlockIndex()] = true; // Mark block as free
                long blockOffset = (long) node.getBlockIndex() * BLOCK_SIZE;
                disk.seek(blockOffset);
                disk.write(new byte[BLOCK_SIZE]); // Clear block data   
            }
            node.setBlockIndex(-1); 
            int nextBlock = node.getNext(); 
            node.setNext(-1);
            writeFNodeToDisk(currentBlock); 
            currentBlock = nextBlock;
        }
      
    }
}
