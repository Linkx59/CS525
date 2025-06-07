Makefile:
- support make, make clean and make run

Structure Point:
- The file contains a descriptor page at the start of it containing the total number of page of the file (not counting the descriptor page(s))
- The management information is a struct containing only the posix file descriptor of the file

Technical Point:
- The appendEmptyBlock function has error detection in case it can not append a full block
- The updateTotalPageNumber function update both in memory and in the file the totalPageNumber
- For each function needing to read from/write to the file we first seek to the position of the wanted block as 
the posix cursor could be elsewhere.
- The number of descriptor page and initial accessible page is parametrable in storage_mgr.h
- ReadFirstBlock , ReadLastBlock, ReadCurrentBlock, ReadPreviousBlock and ReadNextBlock reuse ReadBlock
- ensureCapacity reuse AppendEmptyBlock if needed

Other:
- a test function was added (commented) to test a file with multiple pages and the storage of totalNumsPage in the file