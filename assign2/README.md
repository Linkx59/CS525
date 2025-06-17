Makefile:
    To compile all test files : make all
    To run test_assign2_1 : make run
    To run test_assign2_2 : make run2
    To clean : make clean

    [test_assign2_2 only contain the test for error as LRU_K is not implemented]

Code Structure:
    A bufferPool contains 3 "pool" :
        - FramePool : the data from the pages on file
        - FrameInfoPool : Information about each frame (FrameInfo i is linked to frame i) = Dirty, PageNum and FixCount
        - StrategyBuffer : for FIFO it is a "queue" (we can remove elements that are not at the front) of all frameInfo pointer
                           for LRU it is a "queue" where element at the front are the oldest

Code Logic:
    When pinning a page there is 3 possibility:
        - The page is already buffered
        - The page is not buffered and there is an unused frame (pageNum = NO_PAGE) in the buffer pool
        - The page is not buffered and we need to evict a frame (if possible else Error)
    Reading a page from the disk has a dedicated function to ensure that we count the read for statistic as it is done in multiple place. Similarly writing to disk has a dedicated function (forceFrame) for the same reason.
    
        