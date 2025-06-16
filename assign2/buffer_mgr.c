#include "buffer_mgr.h"



// Buffer Manager Interface Pool Handling

RC initBufferPool(BM_BufferPool * bm, const char *const pageFileName, 
	const int numPages, ReplacementStrategy strategy,void *stratData)
{   
    BM_BufferPoolManagementInformation * bufferMgtData;
    bm = MAKE_POOL();
    bm->pageFile = pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bufferMgtData = bm->mgmtData;
    // Initializing management Information of the buffer
    RC fileOpenRC = openPageFile(pageFileName,bufferMgtData->fileHandle);
    if (fileOpenRC != RC_OK){
        THROW(fileOpenRC,"Could not open the page file");
    }
    bufferMgtData->numReadIO = 0;
    bufferMgtData->numWriteIO = 0;
    bufferMgtData->framePool = (char *) malloc (sizeof(char) * PAGE_SIZE * numPages);
    // Initializing PageHandles
    bufferMgtData->pageHandlePool = (BM_PageHandle *) malloc (sizeof(BM_PageHandle) * numPages);
    for (int i = 0; i<numPages; i++){ 
        BM_PageHandle *pHandle = &(bufferMgtData->pageHandlePool[i]);
        pHandle->pageNum = NO_PAGE;
        pHandle->data = bufferMgtData->framePool[i];
        pHandle->isDirty = FALSE;
        pHandle->fixCount = 0;
    }
    // Initializing strategyBuffer
    bufferMgtData->strategyBuffer = (BM_PageHandle **) malloc (sizeof(BM_PageHandle *) * numPages);
    for (int i = 0; i<numPages; i++){ 
        bufferMgtData->strategyBuffer[i] = &(bufferMgtData->pageHandlePool[i]);
    }
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm){
    // First check if all page have fixCount = 0
    for (int i = 0; i < bm->numPages; i++){
        if (bm->mgmtData->pageHandlePool[i].fixCount != 0){
            THROW(RC_BUFFER_WITH_PINNED_PAGES,"Cannot shutdown buffer pool as it contains pinned pages");
        }
    }
    // We save pages that are dirty
    forceFlushPool(bm);
    // Then we free all the memory that was allocated
    free(bm->mgmtData->pageHandlePool);
    free(bm->mgmtData->framePool);
    free(bm->mgmtData->strategyBuffer);
    closePageFile(bm->mgmtData->fileHandle);
    free(bm);
    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm){
    for (int i = 0; i < bm->numPages; i++){
        BM_PageHandle *pHandle = &(bm->mgmtData->pageHandlePool[i]);
        if (pHandle->isDirty == TRUE && pHandle->fixCount == 0){
            forcePage(bm, pHandle);
        }
    }
    return RC_OK;
}



// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    page->isDirty == TRUE;
    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
    if (page->fixCount <= 0){
        THROW(RC_FIX_COUNT_ZERO,"Cannot unpin a page that is not pinned");
    }
    page->fixCount --;
    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    writeBlock(page->pageNum, bm->mgmtData->fileHandle, page->data);
    bm->mgmtData->numWriteIO ++;
    page->isDirty == FALSE;
    return RC_OK;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle * page, const PageNumber pageNum){
    // First we check if the page is already buffered
    for (int i = 0; i < bm->numPages; i++){
        BM_PageHandle *pageHandle = &(bm->mgmtData->pageHandlePool[i]);
        if (pageHandle->pageNum == pageNum){
            page = pageHandle;
            page->fixCount ++;
            return RC_OK;
        }
    }
    // The requested page is not buffered, we need to read it from disk
    // First we look for an empty frame
    for (int i = 0; i < bm->numPages; i++){
        BM_PageHandle *pageHandle = &(bm->mgmtData->pageHandlePool[i]);
        if (pageHandle->pageNum == NO_PAGE){
            page = pageHandle;
            page->pageNum = pageNum;
            page->fixCount = 1;
            return readPageFromDisk(bm,page);
        }
    }
    // No empty frame, we need to evict a page
    RC evictResult;
    if (bm->strategy == RS_FIFO){
        evictResult = evictFIFO(bm, page);
    } else if (bm->strategy == RS_LRU){
        evictResult = evictLRU(bm, page);
    }
    if (evictResult != RC_OK){
        return evictResult;
    }
    page->pageNum = pageNum;
    page->fixCount = 1;
    return readPageFromDisk(bm,page);
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm){
    PageNumber * frameContent = (PageNumber *) malloc (sizeof(PageNumber) * bm->numPages);
    for (int i = 0; i < bm->numPages; i++){
        frameContent[i] = bm->mgmtData->pageHandlePool->pageNum;
    }
    return frameContent;
}

bool *getDirtyFlags (BM_BufferPool *const bm){
    bool * dirtyFlags = (bool *) malloc (sizeof(bool) * bm->numPages);
    for (int i = 0; i < bm->numPages; i++){
        dirtyFlags[i] = bm->mgmtData->pageHandlePool->isDirty; // an empty frame is always clean
    }
    return dirtyFlags;
}

int *getFixCounts (BM_BufferPool *const bm){
    int * fixCounts = (int *) malloc (sizeof(int) * bm->numPages);
    for (int i = 0; i < bm->numPages; i++){
        fixCounts[i] = bm->mgmtData->pageHandlePool->fixCount; // an empty frame has always 0 fix
    }
    return fixCounts;
}

int getNumReadIO (BM_BufferPool *const bm){
    return bm->mgmtData->numReadIO;
}

int getNumWriteIO (BM_BufferPool *const bm){
    return bm->mgmtData->numWriteIO;
}

// Strategy eviction function
RC evictFIFO(BM_BufferPool *const bm, BM_PageHandle *page){
    // strategyBuffer is a queue of pageHandle pointer where the key is the page time of insertion in the buffer
    BM_PageHandle ** strategyBuffer = bm->mgmtData->strategyBuffer;
    // First we need to find the first frame that can be evicted ,i.e. that has no fix
    for (int i = 0; i<bm->numPages; i++){
        BM_PageHandle *pageHandle = strategyBuffer[i];
        if (pageHandle->fixCount == 0){
            page = pageHandle;
            if (pageHandle->isDirty == TRUE){
                forcePage(bm, pageHandle);
            }
            updateQueue(i, bm, bm->numPages);
            return RC_OK;
        }
    }
    THROW(RC_FULL_BUFFER,"The Buffer is full of pinned pages");
}

RC evictLRU(BM_BufferPool *const bm, BM_PageHandle *page){ // TODO
    return RC_OK;
}

// Utility
RC readPageFromDisk(BM_BufferPool *const bm, BM_PageHandle *page){
    readBlock(page->pageNum, bm->mgmtData->fileHandle, page->data);
    bm->mgmtData->numReadIO ++;
    return RC_OK;
}

void updateQueue(int pos, void **queue, int queue_length){
    void * updatedElement = queue[pos];
    for(int i = pos; i < queue_length -1; i++){
        queue[i] = queue[i+1];
    }
    queue[queue_length-1] = updatedElement;
}