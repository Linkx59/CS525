#include <stdlib.h>
#include "buffer_mgr.h"


// Buffer Manager Interface Pool Handling

RC initBufferPool(BM_BufferPool *const bm, char *const pageFileName, 
	const int numPages, ReplacementStrategy strategy,void *stratData)
{   
    BM_BufferPoolManagementInformation *bufferMgtData = (BM_BufferPoolManagementInformation *) malloc (sizeof(BM_BufferPoolManagementInformation));
    bm->pageFile = pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->mgmtData = bufferMgtData;
    // Initializing management Information of the buffer
    RC fileOpenRC = openPageFile(pageFileName,&(bufferMgtData->fileHandle));
    if (fileOpenRC != RC_OK){
        free(bufferMgtData);
        bm->mgmtData = NULL;
        THROW(fileOpenRC,"Could not open the page file");
    }
    bufferMgtData->numReadIO = 0;
    bufferMgtData->numWriteIO = 0;
    bufferMgtData->framePool = (char *) malloc (sizeof(char) * PAGE_SIZE * numPages);
    // Initializing BM_FrameInfo
    bufferMgtData->frameInfoPool = (BM_FrameInfo *) malloc (sizeof(BM_FrameInfo) * numPages);
    for (int i = 0; i<numPages; i++){ 
        BM_FrameInfo *frameInfo = &(bufferMgtData->frameInfoPool[i]);
        frameInfo->pageNum = NO_PAGE;
        frameInfo->isDirty = FALSE ;
        frameInfo->fixCount = 0;
    }
    
    
    // Initializing strategyBuffer
    bufferMgtData->strategyBuffer = (BM_FrameInfo **) malloc (sizeof(BM_FrameInfo *) * numPages);
    for (int i = 0; i<numPages; i++){ 
        bufferMgtData->strategyBuffer[i] = &(bufferMgtData->frameInfoPool[i]);
    }
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm){
    if (bm->mgmtData == NULL){
        THROW(RC_BUFFERPOOL_NOT_INITIALIZED,"Buffer not open");
    }
    // First check if all page have fixCount = 0
    for (int i = 0; i < bm->numPages; i++){
        if (bm->mgmtData->frameInfoPool[i].fixCount != 0){
            THROW(RC_BUFFER_WITH_PINNED_PAGES,"Cannot shutdown buffer pool as it contains pinned pages");
        }
    }
    // We save pages that are dirty
    forceFlushPool(bm);
    // Then we free all the memory that was allocated
    free(bm->mgmtData->frameInfoPool);
    free(bm->mgmtData->framePool);
    free(bm->mgmtData->strategyBuffer);
    closePageFile(&(bm->mgmtData->fileHandle));
    free(bm->mgmtData);
    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm){
    if (bm->mgmtData == NULL){
        THROW(RC_BUFFERPOOL_NOT_INITIALIZED,"Buffer not open");
    }
    for (int i = 0; i < bm->numPages; i++){
        BM_FrameInfo *frameInfo = &(bm->mgmtData->frameInfoPool[i]);
        if (frameInfo->isDirty == TRUE && frameInfo->fixCount == 0){
            forceFrame(bm, frameInfo, i);
        }
    }
    return RC_OK;
}



// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    if (bm->mgmtData == NULL){
        THROW(RC_BUFFERPOOL_NOT_INITIALIZED,"Buffer not open");
    }
    int frameIndex = getFrameIndex(bm,page->pageNum);
    if (frameIndex < 0){ // not frame corresponding to the page
        THROW(RC_FRAME_NOT_FOUND,"No frame corresponding to the page");
    }
    BM_FrameInfo *frameInfo = &(bm->mgmtData->frameInfoPool[frameIndex]);
    frameInfo->isDirty = TRUE;
    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
    if (bm->mgmtData == NULL){
        THROW(RC_BUFFERPOOL_NOT_INITIALIZED,"Buffer not open");
    }
    BM_FrameInfo *frameInfo = &(bm->mgmtData->frameInfoPool[getFrameIndex(bm,page->pageNum)]);
    if (frameInfo->fixCount <= 0){
        THROW(RC_FIX_COUNT_ZERO,"Cannot unpin a page that is not pinned");
    }
    frameInfo->fixCount --;
    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    if (bm->mgmtData == NULL){
        THROW(RC_BUFFERPOOL_NOT_INITIALIZED,"Buffer not open");
    }
    int frameIndex = getFrameIndex(bm,page->pageNum);
    if (frameIndex < 0){ // not frame corresponding to the page
        THROW(RC_FRAME_NOT_FOUND,"No frame corresponding to the page");
    }
    BM_FrameInfo *frameInfo = &(bm->mgmtData->frameInfoPool[frameIndex]);
    return forceFrame(bm, frameInfo, frameIndex);
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
    if (bm->mgmtData == NULL){
        THROW(RC_BUFFERPOOL_NOT_INITIALIZED,"Buffer not open");
    }
    // First we check if the page is already buffered
    page->pageNum = pageNum;
    int frameIndex = getFrameIndex(bm,pageNum);
    if (frameIndex >= 0){
        BM_FrameInfo *frameInfo = &(bm->mgmtData->frameInfoPool[frameIndex]);
        page->data = &(bm->mgmtData->framePool[frameIndex * PAGE_SIZE]);
        frameInfo->fixCount ++;
        if (bm->strategy == RS_LRU){ // update last access time of page (by changing its position in the queue)
            int position = getPositionQueue(pageNum, bm->mgmtData->strategyBuffer, bm->numPages);
            updateQueue(position, (void **) bm->mgmtData->strategyBuffer, bm->numPages); // Could be optimized to only traverse the queue one time
        }
        return RC_OK;
    }
    // The requested page is not buffered, we need to read it from disk
    // First we ensure that the page file has at least pageNum+1 pages
    ensureCapacity(pageNum+1, &(bm->mgmtData->fileHandle));
    // Next we look for an empty frame
    for (int i = 0; i < bm->numPages; i++){
        BM_FrameInfo *frameInfo = &(bm->mgmtData->frameInfoPool[i]);
        if (frameInfo->pageNum == NO_PAGE){
            page->data = &(bm->mgmtData->framePool[i * PAGE_SIZE]);
            RC result = readPageFromDisk(bm,page);
            if (result != RC_OK){
                return result;
            }
            frameInfo->pageNum = pageNum;
            frameInfo->fixCount = 1;
            return RC_OK;
        }
    }
    
    // No empty frame, we need to evict a page from the buffer
    if (bm->strategy == RS_FIFO){
        return evictFIFO(bm, page);
    } else if (bm->strategy == RS_LRU){
        return evictLRU(bm, page);
    }
    THROW(RC_STRATEGY_NOT_IMPLEMENTED,"Unknown Strategy , can't evict");
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm){
    PageNumber * frameContent = (PageNumber *) malloc (sizeof(PageNumber) * bm->numPages);
    for (int i = 0; i < bm->numPages; i++){
        frameContent[i] = bm->mgmtData->frameInfoPool[i].pageNum;
    }
    return frameContent;
}

bool *getDirtyFlags (BM_BufferPool *const bm){
    bool * dirtyFlags = (bool *) malloc (sizeof(bool) * bm->numPages);
    for (int i = 0; i < bm->numPages; i++){
        dirtyFlags[i] = bm->mgmtData->frameInfoPool[i].isDirty; // an empty frame is always clean
    }
    return dirtyFlags;
}

int *getFixCounts (BM_BufferPool *const bm){
    int * fixCounts = (int *) malloc (sizeof(int) * bm->numPages);
    for (int i = 0; i < bm->numPages; i++){
        fixCounts[i] = bm->mgmtData->frameInfoPool[i].fixCount; // an empty frame has always 0 fix
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
    // strategyBuffer is a queue of FrameInfo pointer where the key is the page time of insertion in the buffer
    BM_FrameInfo ** strategyBuffer = bm->mgmtData->strategyBuffer;
    // First we need to find the first frame that can be evicted ,i.e. that has no fix
    for (int i = 0; i<bm->numPages; i++){
        BM_FrameInfo *frameInfo = strategyBuffer[i];
        if (frameInfo->fixCount == 0){
            int frameIndex = getFrameIndex(bm, frameInfo->pageNum);
            if (frameInfo->isDirty == TRUE){
                forceFrame(bm, frameInfo, frameIndex);
                frameInfo->isDirty = FALSE;
            }
            page->data = &(bm->mgmtData->framePool[frameIndex*PAGE_SIZE]);
            RC result = readPageFromDisk(bm,page);
            if (result != RC_OK){
                return result;
            }
            frameInfo->pageNum = page->pageNum;
            frameInfo->fixCount = 1;
            updateQueue(i, (void **) strategyBuffer, bm->numPages);
            return RC_OK;
        }
    }
    THROW(RC_FULL_BUFFER,"The Buffer is full of pinned pages");
}

RC evictLRU(BM_BufferPool *const bm, BM_PageHandle *page){
    // strategyBuffer is a queue of FrameInfo pointer where the key is the page time of last access
    BM_FrameInfo ** strategyBuffer = bm->mgmtData->strategyBuffer;
    // First we need to find the first frame that can be evicted ,i.e. that has no fix
    for (int i = 0; i<bm->numPages; i++){
        BM_FrameInfo *frameInfo = strategyBuffer[i];
        if (frameInfo->fixCount == 0){
            int frameIndex = getFrameIndex(bm, frameInfo->pageNum);
            if (frameInfo->isDirty == TRUE){
                forceFrame(bm, frameInfo, frameIndex);
                frameInfo->isDirty = FALSE;
            }
            page->data = &(bm->mgmtData->framePool[frameIndex*PAGE_SIZE]);
            RC result = readPageFromDisk(bm,page);
            if (result != RC_OK){
                return result;
            }
            frameInfo->pageNum = page->pageNum;
            frameInfo->fixCount = 1;
            updateQueue(i, (void **) strategyBuffer, bm->numPages);
            return RC_OK;
        }
    }
    THROW(RC_FULL_BUFFER,"The Buffer is full of pinned pages");
}

// Utility
RC readPageFromDisk(BM_BufferPool *const bm, BM_PageHandle *page){
    bm->mgmtData->numReadIO ++;
    return readBlock(page->pageNum, &(bm->mgmtData->fileHandle), page->data);
}

void updateQueue(int pos, void **queue, int queue_length){
    void * updatedElement = queue[pos];
    for(int i = pos; i < queue_length -1; i++){
        queue[i] = queue[i+1];
    }
    queue[queue_length-1] = updatedElement;
}

int getPositionQueue(PageNumber pageNum, BM_FrameInfo **queue, int queue_length){
    for(int i=0; i < queue_length; i++){
        if (queue[i]->pageNum == pageNum){
            return i;
        }
    }
    return -1;

}

int getFrameIndex(BM_BufferPool *const bm, PageNumber pageNum){ 
    for (int i = 0; i<bm->numPages; i++){
        if (bm->mgmtData->frameInfoPool[i].pageNum == pageNum){
            return i;
        }
    }
    return -1;
}

RC forceFrame(BM_BufferPool *const bm, BM_FrameInfo * frameInfo, int frameIndex){
    bm->mgmtData->numWriteIO ++;
    frameInfo->isDirty = FALSE;
    return writeBlock(frameInfo->pageNum, &(bm->mgmtData->fileHandle), &(bm->mgmtData->framePool[frameIndex*PAGE_SIZE]));
}