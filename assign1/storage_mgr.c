#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "dberror.h"
#include "storage_mgr.h"

/* manipulating page files */
void initStorageManager (void){};

RC createPageFile (char *fileName){
    int init_page_number = INIT_PAGE_NUMBER;
    FILE *f = fopen(fileName,"wb");
    if (f == NULL){
        THROW(RC_FILE_NOT_FOUND, "The File could not be created\n");
    };
    /* We reserve a page sized zone at the start of the file for information like totalNumPages */
    fwrite(&init_page_number, sizeof(int), 1, f);
    /* We init the rest of the descriptor page and the first useable page with '/0' */
    for(int i = 0; i < (INIT_PAGE_NUMBER * PAGE_SIZE) + (DESCRIPTOR_PAGE_NUMBER * PAGE_SIZE - sizeof(int)); i++){
        char empty = '\0';
        fwrite(&empty,sizeof(char),1,f);
    }
    fclose(f);
    return RC_OK;
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle){
    SM_FileManagementInfo fMngInfo;
    FILE *f;
    if (fHandle == NULL){
        THROW(RC_FILE_HANDLE_NOT_INIT,"fHandle is NULL");
    }
    f = fopen(fileName, "rb+");
    if (f == NULL){
        THROW(RC_FILE_NOT_FOUND,"No file found");
    }
    fHandle->fileName = fileName;
    fHandle->curPagePos = 0;
    fMngInfo.posixFileDescriptor = f;
    fHandle->mgmtInfo = fMngInfo;
    /* We read the total number of pages of the file stored in the descriptor page*/
    fread(&(fHandle->totalNumPages),sizeof(int),1,f);
    /* We seek to the first useable page */
    fseek(f,ACCESSIBLE_PAGE_OFFSET,SEEK_SET);
    return RC_OK;
}

RC closePageFile (SM_FileHandle *fHandle){
    if (fHandle == NULL){
        THROW(RC_FILE_HANDLE_NOT_INIT,"fHandle is NULL");
    }
    fclose(fHandle->mgmtInfo.posixFileDescriptor);
    return RC_OK;
}

RC destroyPageFile (char *fileName){
    if(remove(fileName) != 0){
        THROW(RC_FILE_NOT_FOUND,"Unable to delete due to permissions or file do not exist");
    }
    return RC_OK;
}

RC updateTotalPageNumber (int newTotalPageNumber, SM_FileHandle *fHandle){
    if (fHandle == NULL){
        THROW(RC_FILE_HANDLE_NOT_INIT,"fHandle is NULL");
    }
    fHandle->totalNumPages = newTotalPageNumber;
    fseek(fHandle->mgmtInfo.posixFileDescriptor, 0, SEEK_SET);
    fwrite(&(fHandle->totalNumPages), sizeof(int), 1, fHandle->mgmtInfo.posixFileDescriptor);
    return RC_OK;
}

/* reading blocks from disc */
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (fHandle == NULL){
        THROW(RC_FILE_HANDLE_NOT_INIT,"fHandle is NULL");
    }
    if (pageNum >= fHandle->totalNumPages){
        THROW(RC_READ_NON_EXISTING_PAGE,"The page do not exist (Exceeding Total Page Number)");
    }
    if (pageNum < 0){
        THROW(RC_READ_NON_EXISTING_PAGE,"The page do not exist (Negative Page)");
    }
    fseek(fHandle->mgmtInfo.posixFileDescriptor, ACCESSIBLE_PAGE_OFFSET + pageNum * PAGE_SIZE, SEEK_SET);
    fHandle->curPagePos = pageNum;
    fread(memPage, PAGE_SIZE, 1, fHandle->mgmtInfo.posixFileDescriptor);
    return RC_OK;
}

int getBlockPos (SM_FileHandle *fHandle){
    return fHandle->curPagePos;
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(0,fHandle,memPage);
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (fHandle == NULL){
        THROW(RC_FILE_HANDLE_NOT_INIT,"fHandle is NULL");
    }
    return readBlock(fHandle->totalNumPages -1, fHandle, memPage);
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (fHandle == NULL){
        THROW(RC_FILE_HANDLE_NOT_INIT,"fHandle is NULL");
    }
    return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (fHandle == NULL){
        THROW(RC_FILE_HANDLE_NOT_INIT,"fHandle is NULL");
    }
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}   

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (fHandle == NULL){
        THROW(RC_FILE_HANDLE_NOT_INIT,"fHandle is NULL");
    }
    return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

/* writing blocks to a page file */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (fHandle == NULL){
        THROW(RC_FILE_HANDLE_NOT_INIT,"fHandle is NULL");
    }
    if (pageNum >= fHandle->totalNumPages || pageNum < 0){
        THROW(RC_WRITE_FAILED,"The page do not exist");
    }
    fseek(fHandle->mgmtInfo.posixFileDescriptor, ACCESSIBLE_PAGE_OFFSET + pageNum * PAGE_SIZE, SEEK_SET);
    fHandle->curPagePos = pageNum;
    fwrite(memPage, PAGE_SIZE, 1, fHandle->mgmtInfo.posixFileDescriptor);
    return RC_OK;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    if (fHandle == NULL){
        THROW(RC_FILE_HANDLE_NOT_INIT,"fHandle is NULL");
    }
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

RC appendEmptyBlock (SM_FileHandle *fHandle){
    if (fHandle == NULL){
        THROW(RC_FILE_HANDLE_NOT_INIT,"fHandle is NULL");
    }
    /* First we add a page to the file */
    fseek(fHandle->mgmtInfo.posixFileDescriptor,0,SEEK_END);
    int i;
    for (i = 0; i < PAGE_SIZE; i++){
        char empty = '\0';
        if (fwrite(&empty,sizeof(char),1,fHandle->mgmtInfo.posixFileDescriptor) != sizeof(char)){
            break;
        }
    }
    if (i < PAGE_SIZE){ // We failed to append an entire block and clean up the partial block
        ftruncate(fileno(fHandle->mgmtInfo.posixFileDescriptor), ftell(fHandle->mgmtInfo.posixFileDescriptor) - i);
        THROW(RC_WRITE_FAILED, "Append Failed");
    }
    updateTotalPageNumber(fHandle->totalNumPages + 1, fHandle);
    return RC_OK;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
    if (fHandle == NULL){
        THROW(RC_FILE_HANDLE_NOT_INIT,"fHandle is NULL");
    }
    if (numberOfPages <= fHandle->totalNumPages){
        return RC_OK;
    }
    for (int i = 0; i < numberOfPages - fHandle->totalNumPages; i++){
        if (appendEmptyBlock(fHandle) == RC_WRITE_FAILED){
            updateTotalPageNumber(fHandle->totalNumPages + i, fHandle);
            THROW(RC_WRITE_FAILED, "ensureCapacity Failed");
        }
    }
    updateTotalPageNumber(numberOfPages, fHandle);
    return RC_OK;
}