/*
 * buffer_mgr.c
 */

#include "stdio.h"
#include "stdlib.h"
#include "string.h"

#include "buffer_mgr.h"
#include "storage_mgr.h"

/*Structure for PageFrame inside BufferPool*/
typedef struct PageFrame
{
	int frameNum;		//number associated with each Page frame
	int pageNum;		//Page Number of the Page present in the Page frame
	int dirtyFlag;		//Dirty flag to determine whether page was modified/write
	int fixCount;		//Fix count to mark whether the page is in use by other users
	int refBit;			//Reference bit used in CLOCK Algorithm to mark the page which is referred
	char *data;			//Actual data present in the page
	struct pageFrame *next, *prev;	//Nodes of the Doubly linked List where each node is a frame, pointing to other frames
}PageFrame;

/*Structure for Buffer Pool to store Management Information*/
typedef struct BM_BufferPool_Mgmt
{
	int occupiedCount;		//to keep count of number of frames occupied inside the pool
	void *stratData;		//to pass parameters for page replacement strategies
	PageFrame *head,*tail,*start;	//keep track of nodes in linked list
	PageNumber *frameContent;	//an array of page numbers to store the statistics of number of pages stored in the page frame
	int *fixCount;				//an array of integers to store the statistics of fix counts for a page
	bool *dirtyBit;				//an array of bool's to store the statistics of dirty bits for modified page
	int numRead;				//to give total number of pages read from the buffer pool
	int numWrite;				//to give total number of pages wrote into the buffer pool
}BM_BufferPool_Mgmt;


/*This function is used to create a Buffer Pool with specified number of Page Frames
 i.e linked list of frames with some default values, with the first frame acting as the head node
 while the last acting as the tail node.
 This function is called by the initBufferPool() function passing mgmt info as the parameter*/
void createPageFrame(BM_BufferPool_Mgmt *mgmt)
{
	//Create a frame and assign a memory to it
	PageFrame *frame = (PageFrame *) malloc(sizeof(PageFrame));

	//intialise the page properties of the frames i.e each frame has a page within,
	//so properties (default page values) are applied to the frames itself
	frame->dirtyFlag = 0;	//FALSE
	frame->fixCount = 0;
	frame->frameNum = 0;
	frame->pageNum = -1;
	frame->refBit = 0;

	//allocate memory for page to stored into the pageFrame
	frame->data = calloc(PAGE_SIZE,sizeof(char*));

	//initialise the pointers
	mgmt->head = mgmt->start;

	//if it is the 1st frame make it the HEAD node of Linked List
	if(mgmt->head == NULL)
	{
		mgmt->head = frame;
		mgmt->tail = frame;
		mgmt->start = frame;
	}
	else		//if other than 1st node, appened the nodes to the HEAD node, and make the link between these nodes
	{
		mgmt->tail->next = frame;
		frame->prev = mgmt->tail;
		mgmt->tail = mgmt->tail->next;
	}

	//initialise the other pointers of the linked list
	mgmt->tail->next = mgmt->head;
	mgmt->head->prev = mgmt->tail;
}

// Buffer Manager Interface Pool Handling

/*
 * This function is used to create a Buffer Pool for an existing page file
 * bm -> used to store the mgmtData
 * pageFileName -> name of the page file, whose pages are to be cached
 * numPages -> number of frames in the buffer Pool
 * strategy -> Page Replacement Strategy to be used
 * stratData -> parameters if required for any page replacement strategy
 */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy,void *stratData)
{
	//Memory allocation to store the Buffer Pool Management Data
	BM_BufferPool_Mgmt *bp_mgmt = (BM_BufferPool_Mgmt*)malloc(sizeof(BM_BufferPool_Mgmt));

	bp_mgmt->start = NULL;
	//Storage manager file handle
	SM_FileHandle fHandle;

	int i;

	//open the page file, whose pages are to be cached
	openPageFile((char*) pageFileName,&fHandle);

	//create the frames for buffer pool
	for(i=0;i<numPages;i++)
	{
		createPageFrame(bp_mgmt);
	}

	//initialize the values and store it in management data
	bp_mgmt->tail = bp_mgmt->head;
	bp_mgmt->stratData = stratData;
	bp_mgmt->occupiedCount = 0;
	bp_mgmt->numRead = 0;
	bp_mgmt->numWrite = 0;
	bm->numPages = numPages;
	bm->pageFile = (char*) pageFileName;
	bm->strategy = strategy;
	bm->mgmtData = bp_mgmt;

	//close the page file
	closePageFile(&fHandle);

	return RC_OK;
}

/*
 * This function is used to destroy the buffer pool (bm)
 * All the resources that are allocated i.e all memory allocation are free'd to avoid memory leaks
 * All the dirtyPages are written back, before destroying
 */
RC shutdownBufferPool(BM_BufferPool *const bm)
{
	//load the mgmt data of the buffer pool
	BM_BufferPool_Mgmt *bp_mgmt;
	bp_mgmt = bm->mgmtData;

	//point to the head node
	PageFrame *Frame = bp_mgmt->head;

	//calling forceFlush which writes all the dirtyPages back again, before destroying
	forceFlushPool(bm);

	//free all the page data in the frame
	do
	{
		free(Frame->data);
		Frame= Frame->next;
	}while(Frame!=bp_mgmt->head);

	//make the values NULL
	bp_mgmt->start = NULL;
	bp_mgmt->head = NULL;
	bp_mgmt->tail = NULL;

	//free the entire frame
	free(Frame);

	//free the bufferPool
	free(bp_mgmt);

	//set all the values to 0 or NULL
	bm->numPages = 0;
	bm->mgmtData = NULL;
	bm->pageFile = NULL;

	return RC_OK;
}

/*
 * This function is used to write all the pages to the disk whose dirtyBit is set
 */
RC forceFlushPool(BM_BufferPool *const bm)
{
	//load the mgmt Data
	BM_BufferPool_Mgmt *bp_mgmt;
	bp_mgmt = bm->mgmtData;

	//point to the head node
	PageFrame *Frame = bp_mgmt->head;

	SM_FileHandle fh;

	//open the page file
	if (openPageFile ((char *)(bm->pageFile), &fh) != RC_OK)
	{
		return RC_FILE_NOT_FOUND;
	}

	//check if dirtyFlag is set and fix count is 0,
	//if YES, then write the pages to disk
	do
	{
		if(Frame->dirtyFlag == 1 && Frame->fixCount == 0)
		{
			//write the pages to disk
			if(writeBlock(Frame->pageNum, &fh, Frame->data) != RC_OK)
			{
				closePageFile(&fh);
				return RC_WRITE_FAILED;
			}
			Frame->dirtyFlag = 0;	//mark the dirtyFlag 0
			bp_mgmt->numWrite++;
		}
		Frame = Frame->next;	//move to next frame
	}while(Frame != bp_mgmt->head);

	//close the page file
	closePageFile(&fh);
	return RC_OK;
}

// Buffer Manager Interface Access Pages

/*
 * This function is used to mark the page as dirty
 * The page of BM_PageHandle's is marked with dirty bit set to 1
 */
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	BM_BufferPool_Mgmt *bp_mgmt;
	bp_mgmt = bm->mgmtData;
	PageFrame *frame = bp_mgmt->head;

	do
	{
		//check if the pageNum is same as the page to be marked dirty
		if(page->pageNum == frame->pageNum)
		{
			//mark it's dirty bit
			frame->dirtyFlag = 1;
			return RC_OK;
		}
		frame=frame->next;
	}while(frame!=bp_mgmt->head);

	return RC_OK;
}

/*
 * This function is used to unpinpage
 * After the user/client has done with reading of page it is set free, i.e its fixcount is decremented
 * using unpinpage called as "UNPINNING"
 */
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	BM_BufferPool_Mgmt *bp_mgmt;
	bp_mgmt = bm->mgmtData;
	PageFrame *frame = bp_mgmt->head;

	do
	{
		if(page->pageNum == frame->pageNum)
		{
			//decrement fix count
			frame->fixCount--;
			return RC_OK;
		}
		frame = frame->next;
	}while(frame!= bp_mgmt->head);

	return RC_OK;
}

/*
 * This function writes the page passed to it in BM_PageHandle back to the disk
 */
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
	BM_BufferPool_Mgmt *bp_mgmt;
	bp_mgmt = bm->mgmtData;

	PageFrame *Frame = bp_mgmt->head;
	SM_FileHandle fh;


	//Open PageFile for write operation
	if (openPageFile ((char *)(bm->pageFile), &fh) != RC_OK)
	{
		return RC_FILE_NOT_FOUND;
	}

	//find the page that needs to be written back & check if its dirty flag is 1
	do
	{
		if(Frame->pageNum == page->pageNum && Frame->dirtyFlag == 1)
		{
			if(writeBlock(Frame->pageNum, &fh, Frame->data) != RC_OK)
			{
				closePageFile(&fh);
				return RC_WRITE_FAILED;
			}
			bp_mgmt->numWrite++;	//increment the num of writes performed
			Frame->dirtyFlag = 0;	//unmark its dirty bit
			break;
		}
		Frame= Frame->next;
	}while(Frame!=bp_mgmt->head);

	closePageFile(&fh);
	return RC_OK;
}

/*
 * This function is used to put a page onto the bufferPool
 * Each page is put in a pageFrame, which is put onto the bufferPool
 * This method is called as "PINNING" a page.
 */
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
	// Choose the appropriate Page Replacement Strategy
	// We have implemented FIFO, LRU and CLOCK strategy

	switch(bm->strategy)
	{
	case RS_FIFO:
		pinPageFIFO(bm, page, pageNum);
		break;

	case RS_LRU:
		pinPageLRU(bm,page,pageNum);
		break;

	case RS_CLOCK:
		pinPageCLOCK(bm,page,pageNum);
		break;
	}
	return RC_OK;
}

// FIFO Page Replacement Strategy Implementation
/*
 * This function pinPageFIFO is implementation of First In First Out (FIFO)
 * Here, we have implemented Queue implementation for FIFO
 */
RC pinPageFIFO(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
	SM_FileHandle fh;
	BM_BufferPool_Mgmt *bp_mgmt = bm->mgmtData;
	PageFrame *frame = bp_mgmt->head;

	openPageFile((char*) bm->pageFile,&fh);

	// if page is already present in the buffer pool
	do
	{
		//put the data onto the page and increment the fix count
		if(frame->pageNum == pageNum)
		{
			page->pageNum = pageNum;
			page->data = frame->data;

			frame ->pageNum = pageNum;
			frame->fixCount++;
			return RC_OK;
		}
		frame = frame->next;
	}while(frame!= bp_mgmt->head);

	//if there are remaining frames in the buffer pool, i.e. bufferpool is not fully occupied
	//pin the pages in the empty spaces
	if(bp_mgmt->occupiedCount < bm->numPages)
	{
		frame = bp_mgmt->head;
		frame->pageNum = pageNum;

		//move the header to next empty space
		if(frame->next != bp_mgmt->head)
		{
			bp_mgmt->head = frame->next;
		}
		frame->fixCount++;
		bp_mgmt->occupiedCount++;	//increment the occupied count
	}
	else		//use page replacement strategy FIFO
	{
		//replace pages from frame
		frame = bp_mgmt->tail;
		do
		{
			//check if the page is in use, i.e. fixcount > 0
			//goto next frame whose fix count = 0, and replace the page
			if(frame->fixCount != 0)
			{
				frame = frame->next;
			}
			else
			{
				//before replacing check for dirtyflag if dirty write back to disk and then replace
				if(frame->dirtyFlag == 1)
				{
					ensureCapacity(frame->pageNum, &fh);
					if(writeBlock(frame->pageNum,&fh, frame->data)!=RC_OK)
					{
						closePageFile(&fh);
						return RC_WRITE_FAILED;
					}
					bp_mgmt->numWrite++;
				}

				//update the frame and bufferPool attributes
				frame->pageNum = pageNum;
				frame->fixCount++;
				bp_mgmt->tail = frame->next;
				bp_mgmt->head = frame;

				break;
			}
		}while(frame!= bp_mgmt->head);
	}

	//ensure if the pageFile has the required number of pages, if not create those
	ensureCapacity((pageNum+1),&fh);

	//read the block into pageFrame
	if(readBlock(pageNum, &fh,frame->data)!=RC_OK)
	{
		closePageFile(&fh);
		return RC_READ_NON_EXISTING_PAGE;
	}

	//increment the num of read operations
	bp_mgmt->numRead++;

	//update the attributes and put the data onto the page
	page->pageNum = pageNum;
	page->data = frame->data;

	//close the pageFile
	closePageFile(&fh);

	return RC_OK;
}

// LRU Page Replacement Strategy Implementations
/*
 * This method implements LRU page replacement strategy
 * i.e. the page which is Least Recently Used will be replaced, with the new page
 */
RC pinPageLRU(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
	BM_BufferPool_Mgmt *bp_mgmt = bm->mgmtData;
	PageFrame *frame = bp_mgmt->head;
	SM_FileHandle fh;

	openPageFile((char*)bm->pageFile,&fh);

	//check if frame already in buffer pool
	do
	{
		if(frame->pageNum == pageNum)
		{
			//update the page and frame attributes
			page->pageNum = pageNum;
			page->data = frame->data;

			frame->pageNum = pageNum;
			frame->fixCount++;

			//point the head and tail for replacement
			bp_mgmt->tail = bp_mgmt->head->next;
			bp_mgmt->head = frame;
			return RC_OK;
		}

		frame = frame->next;

	}while(frame!= bp_mgmt->head);

	//if there are empty spaces in the bufferPool , then fill in those frames first
	if(bp_mgmt->occupiedCount < bm->numPages)
	{

		frame = bp_mgmt->head;
		frame->pageNum = pageNum;

		if(frame->next != bp_mgmt->head)
		{
			bp_mgmt->head = frame->next;
		}
		frame->fixCount++;		//increment the fix count
		bp_mgmt->occupiedCount++;	//increment the occupied Count
	}
	else
	{
		//replace pages from frame using LRU
		frame = bp_mgmt->tail;
		do
		{
			//check if page in use, move onto next page to be replaced
			if(frame->fixCount != 0)
			{
				frame = frame->next;
			}
			else
			{
				//before replacing check if dirty flag set, write back content onto the disk
				if(frame->dirtyFlag == 1)
				{
					ensureCapacity(frame->pageNum, &fh);
					if(writeBlock(frame->pageNum,&fh, frame->data)!=RC_OK)
					{
						closePageFile(&fh);
						return RC_WRITE_FAILED;
					}
					bp_mgmt->numWrite++;	//increment number of writes performed
				}

				//find the least recently used page and replace that page
				if(bp_mgmt->tail != bp_mgmt->head)
				{
					frame->pageNum = pageNum;
					frame->fixCount++;
					bp_mgmt->tail = frame;
					//bp_mgmt->head = frame;
					bp_mgmt->tail = frame->next;
					break;
				}
				else
				{
					frame = frame->next;
					frame->pageNum = pageNum;
					frame->fixCount++;
					bp_mgmt->tail = frame;
					bp_mgmt->head = frame;
					bp_mgmt->tail = frame->prev;
					break;
				}
			}
		}while(frame!= bp_mgmt->tail);
	}

	ensureCapacity((pageNum+1),&fh);
	if(readBlock(pageNum, &fh,frame->data)!=RC_OK)
	{
		return RC_READ_NON_EXISTING_PAGE;
	}
	bp_mgmt->numRead++;

	//update the page frame and its data
	page->pageNum = pageNum;
	page->data = frame->data;

	//close the pagefile
	closePageFile(&fh);

	return RC_OK;
}


// CLOCK Page Replacement Strategy Implementations
/*
 * This method implements Clock Page Replacement Strategy
 * it is more optimized version of FIFO, implements a circular queue
 * with a reference bit set for each page in pageFrame
 */
RC pinPageCLOCK(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
	SM_FileHandle fh;
	BM_BufferPool_Mgmt *bp_mgmt = bm->mgmtData;
	PageFrame *frame = bp_mgmt->head;
	PageFrame *temp;
	openPageFile((char*)bm->pageFile,&fh);

	// if frame already in buffer pool

	do
	{
		//if same page
		if(frame->pageNum == pageNum)
		{
			page->pageNum = pageNum;
			page->data = frame->data;

			frame->refBit = 1;	//mark its reference bit as 1, as it is referred again
			frame ->pageNum = pageNum;
			frame->fixCount++;

			return RC_OK;
		}
		frame = frame->next;
	}while(frame!=bp_mgmt->head);

	//if space present will be executed at the start when all the frames are empty
	if(bp_mgmt->occupiedCount < bm->numPages)
	{
		frame = bp_mgmt->head;

		frame->pageNum = pageNum;
		frame->refBit = 1;	//and mark their reference bit as 1

		//all the insertions will be made at the Head,
		//every time the insert takes place head is moved to that node
		if(frame->next != bp_mgmt->head)
		{
			bp_mgmt->head = frame->next;
		}

		frame->fixCount++;
		bp_mgmt->occupiedCount++;

	}
	else
	{
		//if replacement reqd
		frame = bp_mgmt->head;

		do
		{
			//find a page whose reference bit is 0, to be replaced,
			//and along its way make other reference bit which are 1 to 0

			if(frame->fixCount !=0)
			{
				frame = frame->next;
			}
			else
			{
				while(frame->refBit!=0)
				{
					frame->refBit = 0;
					frame= frame->next;
				}

				//if reference bit is 0 replace the page
				if(frame-> refBit == 0)
				{
					//check the page that is replaced,
					//if its dirty bit is set, then writeBlock and then replace
					if(frame->dirtyFlag == 1)
					{
						ensureCapacity(frame->pageNum, &fh);
						if(writeBlock(frame->pageNum,&fh, frame->data)!=RC_OK)
						{
							closePageFile(&fh);
							return RC_WRITE_FAILED;
						}
						bp_mgmt->numWrite++;
					}
					//update all the frame & page attributes
					frame->refBit = 1;
					frame->pageNum = pageNum;
					frame->fixCount++;
					bp_mgmt->head = frame->next;
					break;
				}
			}

		}while(frame!=bp_mgmt->head);
	}
	ensureCapacity((pageNum+1),&fh);

	if(readBlock(pageNum, &fh,frame->data)!=RC_OK)
	{
		closePageFile(&fh);
		return RC_READ_NON_EXISTING_PAGE;
	}

	bp_mgmt->numRead++;
	page->pageNum = pageNum;
	page->data = frame->data;


	closePageFile(&fh);

	return RC_OK;
}

// Statistics Interface
/*
 * The getFrameContents function returns an array of PageNumbers (of size numPages)
 * where the ith element is the number of the page stored in the ith page frame.
 * An empty page frame is represented using the constant NO_PAGE.
 */
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
	BM_BufferPool_Mgmt *bp_mgmt;
	bp_mgmt = bm->mgmtData;
	bp_mgmt->frameContent = (PageNumber*)malloc(sizeof(PageNumber)*bm->numPages);

	PageFrame *frame = bp_mgmt->start;
	PageNumber* frameContents = bp_mgmt->frameContent;

	int i;
	int page_count = bm->numPages;

	if(frameContents != NULL)
	{
		for(i=0;i< page_count;i++)
		{
			frameContents[i] = frame->pageNum;
			frame = frame->next;
		}
	}
	//free(bp_mgmt->frameContent);

	return frameContents;
}

/*
 * The getDirtyFlags function returns an array of bools (of size numPages)
 * where the ith element is TRUE if the page stored in the ith page frame is dirty.
 * Empty page frames are considered as clean.
 */
bool *getDirtyFlags (BM_BufferPool *const bm)
{
	BM_BufferPool_Mgmt *bp_mgmt;
	bp_mgmt = bm->mgmtData;
	bp_mgmt->dirtyBit = (bool*)malloc(sizeof(bool)*bm->numPages);

	PageFrame *frame = bp_mgmt->start;
	bool* dirtyBit = bp_mgmt->dirtyBit;

	int i,page_count = bm->numPages;

	if(dirtyBit != NULL)
	{
		for(i=0;i< page_count;i++)
		{
			dirtyBit[i] = frame->dirtyFlag;
			frame = frame->next;
		}
	}
	//free(bp_mgmt->dirtyBit);

	return dirtyBit;
}

/*
 * The getFixCounts function returns an array of ints (of size numPages)
 * where the ith element is the fix count of the page stored in the ith page frame.
 * Return 0 for empty page frames.
 */
int *getFixCounts (BM_BufferPool *const bm)
{
	BM_BufferPool_Mgmt *bp_mgmt;
	bp_mgmt = bm->mgmtData;
	bp_mgmt->fixCount = (int*)malloc(sizeof(int)*bm->numPages);

	PageFrame *frame = bp_mgmt->start;
	int* fixCount = bp_mgmt->fixCount;

	int i,page_count = bm->numPages;

	if(fixCount != NULL)
	{
		for(i=0;i< page_count;i++)
		{
			fixCount[i] = frame->fixCount;
			frame = frame->next;
		}
	}
	//free(bp_mgmt->fixCount);

	return  fixCount;
}

/*
 * This function gets the total number of ReadBlock operations performed
 */
int getNumReadIO (BM_BufferPool *const bm)
{
	return ((BM_BufferPool_Mgmt*)bm->mgmtData)->numRead;
}

/*
 * This function gets the total number of writeBlock operations performed
 */
int getNumWriteIO (BM_BufferPool *const bm)
{
	return ((BM_BufferPool_Mgmt*)bm->mgmtData)->numWrite;
}
