/*
 * btree_mgr.c
 */

#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "btree_mgr.h"
#include "record_mgr.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "tables.h"


//Structure for BTree Representation
typedef struct BTree
{
	struct Value value;
	struct RID rid;
	int numOfKeys;
	BM_BufferPool *bm;
	BM_PageHandle *ph;
	int maxNumOfKeysPerNode;
	int nodeCounter;
}BTree;


BTree **AllocBTree;


//Store the Number of Keys allocated in B+Tree
int numOfKeys;
int scanNextEntry;

// init and shutdown index manager
/*
 * This is function is used to Initialize Index Manager
 */
RC initIndexManager (void *mgmtData)
{
	return RC_OK;
}

/*
 * This is function is used to shut down the index manager,
 * also free's the memory allocated during the process
 */

RC shutdownIndexManager()
{
	int i;

	//for all keys inserted
	for(i = 0;i<numOfKeys;i++)
	{
		free(AllocBTree[numOfKeys]);
	}
	return RC_OK;
}

// create, destroy, open, and close an btree index

/*
 * This function is used to Create A B+ Tree
 * all the attributes related to the tree are initialized
 */
RC createBtree (char *idxId, DataType keyType, int n)
{
	SM_FileHandle fh;
	SM_PageHandle ph = malloc(PAGE_SIZE*sizeof(char));

	//Create a B-tree, using page file
	createPageFile(idxId);

	openPageFile(idxId,&fh);

	//confirm the number of pages present
	ensureCapacity(1,&fh);

	//Allocate Memory for the Btree
	AllocBTree = (BTree**)malloc(sizeof(BTree*));

	//Update all the BTree Handle attributes
	BTreeHandle *btHandle = (BTreeHandle*)malloc(sizeof(BTreeHandle*));
	btHandle->idxId = idxId;
	btHandle->keyType = keyType;
	btHandle->mgmtData = n;

	//this page will have the number of keys that can be inserted into a single Node
	*((int*)ph)=n;

	//write the value of N to the page
	writeCurrentBlock(&fh,ph);

	closePageFile(&fh);

	//Initialize the Num of Keys to 0
	numOfKeys = 0;
	scanNextEntry = 0;

	return RC_OK;
}

/*
 * This function is used to open the B-Tree alread created above,
 * it read the value from the page file regarding the "N"
 * and stores in the BTREE structure created attributes
 */
RC openBtree (BTreeHandle **tree, char *idxId)
{
	SM_FileHandle fh;

	//Open a PageFile
	openPageFile(idxId,&fh);

	//Create a Btree Handler
	*tree = (BTreeHandle*)malloc(sizeof(BTreeHandle));

	//(*tree)->idxId =(char*)malloc(sizeof(char));
	(*tree)->idxId = idxId;

	//Create a Tree Information Node
	BTree *treeInfo = (BTree*)malloc(sizeof(BTree));

	//Make Buffer Pool & Page Handle to access the pages
	treeInfo->bm = MAKE_POOL();
	treeInfo->ph = MAKE_PAGE_HANDLE();

	//initialize the Buffer Pool
	initBufferPool(treeInfo->bm,idxId,6,RS_FIFO,NULL);

	//Pin the page to be accessed to get data
	pinPage(treeInfo->bm,treeInfo->ph,1);

	//store the page data i.e. N value
	treeInfo->maxNumOfKeysPerNode = *((int*)treeInfo->ph->data);

	//Node Counter to count number of Nodes
	treeInfo->nodeCounter=0;

	//store the entire data into the management data for the Tree Handle
	BTreeHandle *temp = (BTreeHandle*)malloc(sizeof(BTreeHandle));
	temp->mgmtData = treeInfo;
	*tree = temp;

	//unpin the page after read operation is performed
	unpinPage(treeInfo->bm,treeInfo->ph);

	//close the page file
	closePageFile(&fh);

	return RC_OK;
}

/*
 * Used to close the B-Tree
 */
RC closeBtree (BTreeHandle *tree)
{
	//free the memory allocated for the tree
	free(tree);
	return RC_OK;
}

/*
 * This function is used to delete the tree
 * i.e. destroy the page file created and free all memory
 */
RC deleteBtree (char *idxId)
{
	destroyPageFile(idxId);
	numOfKeys = 0;
	scanNextEntry = 0;
	return RC_OK;
}

// access information about a b-tree
/*
 * Get the total Number of Nodes in the B+Tree formed
 */
RC getNumNodes (BTreeHandle *tree, int *result)
{
	//if the node is on the same page i.e. thier RID's are same, then they are on same node
	//so total number of keys inserted MINUS same nodes will give us the final result

	int headNode, compareNode, samePageEntry;

	for(headNode=1;headNode<numOfKeys;headNode++)
	{
		for(compareNode=headNode-1;compareNode>=0;compareNode--)
		{
			if(AllocBTree[headNode]->rid.page==AllocBTree[compareNode]->rid.page)
			{
				samePageEntry++;
			}
		}
	}

	//calculate the NumOfNodes
	int numOfNodes = numOfKeys - samePageEntry;
	*result = numOfNodes;

	return RC_OK;
}

/*
 * Total number of entries in the B+ -Tree i.e. the Number of Keys
 */
RC getNumEntries (BTreeHandle *tree, int *result)
{
	//as we have stored the values for every insert we can utilize that directly here
	*result = numOfKeys;
	return RC_OK;
}

/*
 * Gets the type of Key in the tree inserted and stores it in the result
 */
RC getKeyType (BTreeHandle *tree, DataType *result)
{
	int i;

	for(i=0;i<numOfKeys;i++)
	{
		result[i] = tree->keyType;
	}
	return RC_OK;
}

// index access
/*
 * This method is used to search for a key in the Tree
 * Implemented for INT, FLOAT, STRING type
 */
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{

	int i=0;
	int flagFound=0;

	for(i = 0;i<numOfKeys;i++)
	{

		switch(key->dt)
		{
		case DT_INT:
			if(AllocBTree[i]->value.dt==key->dt && AllocBTree[i]->value.v.intV==key->v.intV)
				flagFound=1;
			break;
		case DT_FLOAT:
			if(AllocBTree[i]->value.dt==key->dt && AllocBTree[i]->value.v.floatV==key->v.floatV)
				flagFound=1;
			break;
		case DT_STRING:
			if(AllocBTree[i]->value.dt==key->dt && AllocBTree[i]->value.v.stringV==key->v.stringV)
				flagFound=1;
			break;
		default:
			return RC_RM_NO_PRINT_FOR_DATATYPE;
		}

		if(flagFound)	//if key found
		{
			result->page = AllocBTree[i]->rid.page;
			result->slot = AllocBTree[i]->rid.slot;
			return RC_OK;
		}

	}
	if(i==numOfKeys)
	{
		return RC_IM_KEY_NOT_FOUND;
	}
}

/*
 * This function is used to insert Keys into the B+ Tree
 * We check if it is the first Node to be inserted,
 * if yes we insert it and increment the num of Keys
 * We also check whether the Key already exists,
 * if yes we return already exists
 * Mark the Node full when the node has "N" keys
 */
RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
	//allocate memory in the node for the Key to be inserted
	BTree *treeInfo = (BTree*)(tree->mgmtData);
	AllocBTree[numOfKeys] = (BTree*)malloc(sizeof(BTree));

	//flag to check whether Key already exists
	int flagFound=0;

	//i.e. first key to be inserted
	if(numOfKeys==0)
	{
		pinPage(treeInfo->bm,treeInfo->ph,numOfKeys);
		markDirty(treeInfo->bm,treeInfo->ph);
		treeInfo->ph->data = "NotFull";

		switch(key->dt)
		{

		case DT_INT:
			AllocBTree[numOfKeys]->value.dt = key->dt;
			AllocBTree[numOfKeys]->value.v.intV = key->v.intV;
			break;

		case DT_FLOAT:
			AllocBTree[numOfKeys]->value.dt = key->dt;
			AllocBTree[numOfKeys]->value.v.floatV = key->v.floatV;
			break;

		case DT_STRING:
			AllocBTree[numOfKeys]->value.dt = key->dt;
			AllocBTree[numOfKeys]->value.v.intV = key->v.intV;
			break;
		default:
			return RC_RM_NO_PRINT_FOR_DATATYPE;
		}
		AllocBTree[numOfKeys]->rid.page = rid.page;
		AllocBTree[numOfKeys]->rid.slot = rid.slot;

		numOfKeys++;
		unpinPage(treeInfo->bm,treeInfo->ph);

		return RC_OK;

	}
	else
	{
		//find if the Key already Exists
		int i=0;
		for(i=0;i<numOfKeys;i++)
		{
			switch(key->dt)
			{

			case DT_INT:
				if(AllocBTree[i]->value.dt==key->dt && AllocBTree[i]->value.v.intV==key->v.intV)
					flagFound=1;
				break;
			case DT_FLOAT:
				if(AllocBTree[i]->value.dt==key->dt && AllocBTree[i]->value.v.floatV==key->v.floatV)
					flagFound=1;
				break;
			case DT_STRING:
				if(AllocBTree[i]->value.dt==key->dt && AllocBTree[i]->value.v.stringV==key->v.stringV)
					flagFound=1;
				break;
			default:
				return RC_RM_NO_PRINT_FOR_DATATYPE;
			}
		}

		if(flagFound)	//key already exists
		{
			return RC_IM_KEY_ALREADY_EXISTS;
		}
		else	//insert the new Key
		{
			pinPage(treeInfo->bm,treeInfo->ph,treeInfo->nodeCounter);
			markDirty(treeInfo->bm,treeInfo->ph);

			if(!(strcmp(treeInfo->ph->data,"NodeFull")))	//if Node is Full //insert into other page
			{
				treeInfo->nodeCounter++;
				unpinPage(treeInfo->bm,treeInfo->ph);
				pinPage(treeInfo->bm,treeInfo->ph,treeInfo->nodeCounter);

				switch(key->dt)
				{
				case DT_INT:
					AllocBTree[numOfKeys]->value.dt = key->dt;
					AllocBTree[numOfKeys]->value.v.intV = key->v.intV;
					break;

				case DT_FLOAT:
					AllocBTree[numOfKeys]->value.dt = key->dt;
					AllocBTree[numOfKeys]->value.v.floatV = key->v.floatV;
					break;

				case DT_STRING:
					AllocBTree[numOfKeys]->value.dt = key->dt;
					AllocBTree[numOfKeys]->value.v.intV = key->v.intV;
					break;

				default:
					return RC_RM_NO_PRINT_FOR_DATATYPE;
				}

				//set the Rid & Value
				AllocBTree[numOfKeys]->rid.page = rid.page;
				AllocBTree[numOfKeys]->rid.slot = rid.slot;

				//increment the Keys and mark the new node as not full
				treeInfo->ph->data="NotFull";
				numOfKeys++;
				unpinPage(treeInfo->bm,treeInfo->ph);
			}
			else
			{
				switch(key->dt)
				{
				case DT_INT:
					AllocBTree[numOfKeys]->value.dt = key->dt;
					AllocBTree[numOfKeys]->value.v.intV = key->v.intV;
					break;

				case DT_FLOAT:
					AllocBTree[numOfKeys]->value.dt = key->dt;
					AllocBTree[numOfKeys]->value.v.floatV = key->v.floatV;
					break;

				case DT_STRING:
					AllocBTree[numOfKeys]->value.dt = key->dt;
					AllocBTree[numOfKeys]->value.v.intV = key->v.intV;
					break;

				default:
					return RC_RM_NO_PRINT_FOR_DATATYPE;
				}

				//set the Rid & Value
				AllocBTree[numOfKeys]->rid.page = rid.page;
				AllocBTree[numOfKeys]->rid.slot = rid.slot;

				//increment the Keys and Make the Node Full as N values are inserted
				treeInfo->ph->data="NodeFull";
				numOfKeys++;
				unpinPage(treeInfo->bm,treeInfo->ph);

			}
			return RC_OK;
		}
	}
	return RC_OK;
}

/*
 * This function is used to Delete a Key from the Tree
 * Marks the node as Not full after deletion of Key from the Node
 */
RC deleteKey (BTreeHandle *tree, Value *key)
{
	//printf("Deleting B-Tree\n");

	BTree* treeInfo = (BTree*)(tree->mgmtData);

	int i=0;
	int flagFound=0;
	int deletedKeyIndex, nextKeyIndex;
	for(i = 0;i<numOfKeys;i++)
	{

		switch(key->dt)
		{
		case DT_INT:
			if(AllocBTree[i]->value.dt==key->dt && AllocBTree[i]->value.v.intV==key->v.intV)
				flagFound=1;
			break;
		case DT_FLOAT:
			if(AllocBTree[i]->value.dt==key->dt && AllocBTree[i]->value.v.floatV==key->v.floatV)
				flagFound=1;
			break;
		case DT_STRING:
			if(AllocBTree[i]->value.dt==key->dt && AllocBTree[i]->value.v.stringV==key->v.stringV)
				flagFound=1;
			break;
		default:
			return RC_RM_NO_PRINT_FOR_DATATYPE;
		}

		if(flagFound)	//if Key found
		{
			pinPage(treeInfo->bm,treeInfo->ph,(i/2));	//find the node page
			treeInfo->ph->data = "NotFull";				//mark it not full node

			markDirty(treeInfo->bm,treeInfo->ph);
			unpinPage(treeInfo->bm,treeInfo->ph);

			deletedKeyIndex=i;
			nextKeyIndex = deletedKeyIndex+1;
			//move the Keys and Nodes after deletion is done
			int k;
			for(k=deletedKeyIndex;k<numOfKeys && nextKeyIndex<numOfKeys;k++)
			{
				switch(AllocBTree[nextKeyIndex]->value.dt)
				{
				case DT_INT:
					AllocBTree[k]->value.dt = AllocBTree[nextKeyIndex]->value.dt;
					AllocBTree[k]->value.v.intV = AllocBTree[nextKeyIndex]->value.v.intV;
					break;
				case DT_FLOAT:
					AllocBTree[k]->value.dt = AllocBTree[nextKeyIndex]->value.dt;
					AllocBTree[k]->value.v.floatV = AllocBTree[nextKeyIndex]->value.v.floatV;
					break;
				case DT_STRING:
					AllocBTree[k]->value.dt = AllocBTree[nextKeyIndex]->value.dt;
					strcpy(AllocBTree[k]->value.v.stringV, AllocBTree[nextKeyIndex]->value.v.stringV);
					break;
				}

				AllocBTree[k]->rid.page = AllocBTree[nextKeyIndex]->rid.page;
				AllocBTree[k]->rid.slot = AllocBTree[nextKeyIndex]->rid.slot;
				nextKeyIndex++;
			}
			numOfKeys--;
			free(AllocBTree[k]);
			return RC_OK;
		}

		if(i == numOfKeys)
			return RC_IM_KEY_NOT_FOUND;
	}

	return RC_OK;
}

/*
 * Create a tree ready for Scan,
 * make all the Keys sorted for the Scanner to read them
 */
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
	int i=0,j, indexSmallKey;
	Value tempValueSwap;
	RID tempRidSwap;

	switch(AllocBTree[i]->value.dt)
	{
	case DT_INT:
		for(i=0;i<numOfKeys-1;i++)
		{
			indexSmallKey = i;

			//SORT
			for(j=i+1;j<numOfKeys;j++)
			{
				if(AllocBTree[j]->value.v.intV < AllocBTree[indexSmallKey]->value.v.intV)
				{
					indexSmallKey = j;
				}
			}
			tempValueSwap = AllocBTree[indexSmallKey]->value;
			tempRidSwap = AllocBTree[indexSmallKey]->rid;

			AllocBTree[indexSmallKey]->value = AllocBTree[i]->value;
			AllocBTree[indexSmallKey]->rid = AllocBTree[i]->rid;

			AllocBTree[i]->value = tempValueSwap;
			AllocBTree[i]->rid = tempRidSwap;
		}
		break;

	case DT_FLOAT:
		for(i=0;i<numOfKeys-1;i++)
		{
			indexSmallKey = i;

			for(j=i+1;j<numOfKeys;j++)
			{
				if(AllocBTree[j]->value.v.floatV < AllocBTree[indexSmallKey]->value.v.floatV)
				{
					indexSmallKey = j;
				}
			}
			tempValueSwap = AllocBTree[indexSmallKey]->value;
			tempRidSwap = AllocBTree[indexSmallKey]->rid;

			AllocBTree[indexSmallKey]->value = AllocBTree[i]->value;
			AllocBTree[indexSmallKey]->rid = AllocBTree[i]->rid;

			AllocBTree[i]->value = tempValueSwap;
			AllocBTree[i]->rid = tempRidSwap;
		}
		break;
	}
	return RC_OK;
}

/*
 * read the entry from the Tree until the No more entires are left in the tree and
 * store the page and slot info in result
 */
RC nextEntry (BT_ScanHandle *handle, RID *result)
{
	if(scanNextEntry < numOfKeys)
	{
		result->page = AllocBTree[scanNextEntry]->rid.page;
		result->slot = AllocBTree[scanNextEntry]->rid.slot;
		scanNextEntry++;
		return RC_OK;
	}
	else
	{
		return RC_IM_NO_MORE_ENTRIES;
	}

}

/*
 * Close the Scan for Tree
 */
RC closeTreeScan (BT_ScanHandle *handle)
{
	free(handle);
	return RC_OK;
}

// debug and test functions
/*
 * Print the B+ TREE Representation
 */
char *printTree (BTreeHandle *tree)
{
	int count = 1, tempCount =1;
	int i;
	char opString[500];
	char newopString[500];
	char finalResult[500]="";
	char TREE[500] = "";

	strcpy(opString,"1,");
	int compare = 2;

	for(i=0;i<numOfKeys;i++)
	{
		if(i%(compare) == 0 && i!=0)
		{
			sprintf(newopString,"%d",AllocBTree[i]->value.v.intV);
			strcat(opString,newopString);
			strcat(opString,",");
			sprintf(newopString,"%d",(tempCount+1));
			strcat(opString,newopString);
			strcat(opString,",");
			tempCount++;
			count++;
		}

		if(i%(compare) == 0 && i!=0)
		{
			sprintf(newopString,"%d",count);
			strcpy(finalResult,newopString);
			strcat(finalResult,"\n");
		}
		sprintf(newopString,"%d",AllocBTree[i]->rid.page);
		strcat(finalResult,newopString);
		strcat(finalResult,".");
		sprintf(newopString,"%d",AllocBTree[i]->rid.slot);
		strcat(finalResult,newopString);
		strcat(finalResult,", ");
		sprintf(newopString,"%d",AllocBTree[i]->value.v.intV);
		strcat(finalResult,newopString);
		strcat(finalResult,",");
		if(!(i%(compare) == 0 && i!=0))
		{
			strcat(TREE,finalResult);
		}

	}

	printf("%s\n",opString);
	printf("%s\n",TREE);
	return tree->idxId;
}
