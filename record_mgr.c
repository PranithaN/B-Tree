/*
 * This assignment is used to create a RECORD MANAGER which handles Tables with some fixed Schema.
 * This file record_mgr.c allows to insert records, delete_records, update_records and Scan through the records.
 * The design implements storing each table in a new pagefile & accessing of these pageFiles (Table) is done
 * using the Buffer Manager
 */
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "unistd.h"

#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "expr.h"


//Struct to store Table Information
typedef struct RM_TableInfo
{
	int numOfTuples;
	int schemaSize;

}RM_TableInfo;

//struct for RECORD MANGER INFORMATION
typedef struct RM_RecordMgmt
{
	BM_BufferPool *bm;	//Buffer Pool Management Information & its attributes
	int *freePages;		//store the freePages details

} RM_RecordMgmt;

//struct for RECORD SCAN MANAGEMENT INFORMATION
typedef struct RM_ScanMgmt
{
	Expr *condn;				//The scan Condition associated with every Scan
	Record *currentRecord;		//helps to find the current record
	int currentPage;			//used to store current page that is scanned info
	int currentSlot;			//used to store current slot that is scanned info

}RM_ScanMgmt;

int totalPages;		//Global Variable to store the 'TOTAL NUMBER OF PAGES IN A PAGE FILE'

/*
 * This method is used to calculate the Offset associated
 * with every attribute, this is done by getting the size for each of these
 * for string we take the length of the String
 */
RC attrOffset (Schema *schema, int attrNum, int *result)
{
	int offset = 0;		//store the OFFSET calculated
	int attrPosition = 0;	//variable used to loop through all the attributes

	//for all the attributes there, find the offset, based on its DataTypes
	for(attrPosition = 0; attrPosition < attrNum; attrPosition++)
	{
		switch (schema->dataTypes[attrPosition])
		{
		case DT_STRING:
			offset += schema->typeLength[attrPosition];		//String add the typeLenghth to the offset
			break;
		case DT_INT:
			offset += sizeof(int);
			break;
		case DT_FLOAT:
			offset += sizeof(float);
			break;
		case DT_BOOL:
			offset += sizeof(bool);
			break;
		}
	}

	*result = offset;	//commit the final calculated offset in the result variable
	return RC_OK;
}

/*TABLE AND RECORD MANAGER FUNCTIOS*/

/*
 * This method is used to Initialise a record manager
 */
RC initRecordManager (void *mgmtData)
{
	/*
	 * All the initializations are done,
	 * nothing to be done here
	 */
	return RC_OK;
}

/*
 * This method is used to Shutdown a record Manger
 */
RC shutdownRecordManager ()
{
	/*
	 * All the memory free'ing is done while, allocation
	 * Nothing to be done here
	 */
	return RC_OK;
}

/*
 * This function is used to Create a Table,
 * and also used to store the Information about the schema
 */
RC createTable (char *name, Schema *schema)
{
	//File handle from Storage Manager
	SM_FileHandle fh;

	char *serializedData;	//data is stored in a serialized format in this variable

	/*
	 * using access to check whether a Table can be accessed,
	 * this access is specified with a existence test F_OK,
	 * Checks whether Table with "name" alread exists
	 * If yes, it returns RC_TABLE_ALREADY_EXISTS
	 */
	if(access(name,F_OK)!= -1)
	{
		return RC_TABLE_ALREADY_EXISTS;
	}

	//Creating a PageFile with name as given in createTable (name, schema)
	if(createPageFile(name)!=RC_OK)
	{
		return RC_FILE_NOT_FOUND;
	}

	//Open the pageFile
	if(openPageFile(name,&fh)!=RC_OK)
	{
		return RC_FILE_NOT_FOUND;
	}

	//Store the table Information & initialize its attributes
	RM_TableInfo *tableInfo = (RM_TableInfo *)malloc(sizeof(RM_TableInfo));

	tableInfo->schemaSize = 0;

	/*The schema is now serialized using serializeSchema() function
	 * The first page of the file is used to store the entire Formatted (Serialized Schema)
	 */
	serializedData = serializeSchema(schema);

	//Write the serialized data ontot Page = 0
	if(writeBlock(0,&fh,serializedData)!=RC_OK)
	{
		return RC_WRITE_FAILED;
	}

	return RC_OK;	//all steps executed correctly, and Table is created, return RC_OK
}

/*
 * This function is used to Open a Created Table,
 * for any operation to be performed, the table has to be opened first
 * Also store the Schema related info by deserializing the schema,
 * in the Table Data
 */
RC openTable (RM_TableData *rel, char *name)
{
	//Record Management to store record attributes
	RM_RecordMgmt *rm_mgmt = (RM_RecordMgmt*)malloc(sizeof(RM_RecordMgmt));

	FILE *fptr;	//FILE pointer
	fptr = fopen(name, "r+");	//Open the PageFile (Table)

	//Get the total Number of Pages in the page File
	char* readHeader;
	readHeader = (char*)calloc(PAGE_SIZE,sizeof(char));

	fgets(readHeader,PAGE_SIZE,fptr);

	char* totalPage;
	totalPage = readHeader;
	totalPages = atoi(totalPage);	//convert to integer

	//Make a Buffer Pool
	rm_mgmt->bm = MAKE_POOL();

	//Make a Page Handle
	BM_PageHandle *page = MAKE_PAGE_HANDLE();

	//Initialize the BufferPool
	initBufferPool(rm_mgmt->bm,name,6,RS_FIFO,NULL);

	//Pin the Page = 0, which has the Schema Information
	pinPage(rm_mgmt->bm,page,0);

	//FreePages are stored in an array
	rm_mgmt->freePages = (int*)malloc(sizeof(int));
	rm_mgmt->freePages[0] = totalPages;

	//initialize the table data attributes

	//Deserialzing the Schema gives us the Relation information (i.e. Schema info)
	rel->schema = deserializeSchema(page->data);

	//store the name of the schema
	rel->name = name;

	//store the record management details
	rel->mgmtData = rm_mgmt;

	//Free the temp. memory allocations
	free(readHeader);
	free(page);

	return RC_OK;
}

/*
 * This function is used to close the Table,
 * after the Operations on the table is Completed
 * In close, all the memory allocations are de-allocated,
 * so as to avoid any memory leaks
 */
RC closeTable (RM_TableData *rel)
{
	//shutdownBufferPool
	shutdownBufferPool(((RM_RecordMgmt*)rel->mgmtData)->bm);

	//Free all the Table data allocations and the Schema
	free(rel->mgmtData);

	free(rel->schema->attrNames);
	free(rel->schema->dataTypes);
	free(rel->schema->keyAttrs);
	free(rel->schema->typeLength);

	free(rel->schema);

	return RC_OK;
}

/*
 * This fucntion is used to delete a created table,
 * it is done by using destroyPageFile function
 * from the BufferManger Implementation
 */
RC deleteTable (char *name)
{
	if(destroyPageFile(name)!=RC_OK)
	{
		return RC_FILE_NOT_FOUND;
	}

	return RC_OK;
}

/*
 * Used to get the Total Number of Tuples in the Table
 */
int getNumTuples (RM_TableData *rel)
{
	int countOfTuples = 0;		//to store the count of Total of number of Tuples
	RC getSuccess;				//flag to mark the success of getting a record

	Record *record = (Record *)malloc(sizeof(Record));
	RID rid;

	rid.page = 1;
	rid.slot = 0;

	//count until we reach the end of the table i.e. last page in the pageFile
	while(rid.page > 0 && rid.page < totalPages)
	{
		getSuccess = getRecord (rel, rid, record);

		/*
		 * If fetching of record is Successfull, then increment the count
		 */
		if(getSuccess == RC_OK)
		{
			countOfTuples++;

			rid.page += 1;
			rid.slot = 0;
		}
	}

	record = NULL;
	free(record); // free record to avoid Memory leaks

	return countOfTuples;// returning the count
}

/*
 * HANDLING OF RECORDS IN A TABLE
 */

/*
 * This function is used to insert a new Record into the table
 * while inserting, the record Manager assigns a RID to the record
 * and update the "record" parameter too
 */
RC insertRecord (RM_TableData *rel, Record *record)
{
	//Create a record variable
	Record *r = (Record *)malloc(sizeof(Record));

	RID rid;
	rid.page = 1;
	rid.slot = 0;

	//Find the next place of insertion of a record
	while(rid.page > 0 && rid.page < totalPages)
	{
		rid.page = rid.page + 1;
		rid.slot = 0;

		/*getRecord(rel, rid, r); //obtaining the record from the table
		//checking for soft delete record in the table space for insertion
		if(strncmp(r->data, "DELETED_RECORD", 14) == 0)
			break;*/
	}


	r = NULL;
	free(r); //free the memory of r which was just a temporary allocation

	//mark the page as free page
	((RM_RecordMgmt *)rel->mgmtData)->freePages[0] = rid.page;

	//create a page handle
	BM_PageHandle *page = MAKE_PAGE_HANDLE();

	//assign the record, its RID and slot number
	record->id.page = ((RM_RecordMgmt *)rel->mgmtData)->freePages[0];
	record->id.slot = 0;

	//Serialize the Record to be inserted
	char * serializedRecord = serializeRecord(record, rel->schema);

	//Pin the record page, to mark that it is in use
	pinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page, ((RM_RecordMgmt *)rel->mgmtData)->freePages[0]);

	//insert the new record data, into the Table i.e. Pages of the PageFile
	memset(page->data, '\0', strlen(page->data));
	sprintf(page->data, "%s", serializedRecord);

	//mark the page as Dirty Page, as now there is a new record entry on that page
	markDirty(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

	//Unpin the page as now it has been used
	unpinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

	//Force Page to push entire data onto the page
	forcePage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

	//printf("record data: %s\n", page->data);

	free(page);		//free page, avoid memory leaks

	((RM_RecordMgmt *)rel->mgmtData)->freePages[0] += 1;

	totalPages++;
	return RC_OK;
}

/*
 * This function is used to Delete the Record from the Table
 * BONUS IMPLEMENTATION - TOMBSTONE IS USED IN DELETION PROCESS
 */
RC deleteRecord (RM_TableData *rel, RID id)
{
	/*A TOMBSTONE FLAG is set to mark it as a Deleted Record, but actual delete is not done
	 * The Record is Prefixed with DELETED_RECORD string
	 * ex. consider the record to be deleted be
	 * (a:1,b:aaaa,c:45), the serializer store it as [1-0](a:1,b:aaaa,c:45)
	 * deleted will be marked as DELETED RECORD[1-0](a:1,b:aaaa,c:45)
	 */

	char deleteTombStomeFlag[14] = "DELETED_RECORD";	//Tombstone flag

	char *temp = (char*)malloc(sizeof(char*));			//temp memory allocation to preappend the flag

	if(id.page > 0 && id.page <=  totalPages)
	{
		BM_PageHandle *page = MAKE_PAGE_HANDLE();

		//Pin page, to mark it in USE
		pinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page, id.page);

		//attach the flag to deletedRecord
		strcpy(temp, deleteTombStomeFlag);
		strcat(temp, page->data);

		//set pageNum
		page->pageNum = id.page;

		//copy the new data onto the Page i.e. modify the page->data
		memset(page->data, '\0', strlen(page->data));
		sprintf(page->data, "%s", temp);

		//marking the page dirty, as new data has been written, i.e. tombstone data
		markDirty(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

		//unpin page, after use
		unpinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

		//write the new data onto the page, in the pageFile
		forcePage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

		page = NULL;
		free(page);		//free page, avoid leaks
		return RC_OK;
	}
	else
	{
		return RC_RM_NO_MORE_TUPLES;
	}

	return RC_OK;
}


/*
 * This function is used to update a Record,
 * updating on a deleted page is not possible
 */
RC updateRecord (RM_TableData *rel, Record *record)
{
	//Find the data to be updated
	if(record->id.page > 0 && record->id.page <=  totalPages)
	{
		BM_PageHandle *page = MAKE_PAGE_HANDLE();

		int pageNum, slotNum;

		// Setting record id and slot number
		pageNum = record->id.page;
		slotNum = record->id.slot;

		//Compare if the record is a deleted Record,
		//return update not possible for deleted records (EC 401)

		if(strncmp(record->data, "DELETED_RECORD", 14) == 0)
			return RC_RM_UPDATE_NOT_POSSIBLE_ON_DELETED_RECORD;

		//Take the serailized updated record data
		char *record_str = serializeRecord(record, rel->schema);

		//pin page
		pinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page, record->id.page);

		//set the new fields, or the entire modified page
		memset(page->data, '\0', strlen(page->data));
		sprintf(page->data, "%s", record_str);

		//free the temp data
		free(record_str);

		//mark the page as dirty
		markDirty(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

		//unpin the page, after use
		unpinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

		//force write onto the page, as it is modified page now
		forcePage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

		//printf("record data in update: %s\n", page->data);

		free(page);		//free page, avoid leaks
		return RC_OK;
	}
	else
	{
		return RC_RM_NO_MORE_TUPLES;	//return the data to be modfied not found
	}

	return RC_OK;
}

/*
 * This function is used to get the Record from the Table
 * using the RID details
 */
RC getRecord (RM_TableData *rel, RID id, Record *record)
{

	//find the record in the record table
	if(id.page > 0 && id.page <=  totalPages)
	{
		//make a page handle
		BM_PageHandle *page = MAKE_PAGE_HANDLE();

		//pin page, and mark it for use
		pinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page, id.page);

		//temp, to store the record data
		char *record_data = (char*)malloc(sizeof(char) * strlen(page->data));

		//copy the data
		strcpy(record_data,page->data);

		//printf("Page data is: %s",record_data);

		//store the record data and id
		record->id = id;

		//deSerialze the data
		Record* deSerializedRecord = deserializeRecord(record_data,rel->schema);

		//unpin the page, after fetching the record
		unpinPage(((RM_RecordMgmt *)rel->mgmtData)->bm, page);

		//return the new data
		record->data = deSerializedRecord->data;

		//printf("Record Data in getRecord: %s\n",record->data);

		//free temp. allocations to avoid memory leaks
		free(deSerializedRecord);
		free(page);

		return RC_OK;
	}
	else		//if record not found return RC_RM_NO_MORE_TUPLES
	{
		return RC_RM_NO_MORE_TUPLES;
	}

	return RC_OK;
}

/*
 * SCANS -  SCANNING THROUGH THE RECORDS WITH A GIVEN CONDITION
 */

/*
 * This function is used to initialize the RM_ScanHandle
 * and all its attributes
 */
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{

	//using Scan Handle Structure & init its attributes
	scan->rel = rel;

	//Initialize the created Scan Management Structure
	RM_ScanMgmt *scan_mgmt = (RM_ScanMgmt*)malloc(sizeof(RM_ScanMgmt));
	scan_mgmt->condn = cond;
	scan_mgmt->currentRecord = (Record*)malloc(sizeof(Record));
	scan_mgmt->currentPage = 1;
	scan_mgmt->currentSlot = 0;

	//store the managememt data
	scan->mgmtData = scan_mgmt;

	return RC_OK;
}

/*
 * This fucntion is used along with the startScan function,
 * call to this method returns the tuples that satisfy the scan condition
 */
RC next (RM_ScanHandle *scan, Record *record)
{
	Value *result;
	RID rid;

	//initialize the page and slot number from the data Structure RM_ScanMgmt
	rid.page = ((RM_ScanMgmt *)scan->mgmtData)->currentPage;
	rid.slot = ((RM_ScanMgmt *)scan->mgmtData)->currentSlot;

	//if the scan condition is passed as NULL, return all the tuples from the table
	if(((RM_ScanMgmt *)scan->mgmtData)->condn == NULL)
	{
		//loop until end of the table
		while(rid.page > 0 && rid.page < totalPages)
		{
			//get the record
			getRecord (scan->rel, rid, ((RM_ScanMgmt *)scan->mgmtData)->currentRecord);

			record->data = ((RM_ScanMgmt *)scan->mgmtData)->currentRecord->data;
			record->id = ((RM_ScanMgmt *)scan->mgmtData)->currentRecord->id;
			((RM_ScanMgmt *)scan->mgmtData)->currentPage = ((RM_ScanMgmt *)scan->mgmtData)->currentPage + 1;

			//assign the new page scan details
			rid.page = ((RM_ScanMgmt *)scan->mgmtData)->currentPage;
			rid.slot = ((RM_ScanMgmt *)scan->mgmtData)->currentSlot;

			return RC_OK;
		}
	}
	else	//if specific scan condition is supplied
	{
		//loop until end of the entire table, scan for each record
		while(rid.page > 0 && rid.page < totalPages)
		{
			//get record
			getRecord (scan->rel, rid, ((RM_ScanMgmt *)scan->mgmtData)->currentRecord);

			//Evaluate the Expression
			evalExpr (((RM_ScanMgmt *)scan->mgmtData)->currentRecord, scan->rel->schema, ((RM_ScanMgmt *)scan->mgmtData)->condn, &result);

			//if result is satisfied, i.e. scan returns the attributes needed i.e. Boolean Attrbiute & v.boolV as 1
			if(result->dt == DT_BOOL && result->v.boolV)
			{
				record->data = ((RM_ScanMgmt *)scan->mgmtData)->currentRecord->data;
				record->id = ((RM_ScanMgmt *)scan->mgmtData)->currentRecord->id;
				((RM_ScanMgmt *)scan->mgmtData)->currentPage = ((RM_ScanMgmt *)scan->mgmtData)->currentPage + 1;

				return RC_OK;
			}
			else	//scan the next record, until the record is found
			{
				//increment the page to next page
				((RM_ScanMgmt *)scan->mgmtData)->currentPage = ((RM_ScanMgmt *)scan->mgmtData)->currentPage + 1;

				//rid as next page id
				rid.page = ((RM_ScanMgmt *)scan->mgmtData)->currentPage;
				rid.slot = ((RM_ScanMgmt *)scan->mgmtData)->currentSlot;
			}
		}
	}

	//re-init to point to first page
	((RM_ScanMgmt *)scan->mgmtData)->currentPage = 1;

	//if all records scanned return no more tuples found, i.e. scan is completed
	return RC_RM_NO_MORE_TUPLES;
}

/*
 * This function is used to indicate the record manager
 * that all the resources can now be cleaned up
 */
RC closeScan (RM_ScanHandle *scan)
{
	//Make all the allocations, NULL and free them

	((RM_ScanMgmt *)scan->mgmtData)->currentRecord = NULL;
	free(((RM_ScanMgmt *)scan->mgmtData)->currentRecord);

	scan->mgmtData = NULL;
	free(scan->mgmtData);

	scan = NULL;
	free(scan);

	return RC_OK;
}

/*
 * DEALING WITH SCHEMAS
 */

/*
 * This function is used to get the entire Size of the record
 */
int getRecordSize(Schema *schema)
{
	int i, recordSize = 0;

	for(i = 0; i < schema->numAttr; i++)
	{
		if(schema->dataTypes[i] == DT_INT)
			recordSize += sizeof(int);
		else if(schema->dataTypes[i] == DT_FLOAT)
			recordSize += sizeof(float);
		else if(schema->dataTypes[i] == DT_BOOL)
			recordSize += sizeof(bool);
		else
			recordSize += schema->typeLength[i];
	}

	return recordSize;
}

/*
 * SCHEMA CREATION -
 * This function is used to Create a new Schema
 * and initialize all the attributes for those schema
 */
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
	//allocate memory for Schema to be created
	Schema *newSchema = (Schema*)malloc(sizeof(Schema));

	//initialize all the attributes for the schema
	newSchema->numAttr = numAttr;
	newSchema->attrNames = attrNames;
	newSchema->dataTypes = dataTypes;
	newSchema->typeLength = typeLength;
	newSchema->keySize = keySize;
	newSchema->keyAttrs = keys;

	return newSchema;
}

//free the allocated Schema
RC freeSchema (Schema *schema)
{
	free(schema);
	return RC_OK;
}

/*
 * DEALING WITH RECORDS AND ATTRIBUTE VALUES
 */

/*
 * Creates a new Record and allocates memmory for record & its data
 */
RC createRecord (Record **record, Schema *schema)
{
	*record = (Record*)malloc(sizeof(Record));
	(*record)->data = (char*)malloc(getRecordSize(schema));

	return RC_OK;
}

/*
 * Freeing the record created earlier
 * the data and the record
 */
RC freeRecord (Record *record)
{
	//data is free'd first
	record->data = NULL;
	free(record->data);

	//free the complete record
	record = NULL;
	free(record);

	return RC_OK;
}

/*
 * This function is used to get the attribute values of a record
 */
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
	//variables for offset
	int offset;
	char *attrData;

	*value = (Value*)malloc(sizeof(Value));

	//calculate the offset, to get the attribute value from
	attrOffset(schema, attrNum, &offset);
	attrData = record->data + offset;

	(*value)->dt =schema->dataTypes[attrNum];

	//switch on the attribute data types
	switch(schema->dataTypes[attrNum])
	{
		case DT_INT:
		{
			memcpy(&((*value)->v.intV) ,attrData,sizeof(int));	//get the attribute into value
		}
		break;

		case DT_STRING:
		{
			char *buf;
			int len = schema->typeLength[attrNum];
			buf = (char *) malloc(len + 1);
			strncpy(buf, attrData, len);
			buf[len] = '\0';
			(*value)->v.stringV = buf;
		}
		break;

		case DT_FLOAT:
		{
			memcpy(&((*value)->v.floatV),attrData, sizeof(float));
		}
		break;

		case DT_BOOL:
		{
			memcpy(&((*value)->v.boolV),attrData ,sizeof(bool));
		}
		break;

		default:			//if different data encountered other than INT, FLOAT, BOOL, STRING return (EC 402)
			return RC_RM_NO_DESERIALIZER_FOR_THIS_DATATYPE;
	}

	return RC_OK;
}

/*
 * This function is used to set the attributes in a record
 */
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
	//Modifying rm_serializer serializeAttr
	int offset;
	char *attrData;

	//calculate the offset values
	attrOffset(schema, attrNum, &offset);
	attrData = record->data + offset;

	//switch on attributes datatype value
	switch(schema->dataTypes[attrNum])
	{
		case DT_INT:
		{
			memcpy(attrData,&(value->v.intV) ,sizeof(int));		//copy the newly set attribute value
		}
		break;

		case DT_STRING:
		{
			char *buf;
			int len = schema->typeLength[attrNum];
			buf = (char *) malloc(len);
			buf = value->v.stringV;
			buf[len] = '\0';			//end the string with '\0'

			memcpy(attrData,buf,len);
		}
		break;

		case DT_FLOAT:
		{
			memcpy(attrData,&(value->v.floatV), sizeof(float));
		}
		break;

		case DT_BOOL:
		{
			memcpy(attrData,&(value->v.boolV) ,sizeof(bool));
		}
		break;

		default:
			return RC_RM_NO_DESERIALIZER_FOR_THIS_DATATYPE;
	}
	return RC_OK;
}
