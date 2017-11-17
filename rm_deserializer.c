#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "dberror.h"
#include "tables.h"
#include "record_mgr.h"

// dynamic string
typedef struct VarString {
	char *buf;
	int size;
	int bufsize;
} VarString;

#define MAKE_VARSTRING(var)				\
		do {							\
			var = (VarString *) malloc(sizeof(VarString));	\
			var->size = 0;					\
			var->bufsize = 100;					\
			var->buf = malloc(100);				\
		} while (0)

#define FREE_VARSTRING(var)			\
		do {						\
			free(var->buf);				\
			free(var);					\
		} while (0)

#define GET_STRING(result, var)			\
		do {						\
			result = malloc((var->size) + 1);		\
			memcpy(result, var->buf, var->size);	\
			result[var->size] = '\0';			\
		} while (0)

#define RETURN_STRING(var)			\
		do {						\
			char *resultStr;				\
			GET_STRING(resultStr, var);			\
			FREE_VARSTRING(var);			\
			return resultStr;				\
		} while (0)

#define ENSURE_SIZE(var,newsize)				\
		do {								\
			if (var->bufsize < newsize)					\
			{								\
				int newbufsize = var->bufsize;				\
				while((newbufsize *= 2) < newsize);			\
				var->buf = realloc(var->buf, newbufsize);			\
			}								\
		} while (0)

#define APPEND_STRING(var,string)					\
		do {									\
			ENSURE_SIZE(var, var->size + strlen(string));			\
			memcpy(var->buf + var->size, string, strlen(string));		\
			var->size += strlen(string);					\
		} while(0)

#define APPEND(var, ...)			\
		do {						\
			char *tmp = malloc(10000);			\
			sprintf(tmp, __VA_ARGS__);			\
			APPEND_STRING(var,tmp);			\
			free(tmp);					\
		} while(0)

/*
 * Function to deserialize the Schema
 */
Schema *
deserializeSchema(char *serializedSchemaData)
{
	int i,j;
	VarString *result;
	MAKE_VARSTRING(result);

	Schema *schema = (Schema*)malloc(sizeof(Schema));

	int schemaNumAttr,lastAttr;

	char *splitStart = (char*)malloc(sizeof(char));
	char *splitEnd = (char*)malloc(sizeof(char));
	char *splitString = (char*)malloc(sizeof(char));

	//split on token values
	splitStart = strtok(serializedSchemaData,"<");
	splitEnd = strtok(NULL,">");

	//convert to long/int values
	schemaNumAttr = strtol(splitEnd, &splitStart,10);

	schema->numAttr = schemaNumAttr;

	schema->attrNames = (char **)malloc(sizeof(char*) * schemaNumAttr);
	schema->dataTypes = (DataType*)malloc(sizeof(DataType) * schemaNumAttr);
	schema->typeLength = (int*)malloc(sizeof(int) * schemaNumAttr);

	splitEnd = strtok(NULL,"(");

  	lastAttr =  schemaNumAttr-1;

  	//put in the dataTypes and thier datalengths
  	for(i=0;i<schemaNumAttr;i++)
  	{
  		splitEnd = strtok(NULL,": ");

  		schema->attrNames[i] = (char*)malloc(sizeof(char*));
  		strcpy(schema->attrNames[i],splitEnd);

  		if(i == lastAttr)
  		{
  			splitEnd = strtok(NULL,") ");
  		}
  		else
  		{
  			splitEnd = strtok(NULL,", ");
  		}

  		if(strcmp(splitEnd,"INT")==0)
  		{
  			schema->dataTypes[i] = DT_INT;
  			schema->typeLength[i] = 0;
  		}
  		else if(strcmp(splitEnd,"FLOAT")==0)
  		{
  			schema->dataTypes[i] = DT_FLOAT;
  			schema->typeLength[i] = 0;
  		}
  		else if(strcmp(splitEnd,"BOOL")==0)
  		{
  			schema->dataTypes[i] = DT_BOOL;
  			schema->typeLength[i] = 0;
  		}
  		else
  		{
  			strcpy(splitString, splitEnd);
  			char *str = (char*)malloc(sizeof(char));
  			sprintf(str,"%d",i);
  			strcat(splitString,str);

  			str = NULL;
  			free(str);
  		}
  	}//end for()

  	//put in the keyAttrs

  	//check if there are any keys present
  	if((splitEnd = strtok(NULL,"("))!=NULL)
  	{
  		splitEnd = strtok(NULL,")");
  		char *splitKey = (char*)malloc(sizeof(char));
  		char *keyAttr[schemaNumAttr];
  		int numOfKeys = 0;

  		splitKey = strtok(splitEnd,", ");

  		//Find out the number of Keys & store the attrValues for those Keys
  		while(splitKey!=NULL)
  		{
  			keyAttr[numOfKeys] = (char*)malloc(sizeof(char*));
  			strcpy(keyAttr[numOfKeys],splitKey);
  			numOfKeys++;
  			splitKey = strtok(NULL,", ");
  		}

  		splitKey = NULL;
  		free(splitKey);

  		//MARK all the key attrs as their INDEX values
  		schema->keyAttrs = (int*)malloc(sizeof(int)*numOfKeys);
  		schema->keySize = numOfKeys;

  		//for every Key, find the attributes and mark it's index
  		for(i=0;i<numOfKeys;i++)
  		{
  			for(j=0;j<schemaNumAttr;j++)
  			{
  				if(strcmp(keyAttr[i],schema->attrNames[j])==0)
  				{
  					//mark the index
  					schema->keyAttrs[i] = j;
  					break;
  				}
  			}
  		}
  	}

  	//for STRING[SIZE] allocate all the attributes

  	if(strlen(splitString)!=0)
  	{
  		splitString = strtok(splitString,"[");
  		if(strcmp(splitString,"STRING")==0)
  		{
  			int val, index;
  			splitString = strtok(NULL,"]");
  			val = atoi(splitString);
  			splitString = strtok(NULL,"=");
  			index = atoi(splitString);
  			schema->dataTypes[index] = DT_STRING;

  			schema->typeLength[index] = val;
  		}
  	}

  	splitString = NULL;
  	splitEnd = NULL;
  	splitStart = NULL;
  	free(splitString);
  	free(splitStart);
  	free(splitEnd);

  	return schema;
}

/*
 * Deserialize Record - split into tokens
 */
Record*
deserializeRecord(char *desiralize_record_str, Schema *schema)
{
	int i, lastAttr = schema->numAttr-1;
	int intVal;
	float floatVal;
	bool boolVal;

	Value *value;
	Record *record = (Record*)malloc(sizeof(Record*));
	record->data = (char*)malloc(sizeof(char*));

	char *splitStart, *splitEnd;

	splitStart = strtok(desiralize_record_str,"(");

	for(i=0;i< schema->numAttr;i++)
	{
		splitEnd = strtok(NULL,":");

		if(i == lastAttr)
		{
			splitEnd = strtok(NULL,")");
		}
		else
		{
			splitEnd = strtok(NULL,",");
		}

		switch(schema->dataTypes[i])
		{
		case DT_INT:
			intVal = strtol(splitEnd, &splitStart, 10);
			MAKE_VALUE(value,DT_INT,intVal);
			setAttr(record,schema,i,value);
			free(value);
			break;

		case DT_FLOAT:
			floatVal = strtof(splitEnd, NULL);
			MAKE_VALUE(value,DT_FLOAT,floatVal);
			setAttr(record,schema,i,value);
			free(value);
			break;

		case DT_BOOL:
			boolVal = (splitEnd[0] == 't') ? TRUE: FALSE;
			MAKE_VALUE(value,DT_BOOL,boolVal);
			setAttr(record,schema,i,value);
			free(value);
			break;

		case DT_STRING:
			MAKE_STRING_VALUE(value,splitEnd);
			setAttr (record,schema,i,value);
			freeVal(value);
			break;
		}

	}

return record;
}
