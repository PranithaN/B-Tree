Group 18 - Assignment 4 - B Tree | Version 1.0 | 12/1/2015





Description

---------------------------------------------------------


The B+ Tree is used to index pages according to their Key values. Every key is inserted at their respective nodes.
The pages can be accessed by following a pointer to that node where the page exist.


How to run

-----------------------------------------------------------


1. Open terminal and Clone from BitBucket to the required location.

2. Navigate to assign4 folder.

3. Use make command to execute B Tree,
 
	$ make all
4. Use make command to execute test_expr,
 
	$ make expr
5. To clean,
	$ make clean





BONUS Implementation - Allow different data types as keys
------------------------------------------------------
We have used int, float and string as keys of different datatypes


Solution Description

-----------------------------------------------------------


initIndexManager: It is used to initialize the index manager.

shutdownIndexManager: It is used to shutdown the index manager

createBtree: This function is used to create a B+ tree and initialize all the attributes to that tree.

openBtree: This Functions opens the B tree index created and uses buffer manager to access the page file.

closeBtree: It is used to free the tree pointer and ensures all the pages are flushed to the page file.

deleteBtree: This function is used to remove the tree

getNumNodes: It takes the tree as input, and results the number of nodes the tree has in its result parameter.

getNumEntries: It takes the tree as input, and results the number of entries the tree has in its result parameter.

getKeyType: It takes the tree as input, and results datatype for the key in its result parameter.

findKey: It takes the tree and its key input and searches its RID to store it to result

insertKey: It takes the tree and its key as input, and while inserting it checks if the node is full or not.

deleteKey: It takes the tree and its key as input, to find and delete the value and its RID in the tree. After deleting it marks the node as not full.

openTreeScan: It takes the tree as input, and create a new ScanHandle for the tree

nextEntry: It takes the ScanHandle and output RID in the ascending order of values

closeTreeScan: It take the ScanHandle and free its management data




Test Cases

-----------------------------------------------------------

Files: test_assign4_1.c test_expr.c

1. The program verifies all the test cases that are mentioned in the test file i.e test_assign4_1 and ensures that there are no errors.

2. The memory leaks in the test case are solved.





Team Members: - Group 18

-----------------------------------------------------------

Loy Mascarenhas

Pranitha Nagavelli

Shalin Chopra