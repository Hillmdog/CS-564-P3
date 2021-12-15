/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/file_exists_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)

{
	// check if the index file exists
	if (!File::exists(outIndexName)) {
		// create the index file if not already exists
		std::shared_ptr<BlobFile> blobFile; // initialized to nullptr
		blobFile = std::make_shared<BlobFile>(BlobFile::create(outIndexName));
		BTreeIndex::file = blobFile.get();
		// initialize metadata
		BTreeIndex::bufMgr = bufMgrIn;
		BTreeIndex::headerPageNum = 1;
		BTreeIndex::rootPageNum = 2;
		BTreeIndex::attributeType = attrType;
		BTreeIndex::attrByteOffset = attrByteOffset;
		// create header page
		Page* headerPage;
		bufMgr->allocPage(file, headerPageNum, headerPage); // file->writePage(headerPageNum, headerPage);
		IndexMetaInfo* headerInfo = (IndexMetaInfo *)headerPage;
		strcpy(headerInfo->relationName, relationName.c_str());
		headerInfo->attrByteOffset = attrByteOffset;
		headerInfo->attrType = attrType;
		headerInfo->rootPageNo = 2; 
		// const IndexMetaInfo btreeHeader = {outIndexName[0], attrByteOffset, attrType, 2};
		// Page headerPage = *(reinterpret_cast<const Page*>(&btreeHeader));

		bufMgr->unPinPage(file, headerPageNum, true);
		// insert entries for every tuple in the base relation using FileScan class
		FileScan fileScanner = FileScan(relationName, bufMgrIn);
		try {
			RecordId rid;
			while (true) {
				fileScanner.scanNext(rid);
				std::string record = fileScanner.getRecord();
				// get key from record 
				const char *record_ptr = record.c_str();
				const void *key = (int *)(record_ptr + attrByteOffset);
				insertEntry(key, rid);
			}
		} catch(EndOfFileException e) {
			// end of file scan
		}
		fileScanner.~FileScan();
	} else {
		// if exists, open the index file
		std::shared_ptr<BlobFile> blobFile;
		blobFile = std::make_shared<BlobFile>(BlobFile::open(outIndexName));
		BTreeIndex::file = blobFile.get();
		// check if the metadata in header matches with the given values
		BTreeIndex::bufMgr = bufMgrIn;
		BTreeIndex::headerPageNum = file->getFirstPageNo(); // or 1
		BTreeIndex::attributeType = attrType;
		BTreeIndex::attrByteOffset = attrByteOffset;
		Page* headerPage;
		bufMgr->readPage(file, headerPageNum, headerPage); // Page headerPage = (*file).readPage(headerPageNum);
		bufMgr->unPinPage(file, headerPageNum, false);
		// convert char[](Page) to struct 
		const IndexMetaInfo* headerInfo = reinterpret_cast<const IndexMetaInfo*>(&headerPage);
		// if not match, throw BadIndexInfoException
		if (headerInfo->relationName != relationName || headerInfo->attrByteOffset != attrByteOffset 
				|| headerInfo->attrType != attrType) {
			throw new BadIndexInfoException(outIndexName);
		}
		BTreeIndex::rootPageNum = headerInfo->rootPageNo;
	}
	scanExecuting = false;
	BTreeIndex::nextPageID = 3;
	BTreeIndex::nodeOccupancy = ( Page::SIZE - sizeof( int ) - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( PageId ) );
	BTreeIndex::leafOccupancy = ( Page::SIZE - sizeof( PageId ) ) / ( sizeof( int ) + sizeof( RecordId ) );
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	// scanExecuting = false;
	endScan();
	// assuming all pinned papges are unpinned as soon as the btree finishes using them.
	try {
		bufMgr->flushFile(file);
	} catch (BadgerDbException e) {
		std::cout << e.message();
		// unpin all pages
		// TODO
	}
	// delete bufMgr;
	file->~File();
}

// -----------------------------------------------------------------------------
// BTreeIndex::traverseTree
//------------------------------------------------------------------------------

LeafNodeInt BTreeIndex::traverseTree (Page current, int target, int level) {
    if (level != 1) {
		// cast page to nonLeafNode
		NonLeafNodeInt* cur = reinterpret_cast<NonLeafNodeInt*>(&current);
		LeafNodeInt next;
		int flag = 0;
		// search for next node
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
			int curkey = cur->keyArray[i];

			// if the target is less than the current key
			if (target < curkey) {
				// recursive case #1
				flag = 1;
				Page* curPage = nullptr; 
				bufMgr->readPage(file, cur->pageNoArray[i], curPage);
				next = traverseTree(*curPage, target, cur->level);
				break;
			}

		}
		if (flag == 0) {
			// std::shared_ptr<Page> nextPage;
			Page* nextPage;
			bufMgr->readPage(file, cur->pageNoArray[INTARRAYNONLEAFSIZE-1], nextPage);
			next = traverseTree(*nextPage, target, cur->level);
		}

		return next;

	} else {
		// leaf nodes!

		// cast to leafNode
		LeafNodeInt* cur = reinterpret_cast<LeafNodeInt*>(&current);

		// check if the target is in this node
		for (int i = 0; i < INTARRAYLEAFSIZE; i ++) {
			if (cur->keyArray[i] == target) {
				return *cur;
			}
		}

		// did not find the target
		LeafNodeInt* none = nullptr;
		return *none;
	}

}

void BTreeIndex::splitNonLeaf(NonLeafNodeInt* oldNode, NonLeafNodeInt* tempNode, NonLeafNodeInt* returnedNode) {
	// extract info from tempNode
	int newKey = tempNode->keyArray[0];
	PageId page1 = tempNode->pageNoArray[0];
	PageId page2 = tempNode->pageNoArray[1];

	// create new page
	Page* newPage = nullptr;
	PageId nextPageId = nextPageID;
	bufMgr->allocPage(file, nextPageId, newPage);
	// unpin page and increment nextPageID
	bufMgr->unPinPage(file, nextPageID, true);
	nextPageID+=1;

	// cast page to node
	NonLeafNodeInt* newNode = reinterpret_cast<NonLeafNodeInt*>(&newPage);

	// set all values in arrays of newNode to null
	for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
		newNode->keyArray[i] = NULL;
	}

	//calc halfindex
	int halfindex = INTARRAYNONLEAFSIZE/2;
	//get middlekey
	int middleKey = oldNode->keyArray[halfindex];
	// get page nos
	Page* oldPage = reinterpret_cast<Page*>(&oldNode);
	PageId nextPage1 = oldPage->page_number();
	PageId nextPage2 = newPage->page_number();

	// remove middlekey and pageNos
	for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
		if (oldNode->keyArray[i] > middleKey) {
			oldNode->keyArray[i-1] = oldNode->keyArray[i];
		}
	}
	oldNode->keyArray[INTARRAYNONLEAFSIZE-1] = NULL;

	//redistribute array values to each node
	for (int i = halfindex; i < INTARRAYNONLEAFSIZE; i++) {
		newNode->keyArray[i-halfindex] = oldNode->keyArray[i];

		// remove old values
		oldNode->keyArray[i] = NULL;
	}
	for (int i = halfindex; i < INTARRAYNONLEAFSIZE+1; i++) {
		newNode->pageNoArray[i-halfindex] = oldNode->pageNoArray[i];
	}

	// insert newKey and pages
	int tempKey[INTARRAYLEAFSIZE];
	PageId temppid[INTARRAYLEAFSIZE+1];

	// if newKey will go into oldNode
	if (newKey < middleKey) {

		// find pos first
		int pos = -1;
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
			if (oldNode->keyArray[i] > newKey || oldNode->keyArray[i] == NULL) {
				pos = i;
				break;
			}
		}	

		for (int i = 0; i < INTARRAYNONLEAFSIZE-1; i++) {
			if ( i < pos) {
				tempKey[i] = oldNode->keyArray[i];
			} else {
				tempKey[i+1] = oldNode->keyArray[i];
			}
		}

		// set middlekey
		tempKey[pos] = newKey;

		// loop for pageNoArray
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
			if ( i < pos) {
				temppid[i] = oldNode->pageNoArray[i];
			} else if (i > pos) {
				temppid[i+1] = oldNode->pageNoArray[i];
			}
		}

		// set pageIds
		temppid[pos] = page1;
		temppid[pos+1] = page2;

		// set temp arrays to oldNode
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
			oldNode->keyArray[i] = tempKey[i];
		}
		for (int i = 0; i < INTARRAYNONLEAFSIZE+1; i++) {
			oldNode->pageNoArray[i] = temppid[i];
		}

	// if newKey will go into newNode
	} else {

		// find pos first
		int pos = -1;
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
			if (newNode->keyArray[i] > newKey || newNode->keyArray[i] == NULL) {
				pos = i;
				break;
			}
		}	

		for (int i = 0; i < INTARRAYNONLEAFSIZE-1; i++) {
			if ( i < pos) {
				tempKey[i] = newNode->keyArray[i];
			} else {
				tempKey[i+1] = newNode->keyArray[i];
			}
		}

		// set middlekey
		tempKey[pos] = newKey;

		// loop for pageNoArray
		for (int i = 0; INTARRAYNONLEAFSIZE; i++) {
			if ( i < pos) {
				temppid[i] = newNode->pageNoArray[i];
			} else if (i > pos) {
				temppid[i+1] = newNode->pageNoArray[i];
			}
		}

		// set pageIds
		temppid[pos] = page1;
		temppid[pos+1] = page2;

		// set temp arrays to newNode
		// set temp arrays to oldNode
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
			newNode->keyArray[i] = tempKey[i];
		}
		for (int i = 0; i < INTARRAYNONLEAFSIZE+1; i++) {
			newNode->pageNoArray[i] = temppid[i];
		}

	}


	//give tempnode middlekey and pages too pass up
	returnedNode->level = oldNode->level+1; // this doesnt actually matter for this node
	returnedNode->keyArray[0] = middleKey;
	returnedNode->pageNoArray[0] = nextPage1;
	returnedNode->pageNoArray[1] = nextPage2;

}

void BTreeIndex::insertIntoNonLeaf(NonLeafNodeInt* tempNode, NonLeafNodeInt* cur) {
	// extract middle key and pageNos from tempNode
	int newKey = tempNode->keyArray[0];
	PageId page1 = tempNode->pageNoArray[0];
	PageId page2 = tempNode->pageNoArray[1];

	int tempKey[INTARRAYLEAFSIZE];
	PageId temppid[INTARRAYLEAFSIZE+1];
	// add them into the current node
	
	// find pos first
	int pos = -1;
	for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
		if (cur->keyArray[i] > newKey) {
			pos = i;
			break;
		}
	}

	for (int i = 0; i < INTARRAYNONLEAFSIZE-1; i++) {
		if ( i < pos) {
			tempKey[i] = cur->keyArray[i];
		} else {
			tempKey[i+1] = cur->keyArray[i];
		}
	}

	// set middlekey
	tempKey[pos] = newKey;

	// loop for pageNoArray
	for (int i = 0; INTARRAYLEAFSIZE; i++) {
		if ( i < pos) {
			temppid[i] = cur->pageNoArray[i];
		} else if (i > pos) {
			temppid[i+1] = cur->pageNoArray[i];
		}
	}

	// set pageIds
	temppid[pos] = page1;
	temppid[pos+1] = page2;

	// add new key and pages
	tempKey[pos] = newKey;
	temppid[pos] = page1;
	// potential place of failure
	temppid[pos+1] = page2;
	
	for (int i  = 0; i < INTARRAYNONLEAFSIZE; i++) {
		cur->keyArray[i] = tempKey[i];
	}
	for (int i  = 0; i < INTARRAYNONLEAFSIZE+1; i++) {
		cur->pageNoArray[i] = temppid[i];
	}

}

/*
	temp node pointer is the output
*/
void BTreeIndex::splitLeafNode(LeafNodeInt* cur, NonLeafNodeInt* tempNode, int target, RecordId id){
	// create new node and page
	Page* newPage = nullptr;
	PageId nextPageId = nextPageID;
	bufMgr->allocPage(file, nextPageId, newPage);
	bufMgr->unPinPage(file, nextPageId, true); // unpinpage
	LeafNodeInt* newNode = reinterpret_cast<LeafNodeInt*>(&newPage);
	nextPageID += 1;

	// set all values of newNodes arrays to null
	for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
		newNode->keyArray[i] = NULL;
		// newNode->ridArray[i] = NULL;
	}

	// set the middle key
	int halfindex = INTARRAYLEAFSIZE/2;
	int middleKey = cur->keyArray[halfindex];

	//populate arrays with old stuff
	for (int i = halfindex; i < INTARRAYLEAFSIZE; i++) {
		newNode->keyArray[i-halfindex] = cur->keyArray[i];
		newNode->ridArray[i-halfindex] = cur->ridArray[i];

		// remove old value
		cur->keyArray[i] = NULL;
		// cur->ridArray[i] = NULL;
	}

	// insert target key and rid into either the old node or the new one
	// create temp new arrays
	int tempKey[INTARRAYLEAFSIZE];
	RecordId temprid[INTARRAYLEAFSIZE];

	// if the target is going into the old node
	if (target < middleKey) {
		int pos = -1;
		int flag = -1;
		for (int i = 0; i < halfindex; i++) {
			if (cur->keyArray[i] < target && cur->keyArray != NULL) {
				tempKey[i] = cur->keyArray[i];
				temprid[i] = cur->ridArray[i];
			} else if (flag == -1) {
				pos = i;
				flag = 0;
			}
			if (cur->keyArray[i] > target){
				tempKey[i+1] = cur->keyArray[i];
				temprid[i+1] = cur->ridArray[i];
			}
		}

		tempKey[pos] = target;
		temprid[pos] = id;
		for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
			cur->keyArray[i] = tempKey[i];
			cur->ridArray[i] = temprid[i];
		}

	// if target goes into new node							
	} else {
		int pos = -1;
		int flag = -1;
		for (int i = 0; i < halfindex; i++) {
			if (newNode->keyArray[i] < target && newNode->keyArray[i] != NULL) {
				tempKey[i] = newNode->keyArray[i];
				temprid[i] = newNode->ridArray[i];
			} else if (flag == -1) {
				pos = i;
				flag = 0;
			}
			if (newNode->keyArray[i] > target){
				tempKey[i+1] = newNode->keyArray[i];
				temprid[i+1] = newNode->ridArray[i];
			}
		}

		tempKey[pos] = target;
		temprid[pos] = id;
		for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
			cur->keyArray[i] = tempKey[i];
			cur->ridArray[i] = temprid[i];
		}
	}

	// fix the linked list
	// fix the linked list
	newNode->rightSibPageNo = cur->rightSibPageNo;
	cur->rightSibPageNo = nextPageID;

	// set tempNodes info
	tempNode->level = 1;
	// need help with this
	tempNode->keyArray[0] = middleKey;
	Page* oldPage = reinterpret_cast<Page*>(&cur);
	// Page* newPage = reinterpret_cast<Page*>(newNode);
	tempNode->pageNoArray[0] = oldPage->page_number();// old node
	tempNode->pageNoArray[1] = newPage->page_number();// new node		
}

void insertIntoLeaf(LeafNodeInt* cur, int target, RecordId id) {
	// create temp new arrays
	int tempKey[INTARRAYLEAFSIZE];
	RecordId temprid[INTARRAYLEAFSIZE];

	// find pos for target key and rid to fit into
	// This needs fixing i think
	int pos = -1;
	int flag = -1;
	for (int i  = 0; i < INTARRAYLEAFSIZE-1; i ++) {
		if (cur->keyArray[i] < target && cur->keyArray[i] != NULL) {
			tempKey[i] = cur->keyArray[i];
			temprid[i] = cur->ridArray[i];
		} else if (flag == -1) {
			pos = i;
			flag = 0;
		}
		if (cur->keyArray[i] > target) {
			tempKey[i+i] = cur->keyArray[i];
			temprid[i+1] = cur->ridArray[i];
		}
	}

	// add target key and id
	tempKey[pos] = target;
	temprid[pos] = id;

	// set cur->arrays to temparrays
	for (int i  = 0; i < INTARRAYLEAFSIZE; i ++) {
		cur->keyArray[i] = tempKey[i];
		cur->ridArray[i] = temprid[i];

	}
}
	
// -----------------------------------------------------------------------------
// BTreeIndex::treeInsertNode
//------------------------------------------------------------------------------
NonLeafNodeInt* BTreeIndex::treeInsertNode(Page current, int target, int level, RecordId id) {
	// if its a non leaf node
	if (level != 1) {
		// cast to non leaf node
		NonLeafNodeInt* cur = reinterpret_cast<NonLeafNodeInt*>(&current);
		NonLeafNodeInt* tempNode;

		int flag = -1;

		//loop to find the next node
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
			int curkey = cur->keyArray[i];
			// if the target is less than the current key
			if (target < curkey) {
				// recursive case # 1
				flag = 1;
				Page* curPage = nullptr; 
				bufMgr->readPage(file, cur->pageNoArray[i], curPage);
				tempNode = treeInsertNode(*curPage, target, cur->level, id);
				break;
			}
		}
		// recursive case #2
		if (flag == -1) {
			Page* curPage = nullptr; 
			bufMgr->readPage(file, cur->pageNoArray[INTARRAYNONLEAFSIZE], curPage);
			tempNode = treeInsertNode(*curPage, target, cur->level, id);
		}
		
		// if nothing needs to be changed
		if (tempNode == nullptr) {
			return nullptr;
		}
		
		// if we need to add a nodes info to this node

		// check if there is space
		// geniune question: what if the key array is full but not the pageNo one
		int space = 0;
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
			if (cur->keyArray[i] == NULL) {
				space = 1;
				break;
			}
		}

		// if there is space
		if (space) {
			// insert the tempNode information into the current node
			insertIntoNonLeaf(tempNode, cur);

			return nullptr;
		
		} else {
			Page* returnedPage = nullptr;
			PageId nextPageId = nextPageID;
			bufMgr->allocPage(file, nextPageId, returnedPage);
			bufMgr->unPinPage(file, nextPageId, true);
			// dont increment nextPageID because this node/page is only temporary
			NonLeafNodeInt* returnedNode = reinterpret_cast<NonLeafNodeInt*>(&returnedPage);
			
			splitNonLeaf(cur, tempNode, returnedNode);

			return returnedNode;
		}


	} else {
		// cast to leafNode
		LeafNodeInt* cur = reinterpret_cast<LeafNodeInt*>(&current);

		// check if there is space in arrays
		int space = 0;
		for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
			if (cur->keyArray[i] == NULL) {
				space = 1;
				break;
			}
		}

		// if there is space
		if (space) {
			// insert key and rid into leafNode
			insertIntoLeaf(cur, target, id);

			// return null if no propogation or splitting is needed
			return NULL;

		// hard case
		} else { // arrays need to be split

			// create tempNode to pass up
			Page* tempPage = nullptr;
			PageId nextPageId = nextPageID;
			bufMgr->allocPage(file, nextPageId, tempPage);
			bufMgr->unPinPage(file, nextPageId, true);
			// dont increment nextPageID because this node/page is only temporary
			NonLeafNodeInt* tempNode = reinterpret_cast<NonLeafNodeInt*>(&tempPage);

			splitLeafNode(cur, tempNode, target, id);

			return tempNode;
		}
	}
}	
	

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{
	// cast key to an int	
	int keyInt = *(reinterpret_cast<int*>(&key));

	// get root
	Page* rootPage = nullptr;
	bufMgr->readPage(file, rootPageNum, rootPage);
	bufMgr->unPinPage(file, rootPageNum, true);

	NonLeafNodeInt* root = reinterpret_cast<NonLeafNodeInt*>(&rootPage);

	// check if roots keyArray is empty
	int isEmpty = 1;
	for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
		if (root->keyArray[i] != NULL) {
			isEmpty = 0; // root is not empty
			break;
		}
	}

	/*	Root Case	*/
	if (isEmpty) {
		
		// create a leafnode
		Page* leafPage = nullptr;
		PageId nextPageId = nextPageID;
		bufMgr->allocPage(file, nextPageId, leafPage);
		bufMgr->unPinPage(file, nextPageId, true);
		nextPageID+=1;

		LeafNodeInt* leaf = reinterpret_cast<LeafNodeInt*>(&leafPage);

		//insert key and rid into leaf node
		leaf->keyArray[0] = keyInt;
		leaf->ridArray[0] = rid;

		// in root node, set pageNoArray at [1] to the leafNode
		root->pageNoArray[1] = leafPage->page_number();
		// set keyArray at [0] to key
		root->keyArray[0] = keyInt;
	}


	/*	Noraml case	*/
	else {
		// insert element in tree with recursion!
		NonLeafNodeInt* node = treeInsertNode(*rootPage, keyInt, 0, rid);

		// check if the returned node is = to nullptr
		if (node != nullptr) {
			// set root node to returned node
			Page* newRoot = reinterpret_cast<Page*>(&node);

			// someone should double check this
			rootPageNum = newRoot->page_number();

		}
		
		
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)  
{
    	// check type ids
    	if (lowOpParm != 0 && lowOpParm != 1) {
        	throw BadOpcodesException();
	} 
    	if (highOpParm != 3 && highOpParm != 4) {
       	 	throw BadOpcodesException();
    	}

	// end scan if one if already going on
	if (scanExecuting) {
		endScan();
	}

	// set scanExecuting to true
	scanExecuting = true;

	// get root page to start
	Page* root = nullptr;
	bufMgr->readPage(file, rootPageNum, root);
	bufMgr->unPinPage(file, rootPageNum, root);

	// can we assume low and highValParm will always point to ints?
	int localLow = *(reinterpret_cast<int*>(&lowValParm));
	// int localLow = *((int *)(lowValParm));
	int localHigh = *(reinterpret_cast<int*>(&highValParm));

    // if low param is greater than high param throw error
    if (localHigh > localLow) {
       	throw BadScanrangeException();
    }

	// change what the values will be based on their operators
	if (lowOpParm == 0) {
		localLow += 1;
	}
	if (highOpParm == 3) {
		localHigh -= 1;
	}

	// set scan variables
	lowValInt = localLow;
	highValInt = localHigh;

	// find the first leaf node
	// not sure how to tell if root is a leaf or not
	LeafNodeInt leaf;
	try {
		// if it is not the leaf
		leaf = traverseTree(*root, lowValInt, 0);
	} catch (BadgerDbException e) {
		// if it is a leaf
		leaf = traverseTree(*root, lowValInt, 1);
	}
	 Page* leafPage = reinterpret_cast<Page*>(&leaf);
	 // pin the page? is there a better way?
	 bufMgr->readPage(file, leafPage->page_number(), leafPage);

	 // is this all or should I also set next Entry

}	


// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid){
	//check if scan is in proccess
	if(scanExecuting == false){
		throw ScanNotInitializedException();
	}
	//look at current page
	LeafNodeInt *node = (LeafNodeInt *) currentPageData;
	outRid = node->ridArray[nextEntry];
	// if rid is a valid key
	if (nextEntry == INTARRAYLEAFSIZE || outRid.page_number == 0) {
		//read page and unpin
		nextEntry = 0;
		bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = node->rightSibPageNo;
  		bufMgr->readPage(file, currentPageNum, currentPageData);
 		node = (LeafNodeInt *) currentPageData;
	}
	//if the scan is complete
	if(nextEntry == -1) {
		throw IndexScanCompletedException();
	}
	else{
		nextEntry++;
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
void BTreeIndex::endScan(){
	//stops scan and  throws error if startScan has not been called
	if(!scanExecuting){
		throw ScanNotInitializedException();
	}
	scanExecuting = false;//end the scan
	//unpin the pages for the scan
	bufMgr->unPinPage(file, currentPageNum, false);
}

}
