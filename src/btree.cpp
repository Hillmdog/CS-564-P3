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
		BTreeIndex::file = &BlobFile::create(outIndexName);
		// initialize metadata
		BTreeIndex::bufMgr = bufMgrIn;
		BTreeIndex::headerPageNum = 1;
		BTreeIndex::rootPageNum = 2;
		BTreeIndex::attributeType = attrType;
		BTreeIndex::attrByteOffset = attrByteOffset;
		// create header page
		const IndexMetaInfo btreeHeader = {outIndexName[0], attrByteOffset, attrType, 2};
		Page headerPage = *(reinterpret_cast<const Page*>(&btreeHeader));
		Page* headerPage_ptr;
		bufMgr->allocPage(file, headerPageNum, headerPage_ptr); // file->writePage(headerPageNum, headerPage);
		headerPage_ptr = &headerPage;
		bufMgr->unPinPage(file, headerPageNum, true);
		// insert entries for every tuple in the base relation using FileScan class
		FileScan fileScanner = FileScan(outIndexName, bufMgrIn);
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
		BTreeIndex::file = &BlobFile::open(outIndexName);
		// check if the metadata in header matches with the given values
		BTreeIndex::bufMgr = bufMgrIn;
		BTreeIndex::headerPageNum = file->getFirstPageNo(); // or 1
		BTreeIndex::attributeType = attrType;
		BTreeIndex::attrByteOffset = attrByteOffset;
		Page* headerPage;
		bufMgr->readPage(file, headerPageNum, headerPage); // Page headerPage = (*file).readPage(headerPageNum);
		bufMgr->unPinPage(file, headerPageNum, false);
		// convert char[](Page) to struct 
		const IndexMetaInfo* btreeHeader = reinterpret_cast<const IndexMetaInfo*>(&headerPage);
		// if not match, throw BadIndexInfoException
		if (btreeHeader->relationName != relationName || btreeHeader->attrByteOffset != attrByteOffset 
				|| btreeHeader->attrType != attrType) {
			throw new BadIndexInfoException(outIndexName);
		}
		BTreeIndex::rootPageNum = btreeHeader->rootPageNo;
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
			Page* nextPage = nullptr; 
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

void splitNonLeaf(NonLeafNodeInt* oldNode, NonLeafNodeInt* newNode, NonLeafNodeInt* tempNode) {
	// create new page
	// unpin page and increment nextPageID

	// cast page to node

	// set all values in arrays of newNode to null

	//calc halfindex
	//get middlekey
	// get middlekeys pageNos

	//populate new nodes arrays with the old stuff except for the middle key and its pageNos
	// challenge here is fitting new stuff in and not having a duplicate pointer

	// insert extracted target key and pagenos into correct node
	// if statement for old node
	// if statement for new node

	// create tempnode to pass up and give it the middlekey and pagenos

}

void insertIntoNonLeaf(NonLeafNodeInt* tempNode, NonLeafNodeInt* cur) {
	// extract middle key and pageNos from tempNode
	int middleKey = tempNode->keyArray[0];
	PageId page1 = tempNode->pageNoArray[0];
	PageId page2 = tempNode->pageNoArray[1];

	int tempKey[INTARRAYLEAFSIZE];
	PageId temppid[INTARRAYLEAFSIZE+1];
	// add them into the current node
	
	// find pos first
	int pos = -1;
	for (int i = 0; i < INTARRAYLEAFNONSIZE; i++) {
		if (cur->keyArray[i] > middlekey) {
			pos = i;
			break
		}
	}

	for (int i = 0; i < INTARRAYLEAFNONSIZE-1; i++) {
		if ( i < pos) {
			tempkey[i] = cur->keyArray[i];
		} else {
			tempkey[i+1] = cur->keyArray[i];
		}
	}

	// set middlekey
	tempkey[pos] = middleKey;

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
	tempKey[pos] = middleKey;
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
void splitLeafNode(LeafNodeInt* cur, NonLeafNodeInt* tempNode){
	// create new node and page
	Page* newPage = nullptr;
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

void insertIntoLeaf(LeafNodeInt* cur) {
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
NonLeafNodeInt* BTreeIndex::treeInsertNode(Page current, int target, int level, RecordId& id) {
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
			bufMgr->allocPage(file, nextPageID, returnedPage);
			bufMgr->unpinPage(file, nextPageID, true);
			// dont increment nextPageID because this node/page is only temporary
			NonLeafNodeInt* returnedNode = reinterpret_cast<NonLeafNodeInt*>(&returnedPage);
			
			splitNonLeaf(cur, tempNode, returnedNode);

			return returnedPage;
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
			insertIntoLeaf(cur);

			// return null if no propogation or splitting is needed
			return NULL;

		// hard case
		} else { // arrays need to be split

			// create tempNode to pass up
			Page* tempPage = nullptr;
			bufMgr->allocPage(file, nextPageID, tempPage);
			bufMgr->unpinPage(file, nextPageID, true);
			// dont increment nextPageID because this node/page is only temporary
			NonLeafNodeInt* tempNode = reinterpret_cast<NonLeafNodeInt*>(&tempPage);

			splitLeafNode(cur, tempNode);

			return tempNode;
		}
	}
}	
	

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

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
	if(scanExecuting == false){
		throw ScanNotInitializedException();
	}
	LeafNodeInt *node = (LeafNodeInt *) currentPageData;
	outRid = node->ridArray[nextEntry];
	if (nextEntry == INTARRAYLEAFSIZE || outRid.page_number == 0) {
		nextEntry = 0;
		bufMgr->unPinPage(file, currentPageNum, false);
		currentPageNum = node->rightSibPageNo;
  		bufMgr->readPage(file, currentPageNum, currentPageData);
 		node = (LeafNodeInt *) currentPageData;
	}	
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
	//stops the current scan and  throws a ScanNotInitializedException if triggered before a succesful startScan call
	if(!scanExecuting){
		throw ScanNotInitializedException();
	}
	scanExecuting = false;//end the scan
	//unpin the pages for the scan
	bufMgr->unPinPage(file, currentPageNum, false);
}

}
