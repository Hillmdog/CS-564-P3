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
	scanExecuting = false;
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
// BTreeIndex::traverseTreeNonLeafNode
//------------------------------------------------------------------------------

NonLeafNodeInt traverseTreeNonLeafNode (Page current, int target) {
    // cast current to nonLeafNode
    NonLeafNodeInt* cur = reinterpret_cast<NonLeafNodeInt*>(&current);

    // base case
    // returns parent to target leaf
    if (cur->level == 1) {
        return *cur; // or current which ever is more useful we can change this
    }

    // loop to find next node
    for (int i = 0; i < sizeof(cur->keyArray); i++) {
        int curkey = cur->keyArray[i];
        
        // recursive case #1
        if (curkey > target) {
            return traverseTreeNonLeafNode(cur->pageNoArray[i], target);
        }
    }

    // recursize case #2
    return traverseTreeNonLeafNode(cur->pageNoArray.back(), target);
}


// -----------------------------------------------------------------------------
// BTreeIndex::treeInsertNode
//------------------------------------------------------------------------------
/*
TODO:
-Concerned about the root bieng created with null lists
-insert nonleafnode that gets propgated back up
-make sure all created nodes lists get set to null

*/
NonLeafNodeInt treeInsertNode(page current, int target, int level, RecordID& id) {
	// if its a non leaf node
	if (level == 0) {
		// cast to non leaf node
		NonLeafNodeInt cur = reinterpret_cast<NonLeafNodeInt*>(&current);
		NonLeafNodeInt newNode = NULL;

		int flag = -1;

		//loop to find the next node
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
			int curkey = keyArray[i];
			// if the target is less than the current key
			if (target < curkey) {
				// recursive case # 1
				flag = 1;
				newNode = treeInsertNode(cur->pageNoArray[i], target, cur->level, id);
				break;
			}
		}
		// recursive case #2
		if (flag == -1) {
			newNode = reeInsertNode(cur->pageNoArray[INTARRAYNONLEAFSIZE], target, cur->level, id);
		}
		
		// if nothing needs to be changed
		if (newNode == NULL) {
			return NULL;
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
			// extract middle key and pageNos

			// add them into the current node

			// return null;


	} else {
		// create new page
		// unpin page and increment nextPageID

		// cast page to node

		// set all values in arrays of newNode to null

		//calc halfindex
		//get middlekey
		// get middlekeys pageNos

		//populate new nodes arrays with the old stuff except for the middle key and its pageNos

		// insert extracted target key and pagenos into correct node
		// if statement for old node
		// if statement for new node

		// create new node to pass up and give it the middlekey and pagenos
		// return newNode;

	}


	} else {
		// cast to leafNode
		LeafNodeInt cur = reinterpret_cast<LeafNodeInt*>(&current);

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
			// create temp new arrays
			int tempKey[INTARRAYLEAFSIZE];
			RecordID temprid[INTARRAYLEAFSIZE];

			// find pos for target key and rid to fit into
			int pos = -1;
			int flag = -1;
			for (int i  = 0; i < INTARRAYLEAFSIZE-1; i ++) {
				if (cur->keyArray[i] < target && cur->keyArray[i] != NULL) {
					tempKey[i] = cur->keyArray[i];
					temprid[i] = cur->keyArray[i];
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
			tempkey[pos] = target;
			temprid[pos] = id;

			// return null if no propogation or splitting is needed
			return NULL;

		// hard case
		} else { // arrays need to be split
			// create new node and page
			Page newPage;
			bufMgr.allocPage(file, nextPageID, &newPage);
			bufMgr.unPinPage(file, nextPageID, &newPage); // unpinpage
			LeafNodeInt newNode = reinterpret_cast<LeafNodeInt*>(&newPage);
			nextPageID += 1;

			// set all values of newNodes arrays to null
			for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
				newNode->keyArray[i] = NULL;
				nwNode->ridArray[i] = NULL;
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
				cur->ridArray[i] = NULL;
			}

			// insert target key and rid into either the old node or the new one
			// create temp new arrays
			int tempKey[INTARRAYLEAFSIZE];
			RecordID temprid[INTARRAYLEAFSIZE];
			
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
				temprid[pos] = rid;
				cur->keyArray = tempKey;
				cur->ridArray = temprid;
			
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
						tempKey[i+1] = arr[i];
						temprid[i+1] = newNode->ridArray[i];
					}
				}

				tempKey[pos] = target;
				temprid[pos] = rid;
				newNode->keyArray = tempKey;
				newNode->ridArray = temprid;
				}

			// fix the linked list
			// fix the linked list
			newNode->rightSibPageNo = cur->rightSibPageNo;
			cur->rightSibPageNo = nextPageID;

			// create new nonLeafode and set middle key to it
			Page tempPage;
			bufMgr.allocPage(file, nextPageID, &tempPage);
			bufmgr.unPinPage(file, nextPageID, &tempPage); // unpin page
			NonLeafNodeInt tempNode = reintrepret_cast<NonLeafNodeInt*>(&newPage);
			// don't incrmenet the page no becasue this node is only temporary!
			//nextPageID+=1;

			// set its junk
			tempNode->level = 1;
			// need help with this
			tempNode->keyArray[0] = middleKey;
			tempNode->pageNoArray[0] = // old node
			tempNode->pageNoArray[1] = // new node

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
	// start with exception handling (need to probobly update what im passing to the exceptions)
    if (*lowOpParm > *highValParm) {
        throw BadScanrangeException();
    }
    // check type ids
    if (!typeid(lowOpParm).name() == "LT" && !typeid(lowOpParm).name() == "LTE") {
        throw BadOpcodesException();
    } 
    if (!typeid(highValParm).name() == "GT" && !typeid(highValParm).name() == "GTE") {
        throw BadOpcodeException();
    }
    
    scanExecuting = true;
    // get the root page
    Page* current = NULL;
    // not sure if this should be * or not
    bufMgr->readPage(file, rootPageNum, current);   
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid){
	if(scanExecuting == false){
		throw ScanNotInitializedException();
	}
	LeafNodeInt *node = (LeafNodeInt *) currentPageData;
	outRid = leafNode->ridArray[nextEntry];
	if (outRid.page_number == 0 || nextEntry == INTARRAYLEAFSIZE) {
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
	scanExecuting = false;//temp name
	//unpin the pages for the scan
	bufMgr->unPinPage(file, currentPageNum, false);
}

}
