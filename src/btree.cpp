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
	std::ostringstream idxStr;
	idxStr << relationName << "." << attrByteOffset;
	outIndexName = idxStr.str();
	// check if the index file exists
	if (!File::exists(outIndexName)) {
		// create the index file if not already exists
		std::shared_ptr<BlobFile> blobFile; // initialized to nullptr
		blobFile = std::make_shared<BlobFile>(BlobFile::create(outIndexName));
		
		// initialize metadata
		BTreeIndex::file = blobFile.get();
		BTreeIndex::bufMgr = bufMgrIn;
		BTreeIndex::attributeType = attrType;
		BTreeIndex::attrByteOffset = attrByteOffset;

		// create header page
		Page* headerPage;
		bufMgr->allocPage(file, headerPageNum, headerPage); // file->writePage(headerPageNum, headerPage);
		IndexMetaInfo* headerInfo = (IndexMetaInfo *)headerPage;
		strcpy(headerInfo->relationName, relationName.c_str());
		headerInfo->attrByteOffset = attrByteOffset;
		headerInfo->attrType = attrType;
		// const IndexMetaInfo btreeHeader = {outIndexName[0], attrByteOffset, attrType, 2};
		// Page headerPage = *(reinterpret_cast<const Page*>(&btreeHeader));
		bufMgr->unPinPage(file, headerPageNum, true);

		// create root page
		Page* rootPage;
		bufMgr->allocPage(file, rootPageNum, rootPage);
		((NonLeafNodeInt*)rootPage)->level=1;
		// set empty nonleaf's keys and pageNums all to NULL 
		for (int i=0; i < INTARRAYNONLEAFSIZE; i++) {
			((NonLeafNodeInt*)rootPage)->keyArray[i] = MYNULL;
			((NonLeafNodeInt*)rootPage)->pageNoArray[i] = Page::INVALID_NUMBER;
		}
		((NonLeafNodeInt*)rootPage)->pageNoArray[INTARRAYNONLEAFSIZE] = Page::INVALID_NUMBER;
		// for debugging
		NonLeafNodeInt* rootInfo = (NonLeafNodeInt*)rootPage;
		bufMgr->unPinPage(file, rootPageNum, true);

		headerInfo->rootPageNo = BTreeIndex::rootPageNum; 

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
				std::cout << *((int*)key) << std::endl;
				if (*((int*)key) == -320) {
					std::cout << "STOP" << std::endl;
				}
				insertEntry(key, rid);
			}
		} catch(EndOfFileException e) {
			// end of file scan
		}
		// fileScanner.~FileScan();
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
		IndexMetaInfo* headerInfo = (IndexMetaInfo*)(headerPage);
		// if not match, throw BadIndexInfoException
		if (headerInfo->relationName != relationName || headerInfo->attrByteOffset != attrByteOffset 
				|| headerInfo->attrType != attrType) {
			throw new BadIndexInfoException(outIndexName);
		}
		BTreeIndex::rootPageNum = headerInfo->rootPageNo;
	}
	scanExecuting = false;
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

LeafNodeInt* BTreeIndex::traverseTree (Page current, int target, int level) {
    if (level != 1) {
		// cast page to nonLeafNode
		NonLeafNodeInt* cur = reinterpret_cast<NonLeafNodeInt*>(&current);
		LeafNodeInt* next;
		int flag = 0;
		int pos = INTARRAYNONLEAFSIZE-1;
		// search for next node
		for (int i = 0; i < INTARRAYNONLEAFSIZE-1; i++) {
			int curkey = cur->keyArray[i];
			// if the target is less than the current key
			if (curkey == MYNULL || target < curkey) {
				// recursive case #1
				flag = 1;
				pos = i;
				Page* nextPage = nullptr; 
				bufMgr->readPage(file, cur->pageNoArray[i], nextPage);
				bufMgr->unPinPage(file, cur->pageNoArray[i], false);
				next = traverseTree(*nextPage, target, cur->level);
				break;
			}

		}
		// enter the node at the right end
		if (flag == 0) {
			Page* nextPage;
			bufMgr->readPage(file, cur->pageNoArray[pos], nextPage);
			bufMgr->unPinPage(file, cur->pageNoArray[pos], false);
			next = traverseTree(*nextPage, target, cur->level);
		}
		return next;

	} else {
		// nonleaf nodes directly above leaf!
		// cast page to nonLeafNode
		NonLeafNodeInt* cur = reinterpret_cast<NonLeafNodeInt*>(&current);
		LeafNodeInt* leaf;
		PageId leafPid;
		int flag = 0;
		int pos = INTARRAYNONLEAFSIZE-1;
		// search for next node
		for (int i = 0; i < INTARRAYNONLEAFSIZE-1; i++) {
			int curkey = cur->keyArray[i];
			// if the target is less than the current key
			if (curkey == MYNULL || target < curkey) {
				// recursive case #1
				flag = 1;
				pos = i;
				Page* leafPage = nullptr; 
				bufMgr->readPage(file, cur->pageNoArray[i], leafPage);
				bufMgr->unPinPage(file, cur->pageNoArray[i], false);
				leaf = (LeafNodeInt*) leafPage;
				leafPid = cur->pageNoArray[i];
				break;
			}

		}
		// enter the node at the right end
		if (flag == 0) {
			Page* leafPage;
			bufMgr->readPage(file, cur->pageNoArray[pos], leafPage);
			bufMgr->unPinPage(file, cur->pageNoArray[pos], false);
			leaf = (LeafNodeInt*) leafPage;
			leafPid = cur->pageNoArray[pos];
		}

		// check if the target is in this node
		for (int i = 0; i < INTARRAYLEAFSIZE; i ++) {
			if (leaf->keyArray[i] != MYNULL && leaf->keyArray[i] >= target) {
				currentPageNum = leafPid;
				return leaf;
			}
		}
		// did not find the target
		currentPageNum = Page::INVALID_NUMBER;
		LeafNodeInt* none = nullptr;
		return none;
	}

}

void BTreeIndex::splitNonLeaf(NonLeafNodeInt* oldNode, NonLeafNodeInt* tempNode, NonLeafNodeInt* returnedNode) {
	// extract info from tempNode
	int newKey = tempNode->keyArray[0];
	PageId page1 = tempNode->pageNoArray[0];
	PageId page2 = tempNode->pageNoArray[1];

	// create new page
	Page* newPage = nullptr;
	bufMgr->allocPage(file, nextPageID, newPage);
	// unpin page and increment nextPageID
	bufMgr->unPinPage(file, nextPageID, true);
	// nextPageID+=1;

	// cast page to node
	NonLeafNodeInt* newNode = (NonLeafNodeInt*)(newPage);

	// set all values in arrays of newNode to null
	for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
		newNode->keyArray[i] = MYNULL;
	}

	//calc halfindex
	int halfindex = INTARRAYNONLEAFSIZE/2;
	//get middlekey
	int middleKey = oldNode->keyArray[halfindex];
	// get page nos
	Page* oldPage = (Page*)(oldNode);
	PageId nextPage1 = oldPage->page_number();
	PageId nextPage2 = newPage->page_number();

	// remove middlekey and pageNos
	for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
		if (oldNode->keyArray[i] > middleKey) {
			oldNode->keyArray[i-1] = oldNode->keyArray[i];
		}
	}
	oldNode->keyArray[INTARRAYNONLEAFSIZE-1] = MYNULL;

	//redistribute array values to each node
	for (int i = halfindex; i < INTARRAYNONLEAFSIZE; i++) {
		newNode->keyArray[i-halfindex] = oldNode->keyArray[i];

		// remove old values
		oldNode->keyArray[i] = MYNULL;
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
			if (oldNode->keyArray[i] > newKey || oldNode->keyArray[i] == MYNULL) {
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
			if (newNode->keyArray[i] > newKey || newNode->keyArray[i] == MYNULL) {
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
	// extract middle key(target) and pageNos from tempNode
	int newKey = tempNode->keyArray[0];
	PageId page1 = tempNode->pageNoArray[0];
	PageId page2 = tempNode->pageNoArray[1];

	int tempKey[INTARRAYNONLEAFSIZE];
	PageId temppid[INTARRAYNONLEAFSIZE+1];
	// add them into the current node
	
	// find pos first
	int pos = INTARRAYNONLEAFSIZE-1;
	// if target < all keys
	// TODO insert to the left end

	for (int i = 0; i < INTARRAYNONLEAFSIZE-1; i++) {
		if (cur->keyArray[i] == MYNULL || cur->keyArray[i] > newKey) {
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

	// loop for pageNoArray
	for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
		if ( i < pos) {
			temppid[i] = cur->pageNoArray[i];
		} else if (i > pos) {
			temppid[i+1] = cur->pageNoArray[i];
		}
	}

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
void BTreeIndex::splitLeafNode(LeafNodeInt* cur, PageId curPid, NonLeafNodeInt*& tempNode, int target, RecordId rid){
	// create new node and page
	Page* newPage = nullptr;
	PageId newPid;
	bufMgr->allocPage(file, newPid, newPage);
	bufMgr->unPinPage(file, newPid, true); // unpinpage
	LeafNodeInt* newNode = (LeafNodeInt*)(newPage);
	// nextPageID += 1;

	// set all values of newNodes arrays to null
	for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
		newNode->keyArray[i] = MYNULL;
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
		cur->keyArray[i] = MYNULL;
		// cur->ridArray[i] = NULL;
	}

	// insert target key and rid into either the old node or the new one
	// create temp new arrays
	int tempKey[INTARRAYLEAFSIZE];
	RecordId temprid[INTARRAYLEAFSIZE];

	// if the target is going into the old node
	if (target < middleKey) {
		int pos = INTARRAYLEAFSIZE-1;
		int flag = -1;
		for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
			if (cur->keyArray[i] < target && cur->keyArray[i] != MYNULL) {
				tempKey[i] = cur->keyArray[i];
				temprid[i] = cur->ridArray[i];
			} else if (flag == -1) {
				pos = i;
				flag = 0;
				break;
			}
		}

		// add target key and rid and what's left in the origional keyarray and ridarray
		tempKey[pos] = target;
		temprid[pos] = rid;
		for (int i = pos+1; i < INTARRAYLEAFSIZE; i++) {
			tempKey[i] = cur->keyArray[i-1];
			temprid[i] = cur->ridArray[i-1];
		}
		// set cur->arrays to temparrays
		for (int i  = 0; i < INTARRAYLEAFSIZE; i ++) {
			cur->keyArray[i] = tempKey[i];
			cur->ridArray[i] = temprid[i];
		}

	// if target goes into new node							
	} else {
		int pos = INTARRAYLEAFSIZE-1;
		int flag = -1;
		for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
			if (newNode->keyArray[i] < target && newNode->keyArray[i] != MYNULL) {
				tempKey[i] = newNode->keyArray[i];
				temprid[i] = newNode->ridArray[i];
			} else if (flag == -1) {
				pos = i;
				flag = 0;
				break;
			}
		}
		// add target key and rid and what's left in the origional keyarray and ridarray
		tempKey[pos] = target;
		temprid[pos] = rid;
		for (int i = pos+1; i < INTARRAYLEAFSIZE; i++) {
			tempKey[i] = newNode->keyArray[i-1];
			temprid[i] = newNode->ridArray[i-1];
		}
		// set cur->arrays to temparrays
		for (int i  = 0; i < INTARRAYLEAFSIZE; i ++) {
			newNode->keyArray[i] = tempKey[i];
			newNode->ridArray[i] = temprid[i];
		}		
	}

	// fix the linked list
	newNode->rightSibPageNo = cur->rightSibPageNo;
	cur->rightSibPageNo = newPid;

	// set tempNodes info
	tempNode->level = 1;
	// need help with this
	tempNode->keyArray[0] = middleKey;
	// Page* oldPage = reinterpret_cast<Page*>(&cur);
	Page* oldPage = (Page*)(cur);
	// Page* newPage = reinterpret_cast<Page*>(newNode);
	tempNode->pageNoArray[0] = curPid;// old node
	tempNode->pageNoArray[1] = newPid;// new node		
}

void insertIntoLeaf(LeafNodeInt* cur, int target, RecordId rid) {
	// create temp new arrays
	int tempKey[INTARRAYLEAFSIZE];
	RecordId temprid[INTARRAYLEAFSIZE];

	// find pos for target key and rid to fit into
	int pos = INTARRAYLEAFSIZE-1;
	int flag = -1;
	// if target < all keys
	// TODO insert to the left end

	for (int i  = 0; i < INTARRAYLEAFSIZE-1; i ++) {
		if (cur->keyArray[i] != MYNULL && cur->keyArray[i] < target) {
			tempKey[i] = cur->keyArray[i];
			temprid[i] = cur->ridArray[i];
		} else if (flag == -1) {
			pos = i;
			flag = 0;
			break;
		}
	}

	// add target key and rid and what's left in the origional keyarray and ridarray
	tempKey[pos] = target;
	temprid[pos] = rid;
	for (int i  = pos+1; i < INTARRAYLEAFSIZE; i++) {
		tempKey[i] = cur->keyArray[i-1];
		temprid[i] = cur->ridArray[i-1];
	}

	// set cur->arrays to temparrays
	for (int i  = 0; i < INTARRAYLEAFSIZE; i ++) {
		cur->keyArray[i] = tempKey[i];
		cur->ridArray[i] = temprid[i];

	}
}
	
// -----------------------------------------------------------------------------
// BTreeIndex::treeInsertNode
//------------------------------------------------------------------------------
NonLeafNodeInt* BTreeIndex::treeInsertNode(Page current, PageId curPid, int target, int level, RecordId rid) {
	// if its not parent of leaf node
	if (level != 1) {
		// cast to non leaf node
		NonLeafNodeInt* cur = reinterpret_cast<NonLeafNodeInt*>(&current);
		NonLeafNodeInt* tempNode;

		int flag = -1;
		int keymatched;

		//loop to find the next node
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
			int curkey = cur->keyArray[i];
			if (curkey == MYNULL) {
				keymatched = i;
				break;
			}
			// else if the target is less than the current key
			 else if (target < curkey) {
				// recursive case # 1
				flag = 1;
				Page* nextPage = nullptr; 
				bufMgr->readPage(file, cur->pageNoArray[i], nextPage);
				tempNode = treeInsertNode(*nextPage, cur->pageNoArray[i], target, cur->level, rid);
				break;
			}
		}
		// recursive case #2
		if (flag == -1) {
			Page* nextPage = nullptr; 
			bufMgr->readPage(file, cur->pageNoArray[keymatched], nextPage);
			tempNode = treeInsertNode(*nextPage, cur->pageNoArray[keymatched], target, cur->level, rid);
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
			if (cur->keyArray[i] == MYNULL) {
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
			bufMgr->unPinPage(file, nextPageID, true);
			// dont increment nextPageID because this node/page is only temporary
			NonLeafNodeInt* returnedNode = (NonLeafNodeInt*)(returnedPage);
			
			splitNonLeaf(cur, tempNode, returnedNode);

			return returnedNode;
		}


	// if it's a non leaf node whose children are leaf nodes
	} else {
		NonLeafNodeInt* cur = reinterpret_cast<NonLeafNodeInt*>(&current);
		LeafNodeInt* leafNode;
		PageId leafPid;

		int flag = -1;
		int keymatched;

		//loop to find the next node
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
			int curkey = cur->keyArray[i];
			if (curkey == MYNULL) {
				keymatched = i;
				break;
			}
			// else if the target is less than the current key
			 else if (target < curkey) {
				// recursive case # 1
				flag = 1;
				Page* nextPage = nullptr; 
				bufMgr->readPage(file, cur->pageNoArray[i], nextPage);
				leafNode = (LeafNodeInt*)nextPage;
				leafPid = cur->pageNoArray[i];
				break;
			}
		}
		// recursive case #2
		if (flag == -1) {
			Page* nextPage = nullptr; 
			bufMgr->readPage(file, cur->pageNoArray[keymatched], nextPage);
			leafNode = (LeafNodeInt*)nextPage;
			leafPid = cur->pageNoArray[keymatched];
		}

		// check if there is space in arrays
		int space = 0;
		for (int i = 0; i < INTARRAYLEAFSIZE; i++) {
			if (leafNode->keyArray[i] == MYNULL) {
				space = 1;
				break;
			}
		}

		// if there is space
		if (space) {
			// insert key and rid into leafNode
			insertIntoLeaf(leafNode, target, rid);

			// return null if no propogation or splitting is needed
			return nullptr;

		// hard case
		} else { // arrays need to be split

			// create tempNode to pass up
			Page* tempPage = nullptr;
			bufMgr->allocPage(file, nextPageID, tempPage);
			bufMgr->unPinPage(file, nextPageID, true);
			// dont increment nextPageID because this node/page is only temporary
			NonLeafNodeInt* tempNode = (NonLeafNodeInt*)(tempPage);

			splitLeafNode(leafNode, leafPid, tempNode, target, rid);

			// if leafNode is directly under root, need to merge tempNode into cur root and return a new root
			if (curPid == rootPageNum) {
				int space_root = 0;
				for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
					if (cur->keyArray[i] == MYNULL) {
						space_root = 1;
						break;
					}
				}

				// if there is space
				if (space_root) {
					// insert the tempNode information into the current node
					insertIntoNonLeaf(tempNode, cur);

					return nullptr;
				
				} else {
					Page* returnedPage = nullptr;
					bufMgr->allocPage(file, nextPageID, returnedPage);
					bufMgr->unPinPage(file, nextPageID, true);
					// dont increment nextPageID because this node/page is only temporary
					NonLeafNodeInt* returnedNode = (NonLeafNodeInt*)(returnedPage);
					
					splitNonLeaf(cur, tempNode, returnedNode);

					return returnedNode;
				}
			}
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
	// int keyInt = *(reinterpret_cast<int*>(&key));
	int keyInt = *((int*)key);

	// get root
	Page* rootPage = nullptr;
	bufMgr->readPage(file, rootPageNum, rootPage);
	bufMgr->unPinPage(file, rootPageNum, true);

	// NonLeafNodeInt* root = reinterpret_cast<NonLeafNodeInt*>(&rootPage);
	NonLeafNodeInt* root = (NonLeafNodeInt*)(rootPage);

	// check if roots keyArray is empty
	int isEmpty = 1;
	for (int i = 0; i < INTARRAYNONLEAFSIZE; i++) {
		if (root->keyArray[i] != MYNULL) {
			isEmpty = 0; // root is not empty
			break;
		}
	}

	/*	Root Case	*/
	if (isEmpty) {
		
		// create a leafnode
		Page* leafPage = nullptr;
		bufMgr->allocPage(file, nextPageID, leafPage);
		// std::cout<< leafPage->page_number() << std::endl;
		bufMgr->unPinPage(file, nextPageID, true);

		// in root node, set pageNoArray at [1] to the leafNode
		root->pageNoArray[1] = nextPageID;
		// set keyArray at [0] to key
		root->keyArray[0] = keyInt;
		
		// LeafNodeInt* leaf = reinterpret_cast<LeafNodeInt*>(&leafPage);
		LeafNodeInt* leaf = (LeafNodeInt*)(leafPage);

		// set empty leaf's keys all to MYNULL 
		for (int i=0; i < INTARRAYLEAFSIZE; i++) {
			leaf->keyArray[i] = MYNULL;
		}
		//insert key and rid into leaf node
		leaf->keyArray[0] = keyInt;
		leaf->ridArray[0] = rid;
	}


	/*	Noraml case	*/
	else {
		// insert element in tree with recursion!
		NonLeafNodeInt* node = treeInsertNode(*rootPage, rootPageNum, keyInt, root->level, rid);

		// check if the returned node is = to nullptr
		if (node != nullptr) {
			// set root node to returned node

			Page* newRoot = (Page*)(node);

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
    	if (lowOpParm != 2 && lowOpParm != 3) {
        	throw BadOpcodesException();
	} 
    	if (highOpParm != 0 && highOpParm != 1) {
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
	NonLeafNodeInt* rootNode = (NonLeafNodeInt*)root;

	// can we assume low and highValParm will always point to ints?
	int localLow = *((int*)(lowValParm));
	// int localLow = *((int *)(lowValParm));
	int localHigh = *((int*)(highValParm));

    // if low param is greater than high param throw error
    if (localHigh > localLow) {
       	throw BadScanrangeException();
    }

	// change what the values will be based on their operators
	// change < to <= and > to >=
	if (highOpParm == 0) {
		localHigh -= 1;
	}
	if (lowOpParm == 3) {
		localLow += 1;
	}

	// set scan variables
	lowValInt = localLow;
	highValInt = localHigh;

	// find the first leaf node
	// not sure how to tell if root is a leaf or not
	LeafNodeInt* leaf;
	// try {
	// 	// if it is not the leaf
	// 	leaf = traverseTree(*root, lowValInt, rootNode->level);
	// } catch (BadgerDbException e) {
	// 	// if it is a leaf
	// 	// leaf = traverseTree(*root, lowValInt, 1);
	// }
	leaf = traverseTree(*root, lowValInt, rootNode->level);
	if (leaf == nullptr) {
		currentPageData = nullptr;
		return;
	}
	currentPageData = (Page*)(leaf);
	// pin the page? is there a better way?
	// Re: do it in scanNext()
	//  bufMgr->readPage(file, leafPage->page_number(), leafPage);

	// is this all or should I also set next Entry
	// Re: set it in scanNext()

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
