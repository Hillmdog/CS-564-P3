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
		scanExecuting = false;
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
		scanExecuting = false;
	}
}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{ 
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
	delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::traverseTreeNonLeafNode
//------------------------------------------------------------------------------

NonLeafNodeInt traverseTreeNonLeafNode (page current, int target) {
    // cast current to nonLeafNode
    NonLeafNodeInt cur = reinterpret_cast<*NonLeafNodeInt>(&current);

    // base case
    // returns parent to target leaf
    if (cur->level == 1) {
        return cur; // or current which ever is more useful we can change this
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
    page current = -1;
    // not sure if this should be * or not
    bufMgr->readPage(*file, rootPageNum, current);     

{

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid){
	if(scanExecuting == false){
		throw ScanNotInitializedException();
	}
	if(nextEntry == -1) {
		throw new IndexScanCompletedException();
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
