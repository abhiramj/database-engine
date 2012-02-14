#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;
enum {DB_INSUFFICIENT_MEMORY=0,DB_UNSUPPORTED_TYPE=2,DB_CREATE_SUCCESS=1} ; 
// stub DBFile header..replace it with your own DBFile.h 

class DBFile {
    private: 
        File *fileP;
        int header;
        char* path;
        Page *pageP;
        Page *tempPage;
        bool isDirtyPage;
        off_t pageOffset;
        off_t recOffset;
        off_t endPageOffset;
public:
	DBFile (); 

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
        int MakeDbHeader(char*);

};
#endif
