#ifndef DBFILE_H
#define DBFILE_H
//#include "GenericDBFile.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"
typedef enum {heap, sorted, tree, invalid} fType;
enum {DB_INSUFFICIENT_MEMORY=0,DB_UNSUPPORTED_TYPE=2,DB_CREATE_SUCCESS=1} ; 

class GenericDBFile{
    public:
        GenericDBFile();
        virtual ~GenericDBFile(){};
        virtual int Close(){};
        virtual void Add(Record &){};
        virtual int Open (char *){};
        virtual int GetNext(Record&, CNF&, Record&){};
        virtual void Load (Schema &, char *){};
        virtual void MoveFirst (){};
        virtual int GetNext (Record &fetchme){};
	virtual int Create (char *, fType , void *){};
	virtual void setSort(SortInfo *){};
};
class Heap:public GenericDBFile{
    private:
        File *fileP;
        Page *pageP;
        char* path;
        Page *tempPage;
        bool isDirtyPage;
        off_t pageOffset;
        off_t recOffset;
        off_t endPageOffset;
    public:
        Heap();
        virtual ~Heap();
        int Close();
        void Add(Record &);
        int GetNext(Record&, CNF&, Record&);
        int Create (char *, fType , void *);
        int Open (char *);
        void Load (Schema &, char *);
        void MoveFirst ();
        int GetNext (Record &);
	
};

class Sorted:public GenericDBFile{
    private:
        File *fileP;
        Page *pageP;
        char* path;
        Page *tempPage;
        bool isDirtyPage;
        off_t pageOffset;
        off_t recOffset;
        off_t endPageOffset;
        SortInfo* fileOrder;
        BigQ* diffFile;
        bool isWrite;
        Pipe* input;
        Pipe* output;
        bool isCalcQMaker;
        int Merge();

    public:
        Sorted();
        virtual ~Sorted();
        int Close();
        void Add(Record &);
        int GetNext(Record&, CNF&, Record&);
        int Create (char *, fType , void *);
        int Open (char *);
        void Load (Schema &, char *);
        void MoveFirst ();
        int GetNext (Record &);
	void setSort(SortInfo *);

};

class DBFile {
    private:
        string mergePath;
        GenericDBFile* myInternalVar; 
        /*File *fileP;
        Page *pageP;
        char* path;
        Page *tempPage;
        bool isDirtyPage;
        off_t pageOffset;
        off_t recOffset;
        off_t endPageOffset;
*/
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
        int MakeDbHeader(char*,fType,void *);
        fType readHeader(char*,SortInfo *);

};
#endif
