#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <stdlib.h>
#include <string.h>
#include <string>
#include <assert.h>
#include <iostream>
#include <fstream>
using namespace std;
// stub file .. replace it with your own DBFile.cc
GenericDBFile::GenericDBFile(){

}
DBFile::DBFile () {
myInternalVar=NULL;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
    // Not implemented sorted or tree

    if (f_type == tree)
        return 0;
    
    //create header to store attrib
    int status=MakeDbHeader(f_path,f_type,startup);
    if (status == DB_INSUFFICIENT_MEMORY || status== DB_UNSUPPORTED_TYPE)
	return 0;
    if (f_type==sorted)
       myInternalVar= new Sorted(); 
    else if (f_type==heap)
        myInternalVar= new Heap();
    else return DB_UNSUPPORTED_TYPE;
    myInternalVar->Create(f_path,f_type, startup);
    return DB_CREATE_SUCCESS;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
	myInternalVar->Load(f_schema, loadpath);

}

int DBFile::Open (char *f_path) {
    SortInfo* newSort= new SortInfo;
    newSort->myOrder= new OrderMaker();
    fType file_type=readHeader(f_path,newSort);
    cout<<"Opened file type :  ";
    if (file_type==sorted){
        cout<<"Sorted"<<endl;
       myInternalVar= new Sorted();
	myInternalVar->setSort(newSort);
        newSort->myOrder->Print();
        }
    else if (file_type==heap){
        cout<<"Heap"<<endl;
        myInternalVar= new Heap();
    }
    else {
        cout<<"Cannot open invalid file. Check header file exits at path: "<<f_path<<endl;
        //exit(-1);
        return DB_UNSUPPORTED_TYPE;
    }
        return myInternalVar->Open(f_path);
}

void DBFile::MoveFirst () {
	myInternalVar->MoveFirst();
}

void DBFile::Add (Record &rec) {
    myInternalVar->Add(rec);
}
int DBFile::Close () {
    myInternalVar->Close();
}





int DBFile::GetNext (Record &fetchme) {
    myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    myInternalVar->GetNext(fetchme,cnf,literal);
}

int DBFile::MakeDbHeader(char* bin_path,fType f_type, void *startup){
    string oldString(bin_path);
    size_t lastpos=oldString.find_first_of('.');
#ifdef verbose
    cout<<oldString<<endl;
    cout<<oldString.substr(lastpos+1)<<endl;
    cout<<"Compare to bin is"<<oldString.compare((lastpos+1),oldString.length(),"bin")<<endl;
#endif
    string headerStr=oldString.replace((lastpos+1),oldString.length(),"header");
    ofstream headerFile;
    headerFile.open(headerStr.c_str());
    if (headerFile.is_open()){
        cout<<"Header created at :"<<headerStr<<endl;
        if (f_type==sorted){
            headerFile<<"sorted"<<endl;
            SortInfo *sortInfo = (SortInfo *) startup;
            headerFile<<sortInfo->runLength<<endl;
            //(sortInfo->myOrder)->Print();
            (sortInfo->myOrder)->PrintToFile(headerFile);
        }
        else if (f_type==heap)
            headerFile<<"heap"<<endl;
        headerFile.close();
        return 1;
    }
    else {
        cout<<"Unable to create file at :"<<headerStr<<endl;
        return 0;
    }


}

fType DBFile::readHeader(char* bin_path, SortInfo *getInfo){
    string oldString(bin_path);
    size_t lastpos=oldString.find_last_of('.');
#ifdef verbose
    cout<<oldString<<endl;
    cout<<oldString.substr(lastpos+1)<<endl;
    cout<<"Compare to bin is"<<oldString.compare((lastpos+1),oldString.length(),"bin")<<endl;
#endif
    string headerStr=oldString.replace((lastpos+1),oldString.length(),"header");
    cout<<"File header is :"<<headerStr<<endl;
    ifstream headerFile;
    headerFile.open(headerStr.c_str());
    string isFileType;
    if (headerFile.is_open())
    {
        if ( headerFile.good() )
        {
            getline (headerFile,isFileType);
            //cout<<isFileType<<endl;
        }
    }
    if ((isFileType.compare("sorted"))==0){
	string type;
	headerFile>>(getInfo->runLength);
	headerFile>>(getInfo->myOrder->numAtts);
	for (int i = 0; i < getInfo->myOrder->numAtts; i++)
        {
            headerFile >> getInfo->myOrder->whichAtts[i];
            headerFile >> type;
            if (type.compare("Int") == 0)
                getInfo->myOrder->whichTypes[i] = Int;
            else if (type.compare("Double") == 0)
                getInfo->myOrder->whichTypes[i] = Double;
            else if (type.compare("String")==0)
                getInfo->myOrder->whichTypes[i] = String;
            else 
            {
                cout<<"Got invalid datatype"<<endl;
                return invalid; 
            }
        }
	headerFile.close();
        return sorted;
	}
    else if ((isFileType.compare("heap"))==0){
	headerFile.close();
        return heap;
	}
    else {
	headerFile.close();
		return invalid;
	}
}

