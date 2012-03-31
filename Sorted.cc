#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
//#include "GenericDBFile.h"
#include "DBFile.h"
#include "Defs.h"
#include <string.h>
#include <string>
#include <assert.h>
#include <iostream>
#include <fstream>
#include "BigQ.h"
#define verbose 1
#ifndef buffsz
#define buffsz 100
#endif
using namespace std;

Sorted::Sorted(){
    fileP=NULL;
    endPageOffset=0;
    pageOffset=0;
    recOffset=0;
    path=NULL;
    pageP= NULL;
    tempPage=NULL;
    isDirtyPage=false;
    fileOrder=NULL;
    diffFile =NULL;
    isWrite=false;
    isCalcQMaker=false;
    input = new (nothrow) Pipe(buffsz);
    output = new (nothrow) Pipe(buffsz);
    if(!input || !output)
        cout<<"pipe creation failed"<<endl;
}

Sorted::~Sorted(){

}
void Sorted::setSort(SortInfo *getSort){
fileOrder=getSort;
}

int Sorted::Close(){
// write code for writing BigQ to sorted disk/file
    int mergeStat;
	if (isWrite){
	mergeStat=Merge();
	isWrite=false;
	}
    if (mergeStat==0){
        cout<<"Merge exited with error"<<endl;
        return 0;
    }
    //delete input;
    //delete output;
    fileP->Close();
    delete fileP;
    if (path!=NULL)
        delete [] path;
    /*if (pageP!=NULL)
      delete pageP;
      */
    isDirtyPage=false;
    if (tempPage!=NULL)
        delete tempPage;
    printf("File closed \n");
    return 1;

}

void Sorted::Add(Record &rec){
    isWrite=true;
    if (diffFile==NULL){
        pthread_t bqStart;
        diffFile = new (nothrow) BigQ(*input, *output, *(fileOrder->myOrder), fileOrder->runLength);
        if(!diffFile)
            cout<<"bigq failed"<<endl;
    }
   input->Insert(&rec);
}
int Sorted::GetNext(Record& fetchme, CNF& cnf, Record& literal){
    ComparisonEngine matcher;
    while (GetNext(fetchme)!=0){
        if(matcher.Compare(&fetchme,&literal,&cnf)==0)
            continue;
        return 1;
    }

    // If QueryMaker is not calculated, make it
    /*if (!isCalcQMaker){
    
    }*/
    return 0;

}
int Sorted::Open(char* f_path){
    cout<<"Opening file from path   :"<<f_path<<endl;
    path= new (nothrow) char[strlen(f_path)+1];
    pageP=new (nothrow) Page();
    if (pageP==NULL )
        return 0;
    strcpy(path,f_path);
    fileP=new (nothrow) File();
    assert(path!=NULL && fileP!= NULL);
    fileP->Open(5,path);
    int length = fileP->GetLength();
    if (length!=0)
    endPageOffset=length-1;
    else endPageOffset=0;
    //cout<<"End page offset:    "<<endPageOffset<<endl;
    if (endPageOffset!=0)
        fileP->GetPage(pageP,0);
    printf("No of pages:  %ld\n ",fileP->GetLength());
    pageOffset=0;
    recOffset=0;
    isDirtyPage=false;
    tempPage=NULL;
    return 1;
}

int Sorted::Create (char *f_path, fType f_type, void *startup) {
    // Not implemented sorted or tree

    fileP=new (nothrow) File();
    pageP=new (nothrow) Page();
    // Insufficient memory for file or header
    if (fileP==NULL || pageP==NULL){
        printf("Not enough memory to create file.\n");
        return DB_INSUFFICIENT_MEMORY;
    }
    
// Big edit

/*int fPlace=0;
    string schemaF (f_path);
    fPlace=schemaF.find_first_of('.');
    schemaF.erase(fPlace,schemaF.length()-1);
    cout<<"Current scheme is :  "<<schemaF<<endl;


*/
    SortInfo* sort= (SortInfo *)startup;


    fileOrder= new (nothrow) SortInfo;
    fileOrder->runLength = sort->runLength;
    fileOrder->myOrder = sort->myOrder;
    //fileP->AddPage(pageP,0);
    fileP->Open(0,f_path);
    endPageOffset=0;
    path=new char[strlen(f_path)+1];
    assert(path!=NULL);
    strcpy(path,f_path);
    return DB_CREATE_SUCCESS;
    
}

int Sorted::GetNext (Record &fetchme) {
    //Set the page and record to offset values. for page make a copy to a temp variable to cycle records.
    if (isWrite){
        Merge();
        fileP->Open(5,path);
        if (tempPage!=NULL)
        delete tempPage;
        if (pageP!=NULL)
        delete pageP;
        isWrite=false;
        pageOffset=0;
        endPageOffset=fileP->GetLength()-1;
    }
    int pageNum = fileP->GetLength();
    // Load a page with page offset into tempPage. We will use this
    if (tempPage==NULL){
        tempPage= new (nothrow) Page();
        assert(tempPage!=NULL);
        //pageP has a non null record pointing at the beginning
        assert(pageOffset < pageNum-1);
        fileP->GetPage(tempPage,pageOffset);
    }
    Record *tempRec= new (nothrow) Record();
    assert(tempRec!=NULL);
    int isRec=tempPage->GetFirst(tempRec);
    if (isRec==0){
        if (pageOffset >= pageNum-1){
            MoveFirst();
            cout<<"Reached end of file"<<endl;
            return 0;
        }
        else {
            //cout<<"End of page, ---- "<<pageOffset<<" ---- "<<" has "<<recOffset<<" records"<<endl;
            pageOffset++;
            recOffset=0;
            if (pageOffset < pageNum-1){
                assert(pageOffset < pageNum);
                fileP->GetPage(tempPage,pageOffset);
                tempPage->GetFirst(tempRec);
            }
            else return 0;
            //if(tempPage->GetFirst(tempRec)==0)
            //    return 0;
        }
    }
    // cout<<"Current offset of page:  "<<pageOffset<<"  ||  Current offset of record:   "<<(recOffset)<<endl;
    recOffset++;
    fetchme.Consume(tempRec);
    return 1;
}

void Sorted::MoveFirst () {
    recOffset=0;
    pageOffset=0;
    tempPage=NULL;
}

void Sorted::Load (Schema &f_schema, char *loadpath) {

    FILE *tableFile = fopen (loadpath, "r");
    assert(tableFile!=NULL);

    isWrite=true;
    assert (fileOrder!=NULL);
     //   fileOrder->myOrder = new  OrderMaker(f_schema);
    if (diffFile==NULL){
        input = new Pipe(buffsz);
        output = new Pipe(buffsz);
        diffFile= new BigQ(*input, *output, *(fileOrder->myOrder), fileOrder->runLength); 
    }
    Record rec;
    while (rec.SuckNextRecord(&f_schema, tableFile) != 0) {
             input->Insert(&rec);
    }

}

    //Shutting down the input pipe triggers the merging. Now we create a new file. We keep pointers to first two records, one in BigQ and
    //one in the existing file.Do a compare and check which one is written, if one in file is written , get the next record in the file while
    //the file still has records, else get the next record record from BigQ while it still has records. When one of them runs out, write existing
    //records to file. 

int Sorted::Merge(){
   input->ShutDown();
   cout<<"Total pages  :"<< fileP->GetLength()<<endl;
   File* newMerged=new (nothrow) File();
   if (newMerged!=NULL)
   newMerged->Open(0,"mergetmp.bin");
   else {
	cout<<"Could not create file"<<endl;
	return 0;
	}
   Page* readIn = new (nothrow) Page();
   Page* writeIn = new (nothrow) Page();
   if (readIn==NULL || writeIn==NULL){
	cout<<"Could not create temp memory . Not enough space"<<endl;
	return 0;
	}
   writeIn->EmptyItOut();
   readIn->EmptyItOut();
   Record *rec1 = new (nothrow) Record();
   Record *rec2 = new (nothrow) Record();
   int currFilePOffset=0;
   int newFilePOffset=0;
   int hasSpace=0;
   ComparisonEngine ce;
   if (fileP->GetLength()!=0){
   fileP->GetPage(readIn,currFilePOffset);
   //cout<<"Current File Offset:   "<<currFilePOffset<<endl;
   currFilePOffset++;
	}
   else {
   	cout<<"Empty file.... continuing"<<endl;
	}


   int gotFileRec=readIn->GetFirst(rec1);
   int gotBigRec=output->Remove(rec2);
   bool isRec1=false;
   while  (gotFileRec && gotBigRec){
       if (fileOrder->myOrder==NULL){
       cout<<"No fileorder"<<endl;
       return 0;
       }
       else {
           //fileOrder->myOrder->Print();
       }
       assert(rec1!=NULL && rec2!=NULL);
       //fileOrder->myOrder->Print();
       if (ce.Compare(rec1,rec2,fileOrder->myOrder)<0){
           isRec1=true;
           assert(rec1!=NULL);
            hasSpace = writeIn->Append(rec1);
            //gotBigRec=output->Remove(rec2);
           assert(rec1!=NULL);
           if (hasSpace)
               //rec1=new (nothrow) Record();
            gotFileRec = readIn->GetFirst(rec1);
       }
       else {
           isRec1=false;
           hasSpace= writeIn->Append(rec2);
           if (hasSpace)
               //rec2=new Record();
            //gotFileRec = readIn->GetFirst(rec1);
            gotBigRec=output->Remove(rec2);
       }
       if (hasSpace==0){
       newMerged->AddPage(writeIn,newFilePOffset);
       newFilePOffset++;
       writeIn->EmptyItOut();
       if (isRec1){
           assert(rec1!=NULL);
           writeIn->Append(rec1);
           rec1=new (nothrow) Record();
           gotFileRec=readIn->GetFirst(rec1);
       }
       else{
           writeIn->Append(rec2);
           rec2= new (nothrow) Record();
            gotBigRec=output->Remove(rec2);
        }
       }
       if(!gotFileRec){
            if (currFilePOffset < fileP->GetLength()-1){ 
                readIn->EmptyItOut();
               cout<<"Reading file at"<<currFilePOffset<<endl;
                fileP->GetPage(readIn,currFilePOffset);
                rec1=new (nothrow) Record();
                gotFileRec=readIn->GetFirst(rec1);
                currFilePOffset++;
            }
       }
   }
   if (!gotFileRec && gotBigRec){
       do{
           hasSpace=writeIn->Append(rec2);
            if (hasSpace==0){
            newMerged->AddPage(writeIn,newFilePOffset);
            newFilePOffset++;
            writeIn->EmptyItOut();
            writeIn->Append(rec2);
            }
            rec2=new (nothrow) Record();
        }while ((gotBigRec=output->Remove(rec2))!=0);
   }
   if (!gotBigRec && gotFileRec){
       bool isEnd=false;
       while (!isEnd){
        hasSpace=writeIn->Append(rec1);
        if (hasSpace==0){
            newMerged->AddPage(writeIn,newFilePOffset);
            newFilePOffset++;
            writeIn->EmptyItOut();
           assert(rec1!=NULL);
            writeIn->Append(rec1);
        }
        rec1=new (nothrow) Record();
            gotFileRec = readIn->GetFirst(rec1);           
            if(!gotFileRec){
                if (currFilePOffset < fileP->GetLength()-1){
                //readIn->EmptyItOut();
                fileP->GetPage(readIn,currFilePOffset);
                currFilePOffset++;
            gotFileRec = readIn->GetFirst(rec1);           
            }
                else isEnd=true;;
       }
       }
   }
//   cout<<"Got here 2"<<endl;
newMerged->AddPage(writeIn,newFilePOffset);
fileP->Close();
fileP=newMerged;
if (remove(path)!=0){
cout<<"File did not exist or error"<<endl;
}
if (rename("mergetmp.bin",path))
cout<<"Error renaming"<<endl;
else cout<<"Renamed to "<<path<<" succesfully"<<endl;
return 1;
}
