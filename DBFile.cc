#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <string.h>
#include <string>
#include <assert.h>
#include <iostream>
using namespace std;
// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {
    fileP=NULL;
    endPageOffset=0;
    pageOffset=0;
    recOffset=0;
    header=0;
    path=NULL;
    pageP= NULL;
    tempPage=NULL;
    isDirtyPage=false;
}

int DBFile::Create (char *f_path, fType f_type, void *startup) {
    // Not implemented sorted or tree

    if (f_type==sorted || f_type == tree)
        return 0;
    fileP=new (nothrow) File();
    pageP=new (nothrow) Page();
    // Insufficient memory for file or header
    if (fileP==NULL || pageP==NULL){
        printf("Not enough memory to create file.\n");
        return DB_INSUFFICIENT_MEMORY;
    }
    //fileP->AddPage(pageP,0);
    fileP->Open(0,f_path);
    path=new char[strlen(f_path)+1];
    endPageOffset=0;
    assert(path!=NULL);
    //store path
    strcpy(path,f_path);
    //create header to store attrib
    header=MakeDbHeader(path);
    if (header==0)
        return DB_INSUFFICIENT_MEMORY;
    //
    //This is for putting stuff in header
    //
    /*int *headerPtr=&header;
      write(header,headerPtr,sizeof(header));
      close(header);
      */
    return DB_CREATE_SUCCESS;
}















void DBFile::Load (Schema &f_schema, char *loadpath) {

    FILE *tableFile = fopen (loadpath, "r");
    assert(tableFile!=NULL);
    off_t pageCounter=0;


    int counter=0;
    bool hasNextRecord;
    Page *firstPage=NULL;
    Record *temp= new (nothrow) Record();
    Record *temp2=NULL;
    while (1){
        hasNextRecord= false;
        if (pageP==NULL)
            pageP = new (nothrow) Page();
        if (pageCounter==0)
            firstPage=pageP;
        counter=0;
        assert(pageP!=NULL);
        if (temp2!=NULL){
            pageP->Append(temp2);
            temp2=NULL;
        }
        while (temp->SuckNextRecord (&f_schema, tableFile) == 1) {
            counter++;
            hasNextRecord= true;
            if ((pageP->Append(temp))==0){
                temp2= new (nothrow) Record();
                temp2->Consume(temp);
                break;
            }
        }
        if (hasNextRecord==false)
            break;
        assert(fileP!=NULL);
#ifdef verbose
        cout<<"No in page :  "<<pageCounter<<" is records   :"<<counter<<endl;
#endif
        fileP->AddPage(pageP,pageCounter);
        pageCounter++;
        pageP=NULL;
    }
    cout<<"No of pages written in Load for DBFile : "<<(pageCounter+1)<<endl;
    fclose(tableFile);
    pageP=firstPage;

}







int DBFile::Open (char *f_path) {
    path= new (nothrow) char[strlen(f_path)+1];
    pageP=new (nothrow) Page();
    if (pageP==NULL )
        return 0;
    strcpy(path,f_path);
    header=MakeDbHeader(path);
    /*int *fileDes;
      if (read(header,fileDes,4)>0){
      header=(*fileDes);
      }*/
    fileP=new (nothrow) File();
    assert(path!=NULL && fileP!= NULL);
    fileP->Open(5,path);
    int length = fileP->GetLength();
    endPageOffset=length-1;
    //cout<<"End page offset:    "<<endPageOffset<<endl;
    if (endPageOffset!=-1)
        fileP->GetPage(pageP,0);
    assert(length>0 && length <10000);
    printf("No of pages:  %ld\n ",fileP->GetLength());
    pageOffset=0;
    recOffset=0;
    isDirtyPage=false;
    tempPage=NULL;
    return 1;
}














void DBFile::MoveFirst () {
    recOffset=0;
    pageOffset=0;
    tempPage=NULL;
}

int DBFile::Close () {

    // write code for writing dirty page to disk/file
    if (isDirtyPage)
    {
        //cout<<endPageOffset<<endl;
        //getchar();
        fileP->AddPage(pageP,endPageOffset);
    }
    fileP->Close();
    close(header);
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

void DBFile::Add (Record &rec) {
    
    assert(fileP!=NULL);
    bool writeFirst=false;
    int lastPage=endPageOffset;
    assert (pageP!=NULL);
    //    pageP=new Page();
    //else fileP->GetPage(pageP,lastPage);
    // Potential error
   // cout<<" Last page offset "<<lastPage<<endl;
    //if (fileP->GetLength()!=0)
    do {
        if (writeFirst){
            //cout<<endPageOffset<<endl;
            fileP->AddPage(pageP,lastPage);
            lastPage++;
            endPageOffset++;
            pageP=new (nothrow) Page();
            assert(pageP!=NULL);
            pageP->Append(&rec);
            writeFirst=false;
            break;
        }
        int isRec=pageP->Append(&rec);
        if(isRec==0){
            writeFirst=true;
            continue; 
        }
    } while (writeFirst==true);
    assert(fileP!=NULL);
    //cout<<lastPage<<endl;
    isDirtyPage=true;

}





// Logic is to read the page only once and retrieve the records in memory instead of reading the memory again and again.
// For this we need to store Page offset and record offset and load if the page only if record read is the last one.We need a
// state saying if the first GetNext has been called.
//
// Have to implement: Store in PageP, no refetching!!
// Have to write end of page logic
// End of file logic
int DBFile::GetNext (Record &fetchme) {
    //Set the page and record to offset values. for page make a copy to a temp variable to cycle records.
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

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

    ComparisonEngine matcher;
    while (GetNext(fetchme)!=0){
        //f=&fetchme;
        if( matcher.Compare(&fetchme,&literal,&cnf)==0)
            continue;
        return 1;
    }
    return 0;


}

int DBFile::MakeDbHeader(char* bin_path){
    string oldString(bin_path);
    size_t lastpos=oldString.find_last_of('.');
#ifdef verbose
    cout<<oldString<<endl;
    cout<<oldString.substr(lastpos+1)<<endl;
    cout<<"Compare to bin is"<<oldString.compare((lastpos+1),oldString.length(),"bin")<<endl;
#endif
    string newString=oldString.replace((lastpos+1),oldString.length(),"header");
    cout<<newString<<endl;

    int newFile=open(newString.c_str(), O_RDWR | O_CREAT,S_IRUSR | S_IWUSR);
    assert(newFile!=0);
    return newFile;
}

