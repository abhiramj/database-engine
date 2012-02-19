#include "BigQ.h"
#include "Schema.h"
#include "DBFile.h"
#include "ComparisonEngine.h"
#include <vector>
#include <assert.h>
#include <algorithm>
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    // read data from in pipe sort them into runlen pages
    
    sortedWrite.Open(0,"temp.bin");
    int pagesRead=0;
    int currOffset=0;
    // This finds out the pages read for every run, returns 0 when the becomes empty
    do {
        pagesRead=WriteRun(sortorder,in,runlen,currOffset);
        if (pagesRead!=0)
            runStart.push_back(currOffset);
        currOffset+=pagesRead;
    }while (pagesRead!=0);


    //int capacity= recordArray.size();//(i==runlen)?runlen:i;
    //recordArray.sort()

    // construct priority queue over sorted runs and dump sorted data 
    // into the out pipe

    // finally shut down the out pipe
    out.ShutDown ();
    sortedWrite.Close();
}

int BigQ::WriteRun(OrderMaker &sortorder,Pipe &in, int runLength, int currPageOffset){

    //Schema mySchema("catalog","lineitem");
    Page *temp = new (nothrow) Page();
    assert(temp!=NULL);
    // read some number of records into a ?vector? and call sort method with stl comparator
    vector <Record *> recordArray;
    int hasNext=0;
    // Logic is that add to vector
    int runIter=runLength;
    Record *rec,*copyRec;
    rec=new Record;
    int hasSpace=0;
    Page tempPage;
    int iterRec=0;
    while (runIter>0 && in.Remove(rec)==1)
    {
        iterRec++;
        copyRec=new Record;
        copyRec->Copy(rec);
        recordArray.push_back(copyRec);
        hasSpace=tempPage.Append(rec);
        if (hasSpace==0){
            runIter--;
            tempPage.EmptyItOut();
            tempPage.Append(rec);
        }
        rec=new Record;
        /*else {
            copyRec=new Record;
            copyRec->Copy(rec);
            recordArray.push_back(rec);
            break;
        }*/
        
    }
    cout<<"No of records removed from pipe   :"<<iterRec<<endl;
    if (recordArray.size()==0)
        return 0;

    sort(recordArray.begin(),recordArray.end(),CompareTheRecords(&sortorder));


    // sorting the records we have
    cout<<"Array size is  :"<<recordArray.size()<<endl;
    vector<Record *>::iterator it;
    //Record& recRef=recordArray.begin();
    Page* p=new Page();
    bool writePage=false;
    int i=0;
    int recordsWritten=0;
    for (it=recordArray.begin();it<recordArray.end();it++){
        //(*it)->Print(&mySchema);
        

        int hasRec=p->Append(*it);
        recordsWritten++;
        if (hasRec==0){
            writePage=true;
            sortedWrite.AddPage(p,i+currPageOffset);
            cout<<"Page written at:   "<<(currPageOffset+i)<<endl;
            i++;
            p=new Page();
            assert(p!=NULL);
            p->Append(*it);
        }
        
    }
    cout<<"Records written is :"<<recordsWritten<<endl;
    sortedWrite.AddPage(p,i+currPageOffset);
    cout<<"Page written at:   "<<(currPageOffset+i)<<endl;
    if (i==0){
        cout<<"No of pages written:   "<<i+1<<endl;
    }
    else cout<<"No of pages written:   "<<i<<endl;
    return i+1;



}

BigQ::~BigQ () {
}
