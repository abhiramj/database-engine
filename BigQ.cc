#include "BigQ.h"
#include "Defs.h"
#include "Schema.h"
#include "DBFile.h"
#include "ComparisonEngine.h"
#include <vector>
#include <assert.h>
#include <algorithm>
#include <queue>
#include <iomanip>
#define verbose 1
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    // read data from in pipe sort them into runlen pages

    Schema mySchema("catalog","lineitem");
    sortedWrite.Open(0,"temp.bin");
    int pagesRead=0;
    int currOffset=0;
    // This finds out the pages read for every run, returns 0 when the becomes empty
    // runLength and runStart keep the offsets
    do {
        pagesRead=WriteRun(sortorder,in,runlen,currOffset);
        if (pagesRead!=0){
            runLengths.push_back(pagesRead);
            runStart.push_back(currOffset);
        }
        currOffset+=pagesRead;
    }while (pagesRead!=0);

    //Print out offsets and lengths
    int totalRuns=runLengths.size();
    cout<<"No of buffers:  "<<totalRuns<<endl;
    cout<<"Offsets are"<<endl;
    for (int i=0;i<totalRuns;i++){
        cout<<setw(5)<<runStart.at(i);
    }
    cout<<endl;
    for (int i=0;i<totalRuns;i++){
        cout<<setw(5)<<runLengths.at(i);
    }
    cout<<endl;
    // construct priority queue over sorted runs and dump sorted data
    // into the out pipe

    //Set up our structures
    vector<Page *> runBuffers(totalRuns);
    priority_queue<PQRecStruct *,vector< PQRecStruct *>,CompareForPQ> recQ(&sortorder);
    //vector<Page *> outBuffers(2);

    vector<PQRecStruct *> PQRecPtrs(totalRuns);
    //Begin Reading pages to the buffers for the first time
    for (int i=0;i<totalRuns;i++){
        //cout<<runStart.at(i)<<endl;
        runBuffers.at(i)=new (nothrow) Page();
        assert(runBuffers.at(i)!=NULL);
        sortedWrite.GetPage(runBuffers.at(i),runStart.at(i));
        PQRecPtrs.at(i)= new PQRecStruct;
        PQRecPtrs.at(i)->recStruct = new (nothrow) Record();
        PQRecPtrs.at(i)->runNum=i;
        (runBuffers.at(i))->GetFirst(PQRecPtrs.at(i)->recStruct);
        recQ.push(PQRecPtrs.at(i));
        //(PQRecPtrs.at(i)->recStruct)->Print(&mySchema);
    }

    /*
    //Testing code
    cout<<"************************************************"<<endl;
    out.Insert(recQ.top()->recStruct);
    (recQ.pop());
    //(recQ.top()->recStruct)->Print(&mySchema);
    out.Insert(recQ.top()->recStruct);
    recQ.pop();
    out.Insert(recQ.top()->recStruct);
    recQ.pop();
    out.Insert(recQ.top()->recStruct);
    recQ.pop();
    */

    // Next logic is that pop an Element into out pipe, next record comes from the runIndex runNum
    // If we run of records for page , decrement runLength for that page, load a new one by incrementing runStart 
    // as long as runLength is not zero
    int runsEmpty=0;
    int totalRecsinQ=totalRuns;
    bool isNextRunEmpty=false;
    do {
        // popping the 1st elem, finding out where it came from
        int runIndex=recQ.top()->runNum;
        out.Insert(recQ.top()->recStruct);
        recQ.pop();
        PQRecStruct* PQNextRec=new PQRecStruct;
        PQNextRec->recStruct= new Record();
        //Read run if its length is not zero
        if (runLengths.at(runIndex)!=0){
            int hasRec=(runBuffers.at(runIndex))->GetFirst(PQNextRec->recStruct);
            // New page is getting loaded after exhausting old
            if (hasRec==0){
                //cout<<"Size of runbuffers at empty for offset: "<<runStart.at(runIndex)<<"    is   "<<runBuffers.at(runIndex)->numRecs<<endl;
                (runLengths.at(runIndex))--;
                (runStart.at(runIndex))++;
                runBuffers.at(runIndex)->EmptyItOut();
                int lastOffsetOfRun=runStart.at(runIndex)+runLengths.at(runIndex)-1;
                //Check that we are not eating into the next run
                if ( runStart.at(runIndex) > lastOffsetOfRun){
                    runsEmpty++;
                   // cout<<"Run emptied,total runs now stands at:   "<<(totalRuns-runsEmpty)<<endl;
                    //cout<<"Queue has "<<recQ.size()<<"  records"<<endl;
                    continue;
                }
                //If valid, get page
                sortedWrite.GetPage(runBuffers.at(runIndex),runStart.at(runIndex));
                /*if(!(runBuffers.at(runIndex))->GetFirst(PQNextRec->recStruct)){
                  cout<<"Not s'posed to happen, new page loaded with no record"<<endl;
                  break;
                  }*/
                //Got next record from new page
                (runBuffers.at(runIndex))->GetFirst(PQNextRec->recStruct);
                PQNextRec->runNum=runIndex;
            }else PQNextRec->runNum=runIndex;
            recQ.push(PQNextRec);
            totalRecsinQ++;
        }else {
            runsEmpty++;
           // cout<<"Run emptied,total runs now stands at:   "<<(totalRuns-runsEmpty)<<endl;
           // cout<<"Queue has "<<recQ.size()<<"  records"<<endl;
        }
    }while (runsEmpty<totalRuns);
   // cout<<"totalRecsinQ  "<<totalRecsinQ<<endl;



    // finally shut down the out pipe
    out.ShutDown ();
    sortedWrite.Close();
}

int BigQ::WriteRun(OrderMaker &sortorder,Pipe &in, int runLength, int currPageOffset){


    // read some number of records into a ?vector? and call sort method with stl comparator
    vector <Record *> recordArray;
    int hasNext=0;
    // Logic is that add to vector as we remove from pipe and then append to appropriate page.
    // We know runLength no of Pages are reached when page gets full runLength times
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
    }
    //cout<<"No of records removed from pipe   :"<<iterRec<<endl;
    if (recordArray.size()==0)
        return 0;

    sort(recordArray.begin(),recordArray.end(),CompareTheRecords(&sortorder));

    // sorting the records we have
#ifdef verbose
    //cout<<"Array size is  :"<<recordArray.size()<<endl;
#endif
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
#ifdef verbose
           // cout<<"Page written at:   "<<(currPageOffset+i)<<endl;
#endif
            i++;
            p->EmptyItOut();
            assert(p!=NULL);
            p->Append(*it);
        }

    }
    sortedWrite.AddPage(p,i+currPageOffset);
#ifdef verbose
    //cout<<"Records written is :"<<recordsWritten<<endl;
    //cout<<"Page written at:   "<<(currPageOffset+i)<<endl;
#endif
    if (i==0){
        //cout<<"No of pages written:   "<<i+1<<endl;
    }
    recordArray.clear();
    //else cout<<"No of pages written:   "<<i<<endl;
    return i+1;
}

BigQ::~BigQ () {
    //sortedWrite.Close();
}
