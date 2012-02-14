#include "BigQ.h"
#include "DBFile.h"
#include "ComparisonEngine.h"
#include <vector>
#include <assert.h>
#include <algorithm>
BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    // read data from in pipe sort them into runlen pages
    DBFile sortedWrite;
    sortedWrite.Create("temp.bin",heap,NULL);
    //sortedWrite.Open("temp.bin");
    int p=WritePage(sortedWrite,sortorder,in,0);
    //int capacity= recordArray.size();//(i==runlen)?runlen:i;
    //recordArray.sort()

    // construct priority queue over sorted runs and dump sorted data 
    // into the out pipe

    // finally shut down the out pipe
    out.ShutDown ();
    sortedWrite.Close();
}

int BigQ::WritePage(DBFile &f,OrderMaker &sortorder,Pipe &in, int pageOffset){

    Record *readIn= new (nothrow) Record();
    Page *temp = new (nothrow) Page();
    assert(temp!=NULL && readIn!=NULL);
    // read some number of records into a ?vector? and call sort method with stl comparator
    vector <Record *> recordArray;
    int hasNext=false;
    // Logic is that add to vector and Page and if the Page becomes full, break
    // and write last drawn record back to pipe. At end sort and
    // write back to DBFile. This is done by write page
    while (in.Remove(readIn)==1){
        recordArray.push_back(readIn);
        hasNext=temp->Append(readIn);
        if (hasNext==0){
            in.Insert(readIn);
            break;
        }
    }
    // sorting the records we have
    sort(recordArray.begin(),recordArray.end(),CompareTheRecords(&sortorder));
    for(int i=0;i<recordArray.size();i++){
        f.Add(*recordArray[i]);
    }

    return 1;

    //f->AddPage(temp,pageOffset);


}

BigQ::~BigQ () {
}
