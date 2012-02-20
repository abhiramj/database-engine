#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "DBFile.h"
#include "ComparisonEngine.h"
#include <vector>
using namespace std;

struct PQRecStruct{
Record* recStruct;
int runNum;
};
class BigQ {

    public:

        BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
        int WriteRun(OrderMaker &sortorder,Pipe &in, int runLength,int offsetToWrite);
        ~BigQ ();
    private:
        File sortedWrite;
        vector<int > runStart;
        vector<int > runLengths;
};
static void* getRunsFromInputPipeHelper(void*);
struct CompareTheRecords
{
    OrderMaker *sortOrder;
    CompareTheRecords(OrderMaker *oMaker): sortOrder(oMaker) {}

    bool operator()(Record* const& r1, Record* const& r2)
    {
        ComparisonEngine cEngine;
        if (cEngine.Compare(r1, r2, sortOrder) < 0)
            return true;
        else
            return false;
    }

};
class CompareForPQ
{
    private:
        OrderMaker* sortOrder;
    public:
        CompareForPQ(OrderMaker *sortorder)
        {
            sortOrder = sortorder;
        }
        bool operator()(PQRecStruct *leftRecord, PQRecStruct *rightRecord)
        {
            ComparisonEngine comparisonEngine;
            if(comparisonEngine.Compare(leftRecord->recStruct, rightRecord->recStruct, sortOrder)<0)
                return false;

            return true;

        }
};

#endif
