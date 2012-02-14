#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "DBFile.h"
#include "ComparisonEngine.h"
using namespace std;

class BigQ {

    public:

        BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
        int WritePage(DBFile &f,OrderMaker &sortorder,Pipe &in, int pageOffset);
        ~BigQ ();
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
#endif
