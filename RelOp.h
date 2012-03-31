#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include <sstream>
#include <assert.h>
class RelationalOp {
    public:
        // blocks the caller until the particular relational operator 
        // has run to completion
        virtual void WaitUntilDone () = 0;

        // tell us how much internal memory the operation can use
        virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

    private:
        // pthread_t thread;
        // Record *buffer;
        DBFile *sf_inputFile;
        Pipe *sf_outputPipe;
        CNF *sf_selectOp;
        Record *sf_literal;
        int sf_runlen;
        pthread_t sf_worker;

    public:
        SelectFile(){
        };
        ~SelectFile(){
        };
        void sf_Operation();
        void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
        void WaitUntilDone ();
        void Use_n_Pages (int n);

};

class SelectPipe: public RelationalOp {
    private:
        Pipe *sp_inputPipe;
        Pipe *sp_outputPipe;
        CNF sp_selOp;
        Record *sp_literal;
        int sp_runlen;
        pthread_t sp_worker;

    public:
        SelectPipe() {
        };
        ~SelectPipe() {
        };
        void sp_Operation();
        void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
        void WaitUntilDone () ;
        void Use_n_Pages (int );
};
class Project : public RelationalOp {
    private:
        Pipe *p_inputPipe;
        Pipe *p_outputPipe;
        int *p_keepMe;
        int p_numAttsInput;
        int p_numAttsOutput;
        pthread_t p_worker;
        int p_runlen; 
    public:
        Project(){
        };
        ~Project(){
        };
        void p_Operation();
        void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
        void WaitUntilDone ();
        void Use_n_Pages (int n);
};
class Join : public RelationalOp { 
    public:
        void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { }
        void WaitUntilDone () { }
        void Use_n_Pages (int n) { }
};
class DuplicateRemoval : public RelationalOp {
    private:
        Pipe *dr_inputPipe;
        Pipe *dr_outputPipe;
        Schema *dr_mySchema;
        int dr_runlen;
        pthread_t dr_worker;
    public:
        DuplicateRemoval(){
        };
        ~DuplicateRemoval(){
        };
        void dr_Operation();
        void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) ;
        void WaitUntilDone ();
        void Use_n_Pages (int n);
};
class Sum : public RelationalOp {
    private:
        Pipe *s_inputPipe;
        Pipe *s_outputPipe;
        Function *s_computeMe;
        pthread_t s_worker;
        int s_runlen;
    public:
        Sum(){
        };
        ~Sum(){
        };
        void s_Operation();
        void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe) ;
        void WaitUntilDone () ;
        void Use_n_Pages (int n) ;
};
class GroupBy : public RelationalOp {
    private:
        Pipe *gb_inputPipe;
        Pipe *gb_outputPipe;
        OrderMaker *gb_om;
        Function *gb_computeMe;
        pthread_t gb_worker;
        int gb_runlen;
    public:
        GroupBy() {
        }
        ;
        ~GroupBy() {
        }
        ;
        void gb_Operation();
        void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) ;
        void WaitUntilDone () ;
        void Use_n_Pages (int n) ;
};
class WriteOut : public RelationalOp {
    private:
        Pipe *wo_inputPipe;
        FILE *wo_outFile;
        Schema *wo_schema;
        int wo_runlen;
        pthread_t wo_worker;
    public:
        WriteOut(){
        };
        ~WriteOut(){
        };
        void wo_Operation();
        void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
        void WaitUntilDone ();
        void Use_n_Pages (int n) ;
};
#endif
