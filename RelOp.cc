#include "RelOp.h"
#include "Record.h"
using namespace std;
void initSfThread(void *obj){
    SelectFile sf_obj = *((SelectFile *)  obj);
    sf_obj.sf_Operation();
    pthread_exit(NULL);

}
void initWoThread(void *obj){
    WriteOut wo_obj = *((WriteOut *)  obj);
    wo_obj.wo_Operation();
    pthread_exit(NULL);

}
void initSpThread(void *obj){
    SelectPipe sp_obj = *((SelectPipe *)  obj);
    sp_obj.sp_Operation();
    pthread_exit(NULL);

}
void initSThread(void *obj){
    Sum s_obj = *((Sum *)  obj);
    s_obj.s_Operation();
    pthread_exit(NULL);

}
void* initPThread(void *obj) {
    Project p_obj = *((Project *)obj);
    p_obj.p_Operation();
    pthread_exit(NULL);
}
void* initDrThread(void *obj) {
    DuplicateRemoval dr_obj = *((DuplicateRemoval *) obj);
    dr_obj.dr_Operation();
    pthread_exit(NULL);

}
void* GroupBy_Thread(void * obj) {
    GroupBy gb_obj = *((GroupBy *) obj);
    gb_obj.gb_Operation();
    pthread_exit(NULL);

}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,
        Function &computeMe) {
    this->gb_inputPipe = &inPipe;
    this->gb_outputPipe = &outPipe;
    this->gb_om = &groupAtts;
    this->gb_computeMe = &computeMe;
    pthread_create(&gb_worker, NULL, GroupBy_Thread, (void *) this);
}

void GroupBy::WaitUntilDone() {
    pthread_join(gb_worker, NULL);
}

void GroupBy::Use_n_Pages(int runlen) {
    this->gb_runlen = runlen;
}

void GroupBy::gb_Operation() {
vector<Record *> recVec;
Pipe *tempPipe = new Pipe(10000000);
ComparisonEngine recComp;
Type dType;
int sum=0;
double dsum=0.0;
int result=0;
double dresult=0.0;
BigQ sortBq(*(this->gb_inputPipe), *tempPipe, *gb_om, this->gb_runlen,gb_runlen);
bool isFirst=true;
bool isLast=true;
bool gotAtts=false;
int numAttsIn=0;
int numAttsOut=0;
Record* getRec=new Record();
Record *prevRec= new Record();
Record* thisRec;
        cout<<"here"<<endl;
while (tempPipe->Remove(getRec)){
    //If first then keep in pipe and wait for next
if (isFirst){
isFirst=false;
//tempPipe->Insert(getRec);
//If first is last we will make a pseudoRec diff from normal, that being record with only 1 attrib  and break at end .
prevRec->Copy(getRec);
recVec.push_back(getRec);
getRec= new Record();
if (!tempPipe->Remove(getRec)){
    getRec->Copy(prevRec);
    int currAtts=getRec->GetNumAtts();
    int onlyAtt=0;
    int *onlyAttPtr= &onlyAtt;
    getRec->Project(onlyAtt,1,currAtts);
    isFirst=true;
    isLast=true;
}
else continue;
}
else if((isLast && isFirst) || !isFirst){
    int isNotSame=recComp.Compare(getRec,prevRec,gb_om);
    if (!isNotSame){
        prevRec->Copy(getRec);
        recVec.push_back(getRec);
        getRec=new Record();
    }
    //make sum and insert in pipe
    else {
        while (!recVec.empty()){
        thisRec= recVec.back();
        if (!gotAtts){
        numAttsIn=thisRec->GetNumAtts();
        gotAtts=true;
        dType = (this->gb_computeMe)->Apply(*thisRec, sum,dsum);
        if (dType == Int)
            result += sum;
        else
            dresult += dsum;
        }
        if (recVec.size()==1){
            thisRec= new Record();
            thisRec->Copy(recVec.front());
        }
        recVec.pop_back();
        }
        assert(gotAtts);
        // Got num of atts,type and recs in recVec .Push it out to pipe while making sum
        // ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    Attribute *a= new Attribute();
    a->name = (char *)malloc(sizeof(10));
    strcpy("Sum",a->name);
    a->myType=dType;
    Schema* sumSchema = new  Schema("sum",1,a);
    char* sumString=new char[30];
    if (dType==Int){
        sprintf(sumString,"%d",result);
    }
    else sprintf(sumString,"%10E",dresult);
    int printLen=strlen(sumString);
    assert(printLen<29);
    sumString[printLen++]="|";
    sumString[printLen]="\0";
    Record* sumRec= new Record();
    sumRec->ComposeRecord(sumSchema,(const char *) sumString);

    numAttsOut = (this->gb_om)->numAtts;
    int num = 0;
    int *keepMe = (this->gb_om)->whichAtts;
    int *keepMeMerge = new int[num + 1];
    for (int i = 1, j = 0; i <= num; i++, j++) {
        keepMeMerge[i] = keepMe[j];
    }
    keepMeMerge[0] = 0;
    //sumRecord->Print(&mySchema,1);
    // //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    gotAtts=false;
    Record *mergedRec= new Record();
    mergedRec->MergeRecords(sumRec, thisRec, 1, numAttsIn,keepMeMerge, (numAttsOut + 1), 1);
    gb_outputPipe->Insert(mergedRec);
    recVec.clear();
    recVec.push_back(getRec);
    delete mergedRec;
    delete keepMeMerge;
    delete keepMe;
    delete sumRec;
    
    }
}
}

gb_outputPipe->ShutDown();
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    this->dr_inputPipe = &inPipe;
    this->dr_outputPipe = &outPipe;
    this->dr_mySchema = &mySchema;
    int dr_pthread = pthread_create(&dr_worker, NULL,initDrThread, (void *) this);
    if (dr_pthread) {
        cout << "Not able to create the thread" << endl;
        exit(-1);
    }
}
void DuplicateRemoval::dr_Operation() {
    Record temp;
    Record *currRec;
    OrderMaker *dr_om;
    //this->dr_runlen = 5;
    dr_om = new OrderMaker(this->dr_mySchema);
    Pipe *bigQSortedPipe = new Pipe(10000000);
    BigQ sortBq(*(this->dr_inputPipe), *bigQSortedPipe, *dr_om, this->dr_runlen,dr_runlen);
    ComparisonEngine recComp;

    bool isFirst = true;
    currRec = new Record;
    while (bigQSortedPipe->Remove(currRec)) {
        //For first one don't compare
        if (isFirst) {
            temp.Copy(currRec);
            (this->dr_outputPipe)->Insert(currRec);
            isFirst = false;
        } else {
            if (recComp.Compare(&temp, currRec, dr_om) != 0) {
                temp.Copy(currRec);
                (this->dr_outputPipe)->Insert(currRec);
            }
        }
        delete currRec;
        currRec = new Record;
    }

    (this->dr_inputPipe)->ShutDown();
    bigQSortedPipe->ShutDown();
    delete bigQSortedPipe;
}

void DuplicateRemoval::WaitUntilDone() {
    pthread_join(dr_worker, NULL);
}

void DuplicateRemoval::Use_n_Pages(int runlen) {
    this->dr_runlen = runlen;
}
void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput,
        int numAttsOutput) {


    this->p_inputPipe = &inPipe;
    this->p_outputPipe = &outPipe;
    this->p_keepMe = keepMe;
    this->p_numAttsInput = numAttsInput;
    this->p_numAttsOutput = numAttsOutput;
    int p_pthread = pthread_create(&p_worker, NULL, initPThread,(void *) this);
    if (p_pthread) {
        cout << "Not able to create the thread" << endl;
        exit(-1);

    }

}

void Project::p_Operation() {
    Record *currRec=new Record();
    while((this->p_inputPipe)->Remove(currRec)) 
    {

        currRec->Project(p_keepMe, p_numAttsOutput, p_numAttsInput);
        (this->p_outputPipe)->Insert(currRec);
        currRec = new Record();
    }
    (this->p_inputPipe)->ShutDown();
    (this->p_outputPipe)->ShutDown();
}

void Project::WaitUntilDone() {

    pthread_join(p_worker, NULL);
}

void Project::Use_n_Pages(int runlen) {
    this->p_runlen = runlen;
}
void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal){
    this->sf_inputFile = &inFile;
    this->sf_outputPipe = &outPipe;
    this->sf_selectOp = &selOp;
    this->sf_literal = &literal;
    int sf_pthread = pthread_create(&sf_worker, NULL, initSfThread,(void *) this);
    if (sf_pthread) {
        cout<<"Not able to create the thread"<<endl;
        exit(-1);
    }
}
void SelectFile::sf_Operation(){
    Record getRec;
    ComparisonEngine compE ;
    int count =0;
    this->sf_inputFile->MoveFirst();
    while ((this->sf_inputFile)->GetNext(getRec, *(this->sf_selectOp),*(this->sf_literal))) {
        (this->sf_outputPipe)->Insert(&getRec);
        count++;
    }
    (this->sf_outputPipe)->ShutDown();
    cout<<"Number of records returned by select is :"<<count<<endl;

}
void SelectFile::WaitUntilDone() {
    pthread_join(sf_worker, NULL);
}
void SelectFile::Use_n_Pages(int runlen) {
    // set the member variable of the class to determine the number of pages to be used for this operation
    sf_runlen = runlen;
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    this->wo_inputPipe = &inPipe;
    this->wo_outFile = outFile;
    this->wo_schema = &mySchema;
    int wo_pthread = pthread_create(&wo_worker, NULL, initWoThread,
            (void *) this);
    if (wo_pthread) {
        cout<<"Error creating thread"<<endl;
        exit(-1);
    }

}
void WriteOut::wo_Operation() {

    Record wo_rec;
    int count = 0;

    while ((this->wo_inputPipe)->Remove(&wo_rec)) {
        wo_rec.PrintToFile(this->wo_outFile,this->wo_schema,count);
        count++;
    }
    fclose(this->wo_outFile);

}
void WriteOut::WaitUntilDone() {
    (this->wo_inputPipe)->ShutDown();
    pthread_join(wo_worker, NULL);
}

void WriteOut::Use_n_Pages(int runlen) {
    this->wo_runlen = runlen;
}
void SelectPipe::sp_Operation(){
    Record getRec;
    ComparisonEngine compE ;
    while ((this->sp_inputPipe)->Remove(&getRec)) {
        int hasRec=compE.Compare(&getRec, (this->sp_literal), &(this->sp_selOp));
        if (hasRec) {
            (this->sp_outputPipe)->Insert(&getRec);
        }
    }
    (this->sp_outputPipe)->ShutDown();
}


void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal){
    this->sp_inputPipe= &inPipe;
    this->sp_outputPipe= &outPipe;
    this->sp_selOp= selOp;
    this->sp_literal=&literal;
    int  sp_pthread= pthread_create(&sp_worker, NULL, initSpThread,
            (void *) this);
    if (sp_pthread) {
        cout<<"Error creating thread"<<endl;
        exit(-1);
    }
}
void SelectPipe::WaitUntilDone (){                                
    (this->sp_inputPipe)->ShutDown();
    pthread_join(sp_worker, NULL);
}
void SelectPipe::Use_n_Pages (int runlen){
    this->sp_runlen=runlen;
}

void Sum::s_Operation(){
    int sum = 0;
    double dsum=0.0;
    int result=0;
    double dresult=0.0;
    bool isSumInt=false;
    Record *currRec= new Record();
    Attribute *a= new Attribute();
    a->name = (char *)malloc(sizeof(10));
    assert(a->name);
    strcpy(a->name,"Sum");
    Type sum_Type;
    while (s_inputPipe->Remove(currRec)){
        sum_Type=(this->s_computeMe)->Apply(*currRec,sum,dsum);
        if (sum_Type==Int){
            result+=sum;
            isSumInt=true;

        }
        else {
            dresult+=dsum;
        }
    }
    a->myType=sum_Type;
    Schema* sumSchema = new  Schema("sum",1,a);
    delete currRec;
    (s_inputPipe)->ShutDown();
    char* sumString=new char[15];
    if (isSumInt){
        sprintf(sumString,"%d",result);
    }
    else sprintf(sumString,"%e",dresult);
    int printLen=strlen(sumString);
    assert(printLen<29);
    sumString[printLen++]='|';
    sumString[printLen]='\0';
    Record sumRec;
    //cout<<sumString<<endl;
    sumRec.ComposeRecord(sumSchema, sumString);
    s_outputPipe->Insert(&sumRec);
    s_outputPipe->ShutDown();
}

void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
    this->s_inputPipe=&inPipe; 
    this->s_outputPipe = &outPipe;
    this->s_computeMe = &computeMe;
    int s_pthread = pthread_create(&s_worker, NULL, initSThread,(void *) this);
    if (s_pthread) {
        cout << "Error while creating thread\n";
        exit(-1);
    }
}
void Sum::WaitUntilDone () { 
    pthread_join(s_worker, NULL);
}
void Sum::Use_n_Pages (int runlen) { 
    s_runlen=runlen;
}

