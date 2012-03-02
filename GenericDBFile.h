//#include"DBFile.h"
class GenericDBFile{
    public:
        GenericDBFile(){
        }
};
class Heap:virtual public GenericDBFile{
    public:
        Heap():GenericDBFile(){
        }
};

class Sorted:virtual public GenericDBFile{
    public:
        Sorted():GenericDBFile(){
        }
};
