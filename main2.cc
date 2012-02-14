#include <iostream>
#include <stdlib.h>
#include "DBFile.h"
#include "test.h"
using namespace std;

int main(){
         DBFile file;
         void* y;
         Schema mySchema ("catalog", "nation");
         file.Open("nation.bin");
         Record temp;
         for (int i=0;i<10000;i++){
            file.MoveFirst();
            if ((file.GetNext(temp)==1)){
                file.Add(temp);
                }
            else break;
         }
        file.Close();

        
         return 1;
}
