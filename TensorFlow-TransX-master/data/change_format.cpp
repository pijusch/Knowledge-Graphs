#include<iostream>
#include<bits/stdc++.h>
#define size 1000
using namespace std;

int main(){

        FILE *fe = fopen("entity2id.txt","r");
        FILE *fr = fopen("relation2id.txt","r");
        
        if(fe==NULL||fr==NULL){
                printf("error");
                return 0;
        }

        map<string,int> me;
        map<string,int> mr;
        
        int ne,nr;

        char a[size];

        fgets(a,size,fe);        
        
        ne = atoi(a);

        fgets(a,size,fr);
                
        nr = atoi(a);

        for(int i=0;i<ne;i++){
                fgets(a,size,fe);
                char *tok;
                tok = strtok(a,"\t");
                string s(tok);
                me[s]=i;
        }

        for(int i=0;i<nr;i++){
                fgets(a,size,fr);
                char *tok = strtok(a,"\t");
                string s(tok);
                mr[s]=i;
        }
        
        fclose(fr);
        fclose(fe);

        FILE *fp = fopen("test.txt","r");
        FILE *f = fopen("test2id.txt","w");

        while(fgets(a,size,fp)!=0){
                char *tok = strtok(a,"\t");
                string s(tok);
                fprintf(f,"%d\t",me[s]);
                tok = strtok(NULL,"\t");
                string d(tok);
                fprintf(f,"%d\t",me[d]);
                tok = strtok(NULL,"\t");
                tok[strlen(tok)-1]=0;
                string g(tok);;
                fprintf(f,"%d\n",mr[g]);
        }

        return 0;
}
