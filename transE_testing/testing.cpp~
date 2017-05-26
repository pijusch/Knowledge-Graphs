#include<iostream>
#include<bits/stdc++.h>
#define size 100000
#define latent_size 100

using namespace std;

float rel_embeddings[size][100+10];
float ent_embeddings[size][100+10];

int nr,ne;

float find_score(int head,int tail,int relation){

        float score=0;

        for(int i=0;i<latent_size;i++){
            score+= fabs(ent_embeddings[head][i]+rel_embeddings[relation][i]-ent_embeddings[tail][i]);
        }

        return score;

}

int sort_compare(const void *i,const void *j){

        float *a = (float *)i;
        float *b = (float *)j;

        if(a[0]>b[0]) return 1;
        else if(a[0]<b[0]) return -1;
        else return 0;
}


int find_hit(int num_hit,int head,int tail, int relation){

    int hit_flag=0;

    float testing_result[nr][2];

    for(int i=0;i<nr;i++){
        testing_result[i][0] = find_score(head,tail,i);
        testing_result[i][1] = i;
    }

    qsort(testing_result,nr,sizeof(testing_result[0]),sort_compare);

    for(int i=0;i<num_hit;i++){
        if((float)relation == testing_result[i][1]) hit_flag=1;
    }

    return hit_flag;

}


int main(){

    FILE *fr = fopen("rel_embeddings.txt","r");
    FILE *fe = fopen("ent_embeddings.txt","r");


    if(fr==NULL||fe==NULL){

            printf("error");
            return 0;

    }

    char str[size];


    while(fgets(str,size,fr)){
        int in=0;
        char *tok;
        tok = strtok(str," ");
        while(tok!=NULL){
            rel_embeddings[nr][in++]=atof(tok);
            tok = strtok(NULL," ");
        }
        nr++;
    }

    while(fgets(str,size,fe)){
        int in=0;
        char *tok;
        tok = strtok(str," ");
        while(tok!=NULL){
            ent_embeddings[ne][in++]=atof(tok);
            tok = strtok(NULL," ");
        }
        ne++;
    }

    printf("done reading the embeddings\n\n");

    printf("running the testing data\n\n");

    fclose(fe);
    fclose(fr);

    FILE *ftest = fopen("test2id.txt","r");

    if(ftest==NULL){
        printf("error @testing file");
    }

    int nt=0;
    int numer=0;

    while(fgets(str,size,ftest)){
        nt++;

        char *tok;
        int input[3],in=0;
        tok = strtok(str,"\t");
        while(tok!=NULL){
            input[in++] = atoi(tok);
            tok = strtok(NULL,"\t");
        }


        numer+=find_hit(1,input[0],input[1],input[2]);


    }
    printf("%f",numer*100.0/nt);

    return 0;
}
