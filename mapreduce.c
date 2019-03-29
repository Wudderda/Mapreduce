#include<stdio.h> 
#include<stdlib.h> 
#include<unistd.h> 
#include<sys/types.h> 
#include<string.h> 
#include<sys/wait.h>
int helper(char * N){
    int n=0;
   for (int i=0; N[i] != '\0'; i++) {
    n = n * 10 + N[i] - '0';
  }
  return n;
}
int main(int argc , char * argv[]) 
{   
    if(argc==3){//mapper
        char za[1023];
        int N=helper(argv[1]);
        int ar[N][2];
        for(int i=0 ; i<N;i++){
            pipe(ar[i]);
        }
        if(fork()>0){
        
            for(int i=0 ;i<N;i++){  //children
                if(fork()==0){
                    for(int j=0 ; j<N ; j++){
                        close(ar[j][1]);
                    } //write ends closed
                    dup2(ar[i][0],0);
                    for(int j=0 ; j<N ; j++){
                        close(ar[j][0]);
                    }
                    char  it[4];
                    sprintf(it, "%d", i);
                    char* argz[3]={argv[2],it,NULL};
                    execv("/home/bs05/e2171338/Desktop/the1_sample_v2/sort/src/Sort_Mapper",argz);
                    exit(0);
                }
            }
            for(int j=0 ; j<N;j++){
                close(ar[j][0]);
            }
            int h=1;
            while(fgets(za,1023,stdin)){
               write(ar[(h-1)%N][1],za,strlen(za));
                h++;
             }
             for(int j=0 ; j<N ; j++){
                close(ar[j][1]);
             }
            for(int j=0 ; j<N ; j++){
                wait(NULL);
             }            
        }
    }
    else if(argc==4){//reducer
        char za[1023];
        int N=helper(argv[1]);
        int ar[N][2];
        int red[N][2];
        int inter[N-1][2];
        for(int i=0 ; i<N;i++){
            pipe(ar[i]);
            pipe(red[i]);
        }
        for(int i=0;i<N-1;i++){
            pipe(inter[i]);
        }
        if(fork()>0){
            for(int i=0 ;i<N;i++){  //children of mappers
                if(fork()==0){
                    for(int j=0 ; j<N ; j++){
                        close(ar[j][1]);//write end closed for mapper
                        close(red[j][0]);// read end closed for reducer
                    } 
                    for(int j=0 ; j<N-1 ; j++){
                        close(inter[j][1]);//write end closed for mapper
                        close(inter[j][0]);// read end closed for reducer
                    } 
                    dup2(red[i][1],1);//stdoutu reducera veriyor
                    dup2(ar[i][0],0);//parenttan gelenleri stdine redirect ediyor
                    for(int j=0 ; j<N ; j++){
                        close(ar[j][0]);
                        close(red[j][1]);
                    }
                    char  it[4];
                    sprintf(it, "%d", i);
                    char* argz[3]={argv[2],it,NULL};
                    execv("/home/bs05/e2171338/Desktop/the1_sample_v2/sort/src/Sort_Mapper",argz);
                    exit(0);
                }
            }
            for(int i=0 ;i<N;i++){  //children of reducers
                if(fork()==0){
                    for(int j=0 ; j<N ; j++){// kullanılmayacak mapper pipeları closed
                        close(ar[j][1]);
                        close(ar[j][0]);
                        close(red[j][1]);
                    } 
                    dup2(red[i][0],0);
                    for(int j=0 ; j<N ; j++){
                        close(red[j][0]);
                    }
                    if(i==0){// sadece ilerideki pipe'ye bağlan
                        for(int j=0 ; j<N-1 ; j++){
                            close(inter[j][0]);
                        }
                        dup2(inter[0][1],1);// stdoutumu bir sonrakine verdim pipe ile 
                        for(int j=0 ; j<N-1 ; j++){
                            close(inter[j][1]);
                        }
                    }
                    else if(i>0 && i<N-1){
                        dup2(inter[i][1],1);//outputumu pipeye yazdım
                        dup2(inter[i-1][0],2);//bir öncekinden geleni stderrime bağladım
                        for(int j=0 ; j<N-1 ; j++){
                            close(inter[j][0]);
                            close(inter[j][1]);
                        }
                    }
                    else{
                        dup2(inter[i-1][0],2);
                        for(int j=0 ; j<N-1 ; j++){
                            close(inter[j][0]);
                            close(inter[j][1]);
                        }
                    }
                    char  it[4];
                    sprintf(it, "%d", i);
                    char* argz[4]={argv[3],it,NULL};
                    execv("/home/bs05/e2171338/Desktop/the1_sample_v2/sort/src/Sort_Reducer",argz);
                    exit(0);
                }
            }
            for(int j=0 ; j<N;j++){
                close(ar[j][0]);
                close(red[j][0]);
                close(red[j][1]);
            }
            for(int j=0 ; j<N-1 ; j++){
                close(inter[j][0]);
                close(inter[j][1]);
            }
            int h=1;
            while(fgets(za,1023,stdin)){
               write(ar[(h-1)%N][1],za,strlen(za)); 
                h++;
             }
             for(int j=0 ; j<N ; j++){
                close(ar[j][1]);
             }
            for(int j=0 ; j<2*N ; j++){
                wait(NULL);
             }            
        }
    }      
}
    

        
