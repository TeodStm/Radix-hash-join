#include "result.h"
#include "relation.h"
//#define buffSize 683
//#define buffSize 65536
#define buffSize 10000


result* resultNodeInit( void ){ //arxikopoihsh enos komvou ths listas twn apotelesmatwn
	result* newNode;
	newNode = malloc( sizeof(struct result) ); //allocate struct 
	if( newNode == NULL ){
		printf("Malloc failed on resultNodeInit\n");
		fflush(stdout);
		exit(-3);
	}
	
	newNode->bufferA = malloc( buffSize*sizeof( struct tuple ) ); //allocate buffers of tuples in struct
	//newNode->bufferB = malloc( buffSize*sizeof( struct tuple ) ); 
	if( newNode->bufferA == NULL ){
		printf("Malloc failed on resultNodeInit\n");
		fflush(stdout);
		exit(-3);
	}
	
	newNode->count = 0;
	newNode->next = NULL;
	return newNode;
}

result* resultListComposition(result* resultList, JoinArgs jArgs[], int size){
	int i, j, write;
	result *firstNode, *newNode;
	
	printf("Mphke sthn resultListComposition\n");
	
	firstNode = resultList;
	write = 0;
	
	for(i = 0 ; i < size; i++){ //gia kathe lista thread
		while( jArgs[i].resList != NULL ){ //gia kathe komvo ths listas tou thread
			for(j = 0; j < jArgs[i].resList->count; j++){ //gia kathe stoixeio sto buffer
				if( resultList->count < buffSize ){ //an xwraei tuple o trexon komvos ths megalhs listas
					resultList->bufferA[write] = jArgs[i].resList->bufferA[j];
					write++;
					resultList->count = resultList->count + 1;
				}
				else{//o trexon komvos ths megalhs listas exei gemisei
					newNode = resultNodeInit();
					write = 0;
					newNode->bufferA[write] = jArgs[i].resList->bufferA[j];
					write++;
					newNode->count = newNode->count + 1;
					resultList->next = newNode;
					resultList = newNode;		
				}
			}
			jArgs[i].resList = jArgs[i].resList->next;
		}
	}
	return firstNode;
}

//void addResultTuple( result* resNode , uint64_t rowidA, uint64_t valueA, uint64_t intermTableIdx){ //prosthetei ena tuple (apotelsma join) sth lista twn apotelesmatwn	
result* addResultTuple( result* resNode , uint64_t rowidA, uint64_t valueA, uint64_t intermTableIdx){ //prosthetei ena tuple (apotelsma join) sth lista twn apotelesmatwn	
	result* tmp;
	tmp = resNode;
	 
	if( resNode->count == buffSize ){ //o buffer exei gemisei se auto ton komvo
		result* newResNode;
		newResNode = resultNodeInit();
		tuple* tA = newTuple(rowidA, valueA, intermTableIdx);
		newResNode->bufferA[ newResNode->count ] = *tA;
		//newResNode->count = newResNode->count + 1;
		newResNode->count = 1;
		newResNode->next = NULL;
		resNode->next = newResNode;
		resNode = newResNode;
		free(tA);
	}
	else{ //xwraei ston trexonta komvo
		tuple* tA = newTuple(rowidA, valueA, intermTableIdx);
		resNode->bufferA[ resNode->count ] = *tA;
		resNode->count = resNode->count + 1;
		free(tA);
	}
	return resNode;
}

void printResultList( result* resList ){ //ektupwsh ths listas apotelesmatwn ths join
	printf("    Printing results from result List (count : %ld)\n", resList->count);
	result* tmp;
	while(1){
		for( int i = 0; i < resList->count; i = i+2){
			printf("(%3ld,%3ld)  -  (%3ld,%3ld)\n", resList->bufferA[i].key, resList->bufferA[i].payload, resList->bufferA[i+1].key, resList->bufferA[i+1].payload);
		}
		if( resList->next == NULL ) break;
		resList = resList->next;
	}
}




