#include<pthread.h>
#include "histogram.h"

int hashFunction_1(int h, int value){
	return(value % h);
}

void fillHistTable( uint64_t* hist, relation* rel, uint64_t size ){
	
	for(int i = 0; i < rel->num_tuples; i++){
		hist[ hashFunction_1( (int)size, rel->tuples[i].payload ) ] = hist[ hashFunction_1( (int)size, rel->tuples[i].payload ) ] + 1;
	}
	
}

int fill_P_HistTable( uint64_t* P_hist, uint64_t* hist, uint64_t size ){
	
	if( hist == NULL || P_hist == NULL){
		printf("fill_P_HistTable: histogram tables given were NULL\n");
		fflush(stdout);
		return 0;
	}
	
	P_hist[0] = 0;
	
	for( int i = 1; i < size; i++ ) P_hist[i] = P_hist[i-1] + hist[i-1];
	return 1;
}

void *fillHistTableT( void *args ){
	HistArgs *hArgs = args;
	
	for(int i = hArgs->start; i < hArgs->end + 1; i++){
		hArgs->hist[ hashFunction_1( (int)hArgs->numOfBuckets, hArgs->rel->tuples[i].payload ) ] = hArgs->hist[ hashFunction_1( (int)hArgs->numOfBuckets, hArgs->rel->tuples[i].payload ) ] + 1;
	}
	pthread_exit(NULL);
	//free(hArgs);
}
