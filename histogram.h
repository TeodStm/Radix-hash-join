#ifndef HISTOGRAM_H
#define HISTOGRAM_H

#include<stdio.h>
#include "relation.h"

typedef struct HistArgs{
	uint64_t *hist;
	relation *rel;
	int numOfBuckets;
	int start;
	int end;
} HistArgs;

void fillHistTable(uint64_t*, relation*, uint64_t);
int hashFunction_1(int, int);
int fill_P_HistTable( uint64_t*, uint64_t*, uint64_t );
void *fillHistTableT( void *args );

#endif
