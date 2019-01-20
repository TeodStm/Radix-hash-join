#ifndef RESULT_H
#define RESULT_H

#include<stdio.h>
#include<stdint.h>
#include "JobQueue.h"

typedef struct result{
	struct tuple* bufferA;
	//struct tuple* bufferB;
	struct result* next;
	uint64_t count;
}result;



result* resultNodeInit( void );
//void addResultTuple( result*, uint64_t, uint64_t, uint64_t);
result* addResultTuple( result*, uint64_t, uint64_t, uint64_t);
void printResultList( result* );

result* resultListComposition(result* resultList, JoinArgs jArgs[], int size);

#endif
