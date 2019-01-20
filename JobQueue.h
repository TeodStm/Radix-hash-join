#ifndef JOBQUEUE_H
#define JOBQUEUE_H
#include<stdio.h>
#include "result.h"

//Job struct
typedef struct Job{
	struct relation *relA;
	struct relation *relB;
	uint64_t *histA;
	uint64_t *histB;
	int buckStartA;
	int buckStartB;
	int bucketId;
	int *bucket;
	int *chain;
	int relWithIndex;
} Job;

typedef struct JQueue{
	Job **j; //?
	int start;
	int end;
	int jobCounter;
	int maxSize;
} JQueue;

typedef struct JoinArgs{
	struct result *resList;
}JoinArgs;

/**JobQueue**/
JQueue* jobQueueInit(int maxSize);
void push(JQueue *queue, Job *job);
void pop( JQueue *queue );

/**Job**/
Job *jobInit(struct relation* relA, struct relation* relB, uint64_t *histA, uint64_t *histB, int buckStartA, int buckStartB, int bucketId, int *bucket, int *chain, int relWithIndex);

#endif
