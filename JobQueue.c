#include "relation.h"
#include "JobQueue.h"


JQueue* jobQueueInit(int maxSize){
	JQueue* queue;
	
	queue = malloc( sizeof(JQueue) );
	if( queue == NULL ){
		printf("Malloc failed on jobQueueInit\n");
		exit(-1);
	}
	
	queue->j = malloc( maxSize*sizeof(Job *) ); //pinakas pou tha krataei ta jobs
	if( queue->j == NULL ){
		printf("Malloc failed on jobQueueInit\n");
		exit(-1);
	}
	
	queue->start = 0;
	queue->end = 0;
	queue->jobCounter = 0;
	queue->maxSize = maxSize;
	
	return queue;
	
}

Job *jobInit(relation* relA, relation* relB, uint64_t *histA, uint64_t *histB, int buckStartA, int buckStartB, int bucketId, int *bucket, int *chain, int relWithIndex){
	Job *newJob;
	
	newJob = malloc( sizeof(Job) );
	if( newJob == NULL ){
		printf("Malloc failed on jobInit\n");
		exit(-1);
	}
	
	newJob->relA = relA;
	newJob->relB = relB;
	newJob->histA = histA;
	newJob->histB = histB;
	newJob->buckStartA = buckStartA;
	newJob->buckStartB = buckStartB;
	newJob->bucketId = bucketId;
	newJob->bucket = bucket;
	newJob->chain = chain;
	newJob->relWithIndex = relWithIndex;
	
	return newJob;	
	
}


void push(JQueue *queue, Job *job){
	
	if( queue->jobCounter == queue->maxSize ){// gematos pinakas me jobs
		printf("H oura me ta jobs einai gemath (den egine push)\n");
		exit(-1);		
	}
	
	queue->j[ queue->end ] = job;
	queue->end = queue->end + 1;
	queue->jobCounter = queue->jobCounter + 1;		
	
}

void pop( JQueue *queue ){
	
	//free( queue->j[ queue->start ] );
	queue->start = queue->start + 1;
	queue->jobCounter = queue->jobCounter - 1;
	
}
