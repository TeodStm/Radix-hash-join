#include<stdio.h>
#include<time.h>
#include<pthread.h>
#include<semaphore.h>
#include<stdlib.h>
#include<unistd.h>
#include "relation.h"
#include "histogram.h"
#include "JobQueue.h"

#define N_h1 4 //gia thn h1 sunarthsh katakermatismou
#define HashValue2 23
#define NUM_OF_THREADS 3

pthread_mutex_t queueMtx, jobEndMtx, barrMtx;
sem_t sem;
pthread_barrier_t barrier;

pthread_mutex_t relOrdMtx;
int endOfJobs;

JQueue *jq;

//result *resultList;

	/***Tuple methods ***/

tuple* newTuple(uint64_t Tkey, uint64_t Tpayload, uint64_t tableIdx ){ //dhmiourgei ena neo tuple me times Tkey kai Tpayload
	
	tuple* newtuple = malloc( sizeof(struct tuple) );
	
	if( newtuple == NULL ){
		printf("Malloc failed on newTuple\n");
		fflush(stdout);
		exit(-1);
	}
	
	newtuple->key = Tkey;
	newtuple->payload = Tpayload;
	newtuple->intermIdx = tableIdx;
	return newtuple;
	
}

void deleteTuple( tuple* Tuple){ //diagrafh enos tuple
	tuple *tupleToDel = Tuple;
	free(tupleToDel);
}


void printTuple(tuple t){ //ektupwsh enos tuple
	printf("KEY: %ld  -  PAYLOAD: %ld\n", t.key, t.payload );
	fflush(stdout);
}

	/***Relation methods***/
	
relation* initRelation(uint64_t size){ //arxikopoihsh tou table enos relation (apla allocation)
	
	relation* newrelation;
	newrelation = malloc( sizeof(struct relation) );
	
	if( newrelation == NULL ){
		printf("Malloc failed ont newRelation");
		fflush(stdout);
		exit(-2);
	}
	
	newrelation->tuples = malloc( size*sizeof(struct tuple) );
	
	if( newrelation->tuples == NULL ){
		printf("Malloc failed ont newRelation");
		fflush(stdout);
		exit(-2);
	}
	
	newrelation->num_tuples = 0;
	
	//printf("A new relation just initialized.\n");
	//fflush(stdout);
	return newrelation;
}

void deleteRelation(relation* rel){
	int i = 0;
	tuple* delTup;
	
	/*for( i = 0; i < rel->num_tuples; i++){
		delTup = &rel->tuples[i];
		deleteTuple(delTup);
	}*/
	
	free(rel->tuples);
	free(rel);
}

void addTuple(relation* rel, uint64_t k, uint64_t pl, uint64_t idx ){ //prosthetei ena tuple se ena relation
	
	tuple* t = newTuple(k, pl, idx);
	rel->tuples[ rel->num_tuples ] = *t;
	rel->num_tuples = rel->num_tuples + 1;
}

void addTupleIndex(relation* rel, uint64_t k, uint64_t pl, uint64_t intermTableIdx, int index){ //prosthetei ena tuple se ena relation se ena sugkekrimeno shmeio ston pinaka twn tuples
	
	tuple* t = newTuple(k, pl,intermTableIdx );
	rel->tuples[ index ] = *t;
	rel->num_tuples = rel->num_tuples + 1;
}

int relationFillrand(relation* rel, uint64_t size){ //gemizei ena relation me tuxaies times
	time_t t;
	uint64_t rowId;
	uint64_t value;
	
	if( rel->num_tuples == size ){
		printf("relation is already full.\n");
		fflush(stdout);
		return 0; //fail
	}
	srand( (unsigned)time(&t) );
	
	for(int i = 0; i < size; i++){
				rowId = (uint64_t)(i+1); // row id
				value = (uint64_t)(rand() % 200 + 1); //random value
				addTuple( rel, rowId, value, 0 );
	}
	return rel->num_tuples; //success
}

int orderedRelationFill( relation* ordRel, relation* rel, uint64_t* P_hist, uint64_t size){ //vazei se seira ta tuples mia sxeshs (me vash to hash value) se enan neo pinaka xrhsimopoiontas to athroistiko istogramma
	
	int bucket;
	uint64_t rowId;
	uint64_t value;
	uint64_t intermTableIdx;
	
	if( ordRel == NULL || rel == NULL || P_hist == NULL ){
		printf("orderedRelationFill: Some of arguments given was invalid(NULL)\n");
		fflush(stdout);
		return 0;
	}
	
	for( int i = 0; i < rel->num_tuples; i++){
		bucket = hashFunction_1( (int)size, (int)(rel->tuples[i].payload) );
		//printf("bucket index is %d\n",bucket);
		
		rowId = rel->tuples[i].key;
		value = rel->tuples[i].payload;
		intermTableIdx = rel->tuples[i].intermIdx;
		
		addTupleIndex( ordRel, rowId, value, intermTableIdx, P_hist[ bucket ] );
		P_hist[ bucket ] = P_hist[ bucket ] + 1;
	}
	return 1;
	
}

void *orderedRelationFillThrd( void *arg ){
	int bucket, relIdx;
	uint64_t rowId;
	uint64_t value;
	uint64_t intermTableIdx;
	OrderedFillArgs *ordArgs;
	
	ordArgs = (OrderedFillArgs *)arg;
	
	if( ordArgs->newRel == NULL || ordArgs->rel == NULL || ordArgs->pHist == NULL ){
		printf("orderedRelationFill: Some of arguments given was invalid(NULL)\n");
		fflush(stdout);
		pthread_exit(NULL);
	}
	
	for( int i = ordArgs->start; i < ordArgs->end + 1; i++){
		bucket = hashFunction_1( (int)ordArgs->size, (int)(ordArgs->rel->tuples[i].payload) );
		//printf("bucket index is %d\n",bucket);
		
		pthread_mutex_lock(&relOrdMtx);
		relIdx = ordArgs->pHist[bucket];
		ordArgs->pHist[bucket] = ordArgs->pHist[bucket] + 1;
		pthread_mutex_unlock(&relOrdMtx);
		
		rowId = ordArgs->rel->tuples[i].key;
		value = ordArgs->rel->tuples[i].payload;
		intermTableIdx = ordArgs->rel->tuples[i].intermIdx;
		
		addTupleIndex( ordArgs->newRel, rowId, value, intermTableIdx, relIdx );
	}
	pthread_exit(NULL);
}

result* compareBuckets( relation* relA, relation* relB, uint64_t* histA, uint64_t* histB, int hsize){
	//H sunarthsh auth ousiastika vriskei ta zeugaria twn buckets sta opoia prepei na ginoun oi sugkriseis timwn (prokuptoun me thn efarmogh ths hashFunction_1 
	result *resultList;
	int i, rc, noResultsFlag = 0;
	pthread_t *tPool;
	Job *jb;
	JoinArgs jArgs[NUM_OF_THREADS];
	endOfJobs = 0;
	
	if( relA->num_tuples == 0 || relB->num_tuples == 0 ){
		resultList = resultNodeInit();
		return resultList;
	}
	
	pthread_mutex_init(&queueMtx, NULL);/* Initialize mutex for JobQueue */
	pthread_mutex_init(&jobEndMtx, NULL);/* Initialize mutex for endOfJobs flag */
	pthread_mutex_init(&barrMtx, NULL); //Initialize mutex for barrier
	
	sem_init(&sem, 0, 0);
	
	jq = jobQueueInit(hsize); //arxikopoihsh ths ouras twn jobs
	
	/** Desmeush xwrou gia to threadPool **/
	tPool = malloc(NUM_OF_THREADS*sizeof(pthread_t));
	
	if( tPool == 0 ){
		printf("Malloc failed in compareBuckets at thread pool creation\n");
		exit(-2);
	}
	
	for( i = 0; i < NUM_OF_THREADS; i++ ){
		jArgs[i].resList = resultNodeInit();
	}
	
	rc = pthread_barrier_init(&barrier, NULL, NUM_OF_THREADS);
	
	
	for( i = 0; i < NUM_OF_THREADS; i++ ){
		pthread_create( &tPool[i], NULL, joinBucketsThrd, (void*)&jArgs[i]);
	}

	
	for( int i = 0; i < hsize; i++){
		if( histA[i] == 0 || histB[i] == 0 ){ //an kapoio apo ta 2 bucket pou prepei na sugkrithoun einai adeia den ginetai kamia sugkrish
			//printf("Bucket %d of relation A or B is empty(they will not be compared).\n",i);
			//fflush(stdout);
			continue;
		}
		int *bucket, *chain;
		int startIndexA = 0, startIndexB = 0;

		if( i != 0 ){
			for( int j = 0; j < i; j++){ //ta startIndexA kai startIndexB kratane to offset sto opoio ksekinaei to prwto tuple enos kadou
				startIndexA = startIndexA + histA[j];
				startIndexB = startIndexB + histB[j];
			}
		}
		if( histA[i] >= histB[i] ){ //creating index on bucket of relation B
			//printf("Create index on bucket %d of relation B\n",i);
			noResultsFlag = 1;
			bucket = malloc( HashValue2*sizeof( int ) );
			chain = malloc( (histB[i])*sizeof( int ) );

			createIndex( relB, histB, histB[i], hsize, bucket, chain, startIndexB, startIndexB + histB[ i ] ); //me thn klhsh auth dhmiourgeitai ena eurethrio sth sxesh B
			
			jb = jobInit( relA, relB, histA, histB, startIndexA, startIndexB, i, bucket, chain, 1 ); //arxikopoihsh enos neou job
			
			pthread_mutex_lock(&queueMtx); //lock ton mutex ths ouras twn jobs
	
			push(jq, jb); //prosthiki tou neou job sthn oura
			sem_post(&sem);
			//printf("MainThread: jobCounter = %d\n", jq->jobCounter);
			
			pthread_mutex_unlock(&queueMtx); //unlock ton mutex ths ouras twn jobs
			
			//sleep(1);
			/*pthread_mutex_lock(&barrMtx);
			pthread_cond_wait(&barrCond, &barrMtx); //afypnish tou thread
			pthread_mutex_unlock(&barrMtx);*/
			
			//lastNode = joinBuckets(  relA, relB, histA, histB, startIndexA, startIndexB, i, lastNode, bucket, chain, 1 ); //edw ginetai h pragmatikh sugkrish twn 2 kadwn (A sxesh den exei eurethrio - B exei )
			
			/*free(bucket);
			free(chain);*/
		}
		else{  //creating index on bucket of relation A
			noResultsFlag = 1;
			bucket = malloc( HashValue2*sizeof( int ) );
			chain = malloc( (histA[i])*sizeof( int ) );
			createIndex( relA, histA, histA[i], hsize, bucket, chain, startIndexA, startIndexA + histA[ i ] ); //me thn klhsh auth dhmiourgeitai ena eurethrio sth sxesh A
			
			jb = jobInit( relB, relA, histB, histA, startIndexB, startIndexA, i, bucket, chain, 0 ); //arxikopoihsh enos neou job
			
			pthread_mutex_lock(&queueMtx); //lock ton mutex ths ouras twn jobs
			
			push(jq, jb); //prosthiki tou neou job sthn oura
			sem_post(&sem);
			//printf("MainThread: jobCounter = %d\n", jq->jobCounter);
			
			pthread_mutex_unlock(&queueMtx); //unlock ton mutex ths ouras twn jobs

			//lastNode = joinBuckets(  relB, relA, histB, histA, startIndexB, startIndexA, i, lastNode, bucket, chain, 0 );
			
			/*free(buckethread will wait on cond);
			free(chain);*/
		}
	}
	pthread_mutex_lock(&jobEndMtx);
	endOfJobs = 1;
	//printf("To main thread edwse ola ta jobs, endOfJobs = %d !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n", endOfJobs);
	pthread_mutex_unlock(&jobEndMtx);
		
	if( noResultsFlag == 0 ){
		for( i = 0; i < NUM_OF_THREADS; i++ ){
			sem_post(&sem);
		}
	}
	
	
	for( i = 0; i < NUM_OF_THREADS; i++){
		pthread_join( tPool[i], NULL ); //wait for threads to terminate
	}
	
	//printf("Ola ta threads termatisan\n");
	fflush(stdout);
	
	resultList = resultNodeInit();
	
	sem_destroy(&sem);
	pthread_mutex_destroy(&jobEndMtx);
	pthread_mutex_destroy(&queueMtx);
	pthread_mutex_destroy(&barrMtx);
	
	pthread_barrier_destroy(&barrier);
	
	free(jq);
	free(tPool);
	
	return resultListComposition(resultList, jArgs, NUM_OF_THREADS);
	
	//sunenvsh listwn
	//printResultList(resultList);
	
}

void joinBuckets_0( relation* relA, relation* relB, uint64_t* histA, uint64_t* histB, int bucketIndxA, int bucketIndxB, int buckNum, result* resList ){
	
	for( int i = bucketIndxA; i < bucketIndxA + histA[buckNum]; i++){
		for( int j = bucketIndxB; j < bucketIndxB + histB[buckNum]; j++){
			if( relA->tuples[i].payload == relB->tuples[j].payload ){
				printf("(%3ld,%3ld)  -  (%3ld,%3ld)\n", relA->tuples[i].key, relA->tuples[i].payload, relB->tuples[j].key, relB->tuples[j].payload);
				addResultTuple( resList , relA->tuples[i].key, relA->tuples[i].payload, 0 );
				addResultTuple( resList , relB->tuples[j].key, relB->tuples[j].payload, 0 );
			}
			
		} 
	}
}

result* joinBuckets( relation* relA, relation* relB, uint64_t* histA, uint64_t* histB, int bucketIndxA, int bucketIndxB, int buckNum, result* resList, int* bucket, int* chain, int indexedRel ){
	//H sunarthsh auth dexetai enan kado me eurethrio kai enan xwris eurethrio kai sugkrinei ta tuples pou periexoun 
	// opoia tuples vriskei oti tairiazoun gia thn praksh tou join , ta prosthetei sth lista twn apotelesmatwn
	
	int AvalueHashed;
	for( int i = bucketIndxA; i < bucketIndxA + histA[ buckNum ]; i++){
		AvalueHashed = hashFunction_2( HashValue2, relA->tuples[i].payload );
		if( bucket[ AvalueHashed ] == -1 ) continue;
		if( relA->tuples[i].payload == relB->tuples[ bucket[ AvalueHashed ] ].payload){
			//add sto result list
			if( indexedRel == 0 ){ //index is on relation A (indexdRel is 0)
				resList = addResultTuple( resList , relB->tuples[ bucket[ AvalueHashed ] ].key, relB->tuples[ bucket[ AvalueHashed ] ].payload, relB->tuples[ bucket[ AvalueHashed ] ].intermIdx );
				resList = addResultTuple( resList , relA->tuples[i].key, relA->tuples[i].payload, relA->tuples[i].intermIdx );
			}
			else{
				resList = addResultTuple( resList , relA->tuples[i].key, relA->tuples[i].payload, relA->tuples[i].intermIdx );
				resList = addResultTuple( resList , relB->tuples[ bucket[ AvalueHashed ] ].key, relB->tuples[ bucket[ AvalueHashed ] ].payload, relB->tuples[ bucket[ AvalueHashed ] ].intermIdx );
			}
		}
		if( chain[ bucket[ AvalueHashed ] - bucketIndxB ] == -1 ) continue;
		int chainIndx = bucket[ AvalueHashed ] - bucketIndxB;
		while( chain[ chainIndx ] != -1){
			if( relA->tuples[i].payload == relB->tuples[ chain[ chainIndx ] ].payload ){
				//add sto result List
				if( indexedRel == 0 ){ //index is on relation A (indexdRel is 0)
					resList = addResultTuple( resList , relB->tuples[ chain[ chainIndx ] ].key, relB->tuples[ chain[ chainIndx ] ].payload, relB->tuples[ chain[ chainIndx ] ].intermIdx );
					resList = addResultTuple( resList , relA->tuples[i].key, relA->tuples[i].payload, relA->tuples[i].intermIdx );
				}
				else{
					resList = addResultTuple( resList , relA->tuples[i].key, relA->tuples[i].payload, relA->tuples[i].intermIdx );
					resList = addResultTuple( resList , relB->tuples[ chain[ chainIndx ] ].key, relB->tuples[ chain[ chainIndx ] ].payload, relB->tuples[ chain[ chainIndx ] ].payload );
				}
			}
			//chainIndx = chain[ chainIndx ];
			if( chain[ chainIndx ] == -1 ) break;
			chainIndx = chain[ chainIndx ] - bucketIndxB; 
		}
	}
	return resList;
}

void *joinBucketsThrd(void *arg){
	//H sunarthsh auth dexetai enan kado me eurethrio kai enan xwris eurethrio kai sugkrinei ta tuples pou periexoun 
	// opoia tuples vriskei oti tairiazoun gia thn praksh tou join , ta prosthetei sth lista twn apotelesmatwn
	relation *relA, *relB;
	uint64_t *histA, *histB;
	int bucketIndxA, bucketIndxB, bucketId, indexedRel, AvalueHashed, i;
	int *bucket, *chain, start;
	result *resList, *firstNode;
	JoinArgs *jArgs;
	
	jArgs = (JoinArgs*)arg;
	
	resList = jArgs->resList;
	firstNode = jArgs->resList;
	
	//pthread_barrier_wait(&barrier);
		
	while(1){
		//printf("Thread with id: %ld goes to sleep\n", pthread_self());
		sem_wait(&sem);
		//printf("Thread with id: %ld woke up\n", pthread_self());
		
		pthread_mutex_lock(&queueMtx);
		pthread_mutex_lock(&jobEndMtx);
		if( endOfJobs == 1 && jq->jobCounter == 0 ){
			pthread_mutex_unlock(&jobEndMtx);
			pthread_mutex_unlock(&queueMtx);
			break;
		}
		pthread_mutex_unlock(&jobEndMtx);
		
		relA = jq->j[ jq->start ]->relA;
		relB = jq->j[ jq->start ]->relB;
		histA = jq->j[ jq->start ]->histA;
		histB = jq->j[ jq->start ]->histB;
		bucketIndxA = jq->j[ jq->start ]->buckStartA;
		bucketIndxB = jq->j[ jq->start ]->buckStartB;
		bucketId = jq->j[ jq->start ]->bucketId;
		bucket = jq->j[ jq->start ]->bucket;
		chain = jq->j[ jq->start ]->chain;
		indexedRel = jq->j[ jq->start ]->relWithIndex;
		//start = jq->start;
		
		pop(jq);
		
		//printf("job counter is %d from thread with id: %ld\n", jq->jobCounter, pthread_self());
		pthread_mutex_unlock(&queueMtx);
		
		for( i = bucketIndxA; i < bucketIndxA + histA[ bucketId ]; i++){
			AvalueHashed = hashFunction_2( HashValue2, relA->tuples[i].payload );
			if( bucket[ AvalueHashed ] == -1 ) continue;
			if( relA->tuples[i].payload == relB->tuples[ bucket[ AvalueHashed ] ].payload){
				//add sto result list
				if( indexedRel == 0 ){ //index is on relation A (indexdRel is 0)
					resList = addResultTuple( resList , relB->tuples[ bucket[ AvalueHashed ] ].key, relB->tuples[ bucket[ AvalueHashed ] ].payload, relB->tuples[ bucket[ AvalueHashed ] ].intermIdx );
					resList = addResultTuple( resList , relA->tuples[i].key, relA->tuples[i].payload, relA->tuples[i].intermIdx );
				}
				else{
					resList = addResultTuple( resList , relA->tuples[i].key, relA->tuples[i].payload, relA->tuples[i].intermIdx );
					resList = addResultTuple( resList , relB->tuples[ bucket[ AvalueHashed ] ].key, relB->tuples[ bucket[ AvalueHashed ] ].payload, relB->tuples[ bucket[ AvalueHashed ] ].intermIdx );
				}
			}
			if( chain[ bucket[ AvalueHashed ] - bucketIndxB ] == -1 ) continue;
			int chainIndx = bucket[ AvalueHashed ] - bucketIndxB;
			while( chain[ chainIndx ] != -1){
				if( relA->tuples[i].payload == relB->tuples[ chain[ chainIndx ] ].payload ){
					//add sto result List
					if( indexedRel == 0 ){ //index is on relation A (indexdRel is 0)
						resList = addResultTuple( resList , relB->tuples[ chain[ chainIndx ] ].key, relB->tuples[ chain[ chainIndx ] ].payload, relB->tuples[ chain[ chainIndx ] ].intermIdx );
						resList = addResultTuple( resList , relA->tuples[i].key, relA->tuples[i].payload, relA->tuples[i].intermIdx );
					}
					else{
						resList = addResultTuple( resList , relA->tuples[i].key, relA->tuples[i].payload, relA->tuples[i].intermIdx );
						resList = addResultTuple( resList , relB->tuples[ chain[ chainIndx ] ].key, relB->tuples[ chain[ chainIndx ] ].payload, relB->tuples[ chain[ chainIndx ] ].payload );
					}
				}
				//chainIndx = chain[ chainIndx ];
				if( chain[ chainIndx ] == -1 ) break;
				chainIndx = chain[ chainIndx ] - bucketIndxB; 
			}
		}
		
		free(bucket); //start kai oxi jq->start giati exoume orisei apo panw oti start = jq->start
		free(chain); //to idio kai edw
		
		pthread_mutex_lock(&queueMtx);
		pthread_mutex_lock(&jobEndMtx);
		if( endOfJobs == 1 && jq->jobCounter == 0 ){
			pthread_mutex_unlock(&jobEndMtx);
			pthread_mutex_unlock(&queueMtx);
			break;
		}
		pthread_mutex_unlock(&jobEndMtx);
		pthread_mutex_unlock(&queueMtx);
	}
	jArgs->resList = firstNode;
	//printf("To thread me id: %ld termatizei!\n", pthread_self());
	sem_post(&sem);
	pthread_exit(NULL);
}

int createIndex( relation* rel, uint64_t* hist, int bucketSize, int hsize, int* bucket, int* chain, int startIndex, int endIndex){
	//dhmiourgei ena eurethrio tupou hashtable ston kado mias sxeshs
	//oi pinakes chain kai bucket dinontai san orismata adeioi kai auth h sunarthsh tous gemizei me times
	
	int *localBucket, *localChain;
	//*chain = malloc( bucketSize*sizeof(int) ); //o chain exei to megethos tou bucket sto opoio xtizetai to eurethrio
	//*bucket = malloc( HashValue2*sizeof(int) ); //o bucket exei megethos oso kai to euros ths hashFunction 2
	if( chain == NULL || bucket == NULL ){
		printf("Malloc failed on createIndex\n");
		fflush(stdout);
		return 0;
	}
	
	for( int i = 0; i < HashValue2; i++) bucket[i] = -1;
	for( int i = 0; i < bucketSize; i++) chain[i] = -1;
	
	for( int i = endIndex-1; i >= startIndex; i--){ //diasxizoume ton kado apo to telos pros thn arxh
		int bktIndx = hashFunction_2( HashValue2, rel->tuples[i].payload );
		if( bucket[ bktIndx ] == -1 ){ //shmainei oti einai to prwto stoixeio me auto to hash value ara grafoume to offset ston busket
			bucket[ bktIndx ] = i;
			//continue;
		}
		else{
			//int checkIndx = bucket[ bktIndx ];
			int checkIndx = bucket[ bktIndx ] - startIndex;
			while( chain[ checkIndx ] != -1 ){
				//checkIndx = chain[ checkIndx ];
				checkIndx = chain[ checkIndx ] - startIndex;
			}
			chain[ checkIndx ] = i;
		}
	}
	return 1;
}

result* RadixHashJoin( relation* relA, relation* relB){
	
	pthread_t *tPool;
	uint64_t *histA, *histB, *P_histA, *P_histB, histSz;
	HistArgs h[NUM_OF_THREADS];
	int tValues, i;
	result *returnResList;
	/*****Creating histogram *******/
	histSz = 1;
	for( i = 0; i < N_h1; i++){
		histSz = histSz*2;
	}
	
	/*Desmeush xwrou gia ta istogrammata*/
	histA = malloc( histSz*sizeof(uint64_t) );
	histB = malloc( histSz*sizeof(uint64_t) );
	P_histA = malloc( histSz*sizeof(uint64_t) );
	P_histB = malloc( histSz*sizeof(uint64_t) );
	
	if( histA == NULL || histB == NULL || P_histA == NULL || P_histB == NULL ){
		printf("Failed to allocate histogram\n");
		fflush(stdout);
		exit(-1);
	}
	
	for( i = 0; i < histSz; i++ ){ //arxikopoihsh twn istogrammatwn me mhdenikes times
		histA[i] = 0;
		histB[i] = 0;
		P_histA[i] = 0;
		P_histB[i] = 0;
	}
	
	/** Dhmiourgia istogrammatwn me pragmatikes times apo ta relations **/
	tPool = malloc(NUM_OF_THREADS*sizeof(pthread_t));
	
	if( tPool == 0 ){
		printf("Malloc failed in RadixHashJoin at thread pool creation\n");
		exit(-2);
	}
	
	/****RelA****/
	tValues = (int)(relA->num_tuples) / NUM_OF_THREADS;
	
	h[0].start = 0;
	h[0].end = tValues - 1; //-1 logw index kai oxi count
	for( i = 1; i < NUM_OF_THREADS; i++ ){
		h[i].start = h[i-1].end + 1;
		h[i].end = h[i].start + tValues - 1; //-1 logw index kai oxi count
	}
	h[NUM_OF_THREADS-1].end = h[NUM_OF_THREADS-1].end + (int)(relA->num_tuples) % NUM_OF_THREADS;
	
	for( i = 0; i < NUM_OF_THREADS; i++ ){
		h[i].numOfBuckets = histSz;
		h[i].rel = relA;
		h[i].hist = malloc( histSz*sizeof(uint64_t) );
		
		if( h[i].hist == NULL ){
			printf("Failed to allocate small histogram\n");
			fflush(stdout);
			exit(-1);
		}
	}
	
	for( int j = 0; j < NUM_OF_THREADS; j++ ){
		for( i = 0; i < histSz; i++ ){ //arxikopoihsh twn istogrammatwn me mhdenikes times
			h[j].hist[i] = 0;
		}
	}
	
	for( i = 0; i < NUM_OF_THREADS; i++ ){
		pthread_create( &tPool[i], NULL, fillHistTableT, (void*)&h[i]);
	}
	
	for( i = 0; i < NUM_OF_THREADS; i++ ){
		pthread_join( tPool[i], NULL ); //anamonh twn threads mexri na oloklhrwsoun thn ergasia tous
	}
		
	for( i = 0; i < histSz; i++ ){
		for( int j = 0; j < NUM_OF_THREADS; j++ ){
			histA[i] = histA[i] + h[j].hist[i];
		}
	}
		
	/****RelB****/	
	tValues = (int)(relB->num_tuples) / NUM_OF_THREADS;	
		
	h[0].start = 0;
	h[0].end = tValues - 1; //-1 logw index kai oxi count
	for( i = 1; i < NUM_OF_THREADS; i++ ){
		h[i].start = h[i-1].end + 1;
		h[i].end = h[i].start + tValues -1; //-1 logw index kai oxi count
	}
	h[NUM_OF_THREADS-1].end = h[NUM_OF_THREADS-1].end + (int)(relB->num_tuples) % NUM_OF_THREADS;
	
	for( i = 0; i < NUM_OF_THREADS; i++ ){
		h[i].rel = relB;
	}
	
	for( int j = 0; j < NUM_OF_THREADS; j++ ){
		for( i = 0; i < histSz; i++ ){ //arxikopoihsh twn istogrammatwn me mhdenikes times
			h[j].hist[i] = 0;
		}
	}
	
	for( i = 0; i < NUM_OF_THREADS; i++ ){
		pthread_create( &tPool[i], NULL, fillHistTableT, (void*)&h[i]);
	}
	
	for( i = 0; i < NUM_OF_THREADS; i++ ){
		pthread_join( tPool[i], NULL ); //anamonh twn threads mexri na oloklhrwsoun thn ergasia tous
	}
	//free(tPool);
	
	for( i = 0; i < histSz; i++ ){
		for( int j = 0; j < NUM_OF_THREADS; j++ ){
			histB[i] = histB[i] + h[j].hist[i];
		}
	}
	/*fillHistTable( histA, relA, histSz );
	fillHistTable( histB, relB, histSz );*/
	
	fill_P_HistTable( P_histA, histA, histSz );
	fill_P_HistTable( P_histB, histB, histSz );
	
	/***2h parallhlopoihsh apo edw kai katw***/
	
	relation *relAord, *relBord; //Autoi oi pinakes tha exoune tis times twn sxesewn taksinomhmenes kata hashvalue ths hash function 1
	OrderedFillArgs ordArgs[NUM_OF_THREADS];
	relAord = initRelation( relA->num_tuples );
	relBord = initRelation( relB->num_tuples );
	
	pthread_mutex_init(&relOrdMtx, NULL);
	
	/**relA**/
	tValues = (int)(relA->num_tuples) / NUM_OF_THREADS;
	
	ordArgs[0].start = 0;
	ordArgs[0].end = tValues - 1; //-1 logw index kai oxi count
	for( i = 1; i < NUM_OF_THREADS; i++ ){
		ordArgs[i].start = ordArgs[i-1].end + 1;
		ordArgs[i].end = ordArgs[i].start + tValues -1; //-1 logw index kai oxi count
	}
	ordArgs[NUM_OF_THREADS-1].end = ordArgs[NUM_OF_THREADS-1].end + (int)(relA->num_tuples) % NUM_OF_THREADS;
	
	for( i = 0; i < NUM_OF_THREADS; i++ ){
		ordArgs[i].rel = relA;
		ordArgs[i].newRel = relAord;
		ordArgs[i].pHist = P_histA;
		ordArgs[i].size = histSz;
	}	
	
	for( i = 0; i < NUM_OF_THREADS; i++ ){
		pthread_create( &tPool[i], NULL, orderedRelationFillThrd, (void*)&ordArgs[i]);
	}
	
	for( i = 0; i < NUM_OF_THREADS; i++ ){
		pthread_join( tPool[i], NULL ); //anamonh twn threads mexri na oloklhrwsoun thn ergasia tous
	}
	
	/**relB**/
	tValues = (int)(relB->num_tuples) / NUM_OF_THREADS;
	
	ordArgs[0].start = 0;
	ordArgs[0].end = tValues - 1; //-1 logw index kai oxi count
	for( i = 1; i < NUM_OF_THREADS; i++ ){
		ordArgs[i].start = ordArgs[i-1].end + 1;
		ordArgs[i].end = ordArgs[i].start + tValues -1; //-1 logw index kai oxi count
	}
	ordArgs[NUM_OF_THREADS-1].end = ordArgs[NUM_OF_THREADS-1].end + (int)(relB->num_tuples) % NUM_OF_THREADS;
	
	for( i = 0; i < NUM_OF_THREADS; i++ ){
		ordArgs[i].rel = relB;
		ordArgs[i].newRel = relBord;
		ordArgs[i].pHist = P_histB;
		ordArgs[i].size = histSz;
	}
	
	for( i = 0; i < NUM_OF_THREADS; i++ ){
		pthread_create( &tPool[i], NULL, orderedRelationFillThrd, (void*)&ordArgs[i]);
	}
	
	for( i = 0; i < NUM_OF_THREADS; i++ ){
		pthread_join( tPool[i], NULL ); //anamonh twn threads mexri na oloklhrwsoun thn ergasia tous
	}
	free(tPool);
	
	returnResList = compareBuckets( relAord, relBord, histA, histB, histSz); //epistrefei result list
	
	pthread_mutex_destroy(&relOrdMtx);
	deleteRelation(relAord);
	deleteRelation(relBord);
	printf("Prin thn compareBuckets\n");
	
	return returnResList;
}

void printJoinResults(relation* relAid, relation* relBid){ //apla ektupwsh apotelesmatvn ths join
	
	printf("---------------------\n");
	printf("| rowId_1 | rowid_2 |\n");
	printf("---------------------\n");
	
	for(int i = 0; i < relAid->num_tuples; i++){
		for( int j = 0; j < relBid->num_tuples; j++){
			if( relAid->tuples[i].payload == relBid->tuples[j].payload){
				printf("|   %2ld    |    %2ld   |\n", relAid->tuples[i].key, relBid->tuples[j].key );
			}
		}
	}
	printf("---------------------\n");
	
	
}

void printRelation(relation* rel){
	
	if( rel == NULL ){
		printf("printRelation: NULL relation given\n");
		return;
	}
	
	printf("-----------------\n");
	printf("|  KEY  |PAYLOAD|\n");
	printf("-----------------\n");
	for( int i = 0; i < rel->num_tuples; i++){
		printf("|  %2ld   |  %2ld   |\n", rel->tuples[i].key, rel->tuples[i].payload );
		//printTuple(rel->tuples[i] );
	}
	printf("-----------------\n");
}

int hashFunction_2(int h, int value){
	return(value % h);
}


 /////////////////////////////////////
void convertToRowIdRel( relation* idrel,relation* rel){ //gemizei ena adeio table me rowids kai times me vash to prwto column
	
	for( int i = 0; i < rel->num_tuples; i++ ){
		idrel->tuples[i].key = (uint64_t)(i+1); //rowId
		idrel->tuples[i].payload = rel->tuples[i].key; //value
	}
	idrel->num_tuples = rel->num_tuples;
	
	
}

relation* ConvertToRowIdRel(relation* rel){ //dhmiourgei table me rowids me vash to 1o column kai to epistrefei
	relation* newRelId;
	newRelId = initRelation(10);
	
	for( int i = 0; i < rel->num_tuples; i++ ){
		addTuple( newRelId, (uint64_t)(i+1), rel->tuples[i].key, 0);
	}
	newRelId->num_tuples = rel->num_tuples;
	return newRelId;
	
}




