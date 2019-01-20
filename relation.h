#ifndef RELATION_H
#define RELATION_H

#include<stdlib.h>
#include<stdint.h>
#include "result.h"

/**STRUCTS**/

//tuple struct
typedef struct tuple{
	uint64_t key; //rowId
	uint64_t payload; 
	uint64_t intermIdx; //index ston endiameso pinaka. Den tha xrhsimopoieitai panta
}tuple;


//relation struct
typedef struct relation{
	tuple *tuples;
	uint64_t num_tuples;
}relation;

/***Tuple methods***/

//tuple* newTuple(uint64_t, uint64_t);
tuple* newTuple(uint64_t, uint64_t, uint64_t);
void deleteTuple(tuple*);
void printTuple(tuple);


/***Relation methods***/

relation* initRelation(uint64_t);
void deleteRelation(relation* rel);

//void addTuple(relation*, uint64_t, uint64_t);
void addTuple(relation*, uint64_t, uint64_t, uint64_t);
//void addTupleIndex(relation*, uint64_t, uint64_t, int);
void addTupleIndex(relation*, uint64_t, uint64_t, uint64_t, int);

int relationFillrand(relation*, uint64_t);
int orderedRelationFill( relation*, relation*, uint64_t*, uint64_t);
result* compareBuckets( relation*, relation*, uint64_t*, uint64_t*, int);

//void joinBuckets( relation* relA, relation* relB, uint64_t* histA, uint64_t* histB, int bucketIndxA, int bucketIndxB, int buckNum, result* resList, int* bucket, int* chain, int indexedRel );
result* joinBuckets( relation* relA, relation* relB, uint64_t* histA, uint64_t* histB, int bucketIndxA, int bucketIndxB, int buckNum, result* resList, int* bucket, int* chain, int indexedRel );
int createIndex( relation* rel, uint64_t* hist, int bucketSize, int hsize, int* bucket, int* chain, int startIndex, int endIndex);
void *joinBucketsThrd(void*); // h sunarthsh tou thread

void printRelation(relation*);
void convertToRowIdRel( relation*,relation*);
relation* ConvertToRowIdRel(relation*);
void printJoinResults(relation*, relation*);

int hashFunction_2(int h, int value);

result* RadixHashJoin( relation*, relation*);

#endif
