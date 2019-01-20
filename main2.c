#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include "result.h"
#include "relation.h"
#include "histogram.h"

#define RAsize 100
#define RBsize 100
#define bucketSz 104856 /*L2 cache:1024Kb , L3 cache:6144Kb (intel-i5 4570)*/
//#define N_h1 2 //gia thn h1 sunarthsh katakermatismou

int main( int argc, char* argv[] ){
	
	relation *relA, *relB;
	result* JoinResults;
	
	relA = initRelation(RAsize); //arxikopoihsh 2 sxesewn
	relB = initRelation(RBsize);
	int r;
	
	if( (r = relationFillrand(relA, (uint64_t)RAsize )) == 0 ){ //Gemisma sxeshs A me tuxaies times
		exit(r);
	}
	printf("A: r = %d\n",r);
	//getchar();
	sleep(1); //ayto ginetai gt dinetai sthn rand ws seed to time kai evgaze idies times stis 2 sxeseis
	
	if( (r = relationFillrand(relB, (uint64_t)RBsize )) == 0 ){ //Gemisma sxeshs B me tuxaies times
		exit(r);
	}
	
	JoinResults = RadixHashJoin( relA, relB );
	printResultList( JoinResults );
	
}
