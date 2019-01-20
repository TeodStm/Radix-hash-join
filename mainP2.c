#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>
#include<time.h>
#include "infoTable.h"
#include "queryTranslation.h"
#include "queryManagement.h"

#define MAX_RELS 20
#define MAX_FILE_NAME 20

int main(int args, char *argv[]){	
	char nameOfFile[20];
	char *line;
	int  numOfRelations = 0;
	ssize_t read;
	size_t len = 0;
	char **fileNames;
	double start_time, end_time, total_time;
	FILE *fp;

	infoTableCell *infoTable;
	
	fileNames = malloc( MAX_RELS*sizeof(char *) );
	if( fileNames == NULL ){
		printf("Malloc failed on fileNames\n");
		exit(-1);
	}
	for( int i = 0; i < MAX_RELS; i++){
		fileNames[i] = malloc( MAX_FILE_NAME*sizeof(char) );
		if( fileNames[i] == NULL ){
			printf("Malloc failed on fileNames\n");
			exit(-1);
		}
	}
	
	printf("Give the names of files and then type \"Done\":\n");
	while( (read = getline( &line, &len, stdin)) != -1 ){
		
		if( strcmp(line, "Done\n") == 0 ) break;
		
		strncpy( fileNames[numOfRelations], line, read-1);
		fileNames[numOfRelations][read-1] = '\0';

		numOfRelations++;
		if( numOfRelations == 20 ){
			printf("Relations buffer is full\n");
			break;
		}
	}
	
	printf("%d relations given\n", numOfRelations);
	if( numOfRelations == 0 ){
		return 0;
	}
	
	for( int i = 0; i < numOfRelations; i++){
		printf("fileNames[%d] : %s\n", i, fileNames[ i ] );
	}
	
	infoTable = infoTableInit(numOfRelations, fileNames); //apothikeush olwn twn sxesewn sthn mnhmh
	/*for( int i = 0; i < numOfRelations; i++){
		printf(" infoTable[%d] -> row: %ld , cols: %d , dist-values: [ ", i, infoTable[i].rows, infoTable[i].columns);
		for(int d = 0; d < infoTable[i].columns; d++){
			printf("%d(%ld, %ld)  ", infoTable[i].distinctVal[d], infoTable[i].min[d], infoTable[i].max[d]);
		}
		printf("]\n");
	}*/
	printf("---------\n");
	for( int i = 0; i < numOfRelations; i++){
		printf(" infoTable[%d] -> row: %ld , cols: %d , dist-values: [ ", i, infoTable[i].rows, infoTable[i].columns);
		for(int d = 0; d < infoTable[i].columns; d++){
			printf("%d(%ld, %ld)  ", infoTable[i].distinctVal[d], infoTable[i].min[d], infoTable[i].max[d]);
		}
		printf("]\n");
	}
	
	fp = fopen("mySmall.result", "w");
	if(fp == NULL){
		printf("fopen failed in mainP2\n");
		exit(-1);
	}
	
	/////////////////////////////////
	start_time = (double)clock();
	printf("\n\nGive your queries you want to execute\nFor another batch of queries type \"F\"\nIn order to quit type \"Q\":\n\n");
	queryListNode* list;
	list = queryListInit();
	while( (read = getline( &line, &len, stdin)) != -1 ){
		if( strcmp(line, "Q\n") == 0 ) break;
		if( strcmp(line, "F\n") == 0 ){
			/////////
			
			executeQuery( list, infoTable, numOfRelations, fp );
			
			printList(list);
			printf("\n\nGive your queries you want to execute\nFor another batch of queries type \"F\"\nIn order to quit type \"Q\":\n\n");
			//freeList
			queryListDelete( list );
			free( list );
			//newList
			list = queryListInit();
			continue;
		}
		addQuery( list, line );
		printf("type another query:\n");
	}
	end_time = (double)clock();
	fclose(fp);
	total_time = (end_time - start_time) / (double)(CLOCKS_PER_SEC);
	printf("\nTotal time for all queries : %f\n", total_time);
}
