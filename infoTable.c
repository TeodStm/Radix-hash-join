#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>
#include "infoTable.h"

infoTableCell *infoTableInit(int size, char **files){
	
	infoTableCell* table;
	FILE *fp;
	uint64_t *buffer, *distBuf;
	int write, flgWrite;
	table = malloc( size*sizeof(struct infoTableCell) );
	
	if( table == NULL ){
		printf("Malloc failed on infoTableInit\n");
		exit(-1);
	}
	
	buffer = malloc( 2*sizeof(uint64_t) ); //rows-columns
	for( int i = 0; i < size; i++){ //gia kathe sxesh
		
		fp = fopen(files[i], "r");
		if( fp == NULL){
			printf("Not such file(%s)\n", files[i]);
			exit(-2);
		}
		int read = fread(buffer, sizeof(uint64_t), 2, fp);
		table[i].rows = buffer[0];
		table[i].columns = buffer[1];
		table[i].colsPtr = malloc( table[i].columns*sizeof(uint64_t *) );
		table[i].distinctVal = malloc( table[i].columns*sizeof(uint32_t) ); /**********/
		table[i].max = malloc( table[i].columns*sizeof(uint64_t) );
		table[i].min = malloc( table[i].columns*sizeof(uint64_t) );
		
		if( table[i].colsPtr == NULL || table[i].distinctVal == NULL ){
			printf("Malloc failed at infoTableInit\n");
			exit(-9);
		}
		
		for( int j = 0; j < table[i].columns; j++){ //gia kathe sthlh
			uint64_t max, min;
						
			table[i].colsPtr[j] = malloc( table[i].rows*sizeof(uint64_t) );
			//distBuf = malloc( table[i].rows*sizeof(uint64_t) );
			write = 0;
			for(int r = 0; r < table[i].rows; r++){ //gia kathe stoixeio sthlhs (mporoume na kanoume ena fread gia kathe column)
				read = fread( &table[i].colsPtr[j][r], sizeof(uint64_t), 1, fp );
				if( read != 1 ){
					printf("Error on fread\n");
					exit(-3);
				}
				/***min-max***/
				if(r == 0){
					max = table[i].colsPtr[j][r];
					min = table[i].colsPtr[j][r];
				}
				else{
					if( table[i].colsPtr[j][r] > max){
						max = table[i].colsPtr[j][r];
					}
					if( table[i].colsPtr[j][r] < min){
						min = table[i].colsPtr[j][r];
					}
				}
				
				/**********************
				flgWrite = 0;
				for( int k = 0; k < write; k++ ){
					if( table[i].colsPtr[j][r] == distBuf[k] ){
						flgWrite = 1;
						break;
					}
				}
				if( flgWrite == 0 ){
					distBuf[write] = table[i].colsPtr[j][r];
					write++;
				}
				********************/
			}
			table[i].distinctVal[j] = (uint32_t) write; //counter pou metraei poses einai oi diakrites times se kathe sthlh tou infoTable
			table[i].min[j] = min;
			table[i].max[j] = max;
			//free(distBuf);
			
		}
		
		
		fclose(fp);
	}
	free(buffer);
	infoTableDistValues(table, size);
	return table;
}


void infoTableDistValues(infoTableCell* infoTable, int size){
	
	uint64_t vectorSize;
	uint32_t distValSum = 0;
	
	for( int i = 0; i < size; i++ ){ //gia kathe sxesh
		
		for( int j = 0; j < infoTable[i].columns; j++ ){ //gia kathe column ths sxeshs
			
			vectorSize = infoTable[i].max[j] - infoTable[i].min[j] + 1;
			uint32_t bitVector[vectorSize];
			for(int b = 0; b < vectorSize; b++){ //arxikopoihsh tou bitVector
				bitVector[b] = 0;
			}
			
			for( int r = 0; r < infoTable[i].rows; r++ ){ //gia kathe stoixeio ths kolwnas
				bitVector[ infoTable[i].colsPtr[j][r] - infoTable[i].min[j] ] = 1;
			}
			distValSum = 0;
			for( int v = 0; v < vectorSize; v++ ){ //metrame poses einai oi diakrites times(dhladh tous assous ston bitVector)
				if( bitVector[v] == 1 ) distValSum++;
			}
			infoTable[i].distinctVal[j] = distValSum;			
		}
	}
}
