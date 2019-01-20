#ifndef INFOTABLE_H
#define INFOTABLE_H

#include<stdio.h>
#include<stdlib.h>

typedef struct infoTableCell{
	uint32_t columns;
	uint64_t rows;
	uint64_t **colsPtr;
	uint32_t *distinctVal;
	uint64_t *max;
	uint64_t* min;
}infoTableCell;


infoTableCell *infoTableInit( int size, char **files );
void infoTableDistValues(infoTableCell* infoTable, int size);

#endif
