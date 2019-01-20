#ifndef QUERYMANAGEMENT_H
#define QUERYMANAGEMENT_H
#define MAX_RELATIONS 4


typedef struct tempResultsTable{
	struct temporaryResults **tempTable; //mporoume na exoume to polu 2 endiamesous pinakes
	int count; //metraei to trexon plhthos endiameswn pinakwn
}tempResultsTable;


typedef struct temporaryResults{
	uint64_t **rowIds;
	int *counts;
	int tableSize;
}temporaryResults;

/*Execution of the Query*/
void executeQuery(queryListNode* startNode, infoTableCell* infoTable, int infoTSize, FILE *fp);

/*Values parsing*/
int* relsParse(char* relations, int infoTSize, int *spaceCount);
int projParse( char* projections, int* proj_idx_rel, int* proj_idx_col, infoTableCell* infoTable, int relsCount, int* rels_idx );
char** predParse(char* predicates, int *predCount );

/*Predicates order*/
char** optPredOrder( infoTableCell* infoTable, char** preds, int predCount, int* rels_idx );

/*Execution of the predicate*/
void filterExecute(infoTableCell* infoTable, int infoTidx, int infoTcol, char filterType, uint64_t filterNum, tempResultsTable* tempResTable, int rel);
tempResultsTable* joinExecute(infoTableCell* infoTable, tempResultsTable* tempResTable, int *rel_idx, int rel1, int rel2, int col1, int col2);
tempResultsTable* joinSameRel(infoTableCell* infoTable, tempResultsTable* intermRes, int* rels_idx, int rel1, int col1, int col2);
tempResultsTable* joinPrevRels(infoTableCell* infoTable, tempResultsTable* tempResTable, int *rel_idx, int rel1, int rel2, int col1, int col2);

/*Check Sums*/
void projectionsResults(infoTableCell *infoTable, temporaryResults *intermRes, int *proj_idx_rel, int *proj_idx_col, int projSize, int *rels_idx, FILE *fp );

/*temporaryResults*/
temporaryResults* temporaryResultsInit();
void temporaryResultsDelete( temporaryResults* tempResTable );

/*tempResultsTable*/
tempResultsTable* tempResultsTableInit();


#endif
