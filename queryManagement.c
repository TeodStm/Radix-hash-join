#include<stdio.h>
#include<stdlib.h>
#include<stdint.h>
#include<string.h>
#include<unistd.h>
#include "infoTable.h"
#include "queryTranslation.h"
#include "queryManagement.h"
#include "relation.h"
#include "result.h"


void executeQuery(queryListNode* startNode, infoTableCell* infoTable, int infoTSize, FILE *fp){
	
	//temporaryResults* intermRes;
	tempResultsTable *intermResT;
	
	while( startNode != NULL ){ //gia kathe query (kathe komvos ths listas periexei ena query)
		//intermRes = temporaryResultsInit();
		/*arxikopoihsh domhs endiameswn apotelesmatwn*/
		intermResT = tempResultsTableInit();
		intermResT->tempTable[0] = temporaryResultsInit();
		intermResT->tempTable[1] = temporaryResultsInit();
		//intermResT->count = 1;
		
		/* RELATIONS */
		int *rels_idx, k, spaceCount = 0, spCount = 0;
		rels_idx = relsParse( startNode->relations, infoTSize, &spaceCount );
		
		/* PROJECTIONS */
		int *proj_idx_rel, *proj_idx_col, projSize = 0;
		
		for(k = 0; k < strlen(startNode->projections); k++){  //ta projections tha isountai me ta kena pou yparxoun meta3y tous + 1
			if(startNode->projections[k] == ' '){
				spCount++;
			}
		}
		proj_idx_rel = malloc((spCount+1)*sizeof(int)); //akrivhs arithmos thesewn pou desmeuontai gia ola ta relations twn projections
		proj_idx_col = malloc((spCount+1)*sizeof(int)); //akrivhs arithmos thesewn pou desmeuontai gia ola ta columns twn projections
		
		if( proj_idx_rel == NULL || proj_idx_col == NULL ){
			printf("Malloc failed in executeQuery\n");
			exit(-1);
		}
		
		projSize = projParse( startNode->projections, proj_idx_rel, proj_idx_col, infoTable, spaceCount, rels_idx );
		
		/* PREDICATES */
		char **preds, *token;
		int predCount = 0;
		char dot[2] = ".";
		preds = predParse( startNode->predicates, &predCount );
		int predCheck[predCount][4], predCheckIdx = 0, c, predCheckFlg = 0;
		
		
		/** optimization ths seiras ekteleshs twn predicates**/
		preds = optPredOrder( infoTable, preds, predCount, rels_idx ); //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		
		for( int p = 0; p < predCount; p++){ //gia kathe predicate
			/** Ginetai parse to Filter predicate kai meta kaleitai sunarthsh gia thn ektelesh tou **/
			char *ret;
			ret = strstr( preds[p], dot );
			if( ret == NULL ){
				printf("No relations given in predicate\n");
			}
			ret = strstr( ret+1, dot );
			
			if( ret == NULL ){ // kathgorhma filtrou
				printf("Kathgorhma filtrou\n");
				int rel, col, filterNum;
				char compType, *tmpFilter; //h tmpFilter krataei to predicate(string) gia na ginei h strtok
				
				tmpFilter = malloc( (strlen(preds[p]) + 1)*sizeof(char) );
				if( tmpFilter == NULL ){
					printf("Malloc failed in executeQuery\n");
					exit(-1);
				}
				
				strcpy( tmpFilter, preds[p] );
				
				token = strtok(tmpFilter, ".");
				rel = atoi(token);
				if( rel == 0 && strcmp(token, "0") != 0 ){
					printf("Wrong input for predicates\n");
					exit(-1);
				}
				if( rel > spaceCount ){
					printf("Wrong input for predicates for relation\n");
					exit(-1);
				}
				
				token = strtok( NULL, "<=>" );
				col = atoi(token);
				if( col == 0 && strcmp(token, "0") != 0 ){
					printf("Wrong input for predicates\n");
					exit(-1);
				}
				if( col >= infoTable[ rels_idx[rel] ].columns ){
					printf("Wrong input for predicates for column\n");
					exit(-1);					
				}
				
				token = strtok( NULL, "<=>" );
				filterNum = atoi(token);
				if( filterNum == 0 && strcmp( token, "0" ) != 0 ){
					printf("Wrong input for predicates\n");
					exit(-1);
				}
				
				for( int l = 0; l < strlen(preds[p]); l++){
					if( preds[p][l] == '<' || preds[p][l] == '=' || preds[p][l] == '>' ){
						compType = preds[p][l];
						break;
					}	
				}
				/*****************************************/				
				printf("---->rel is: %d\n", rel);
				printf("---->col is: %d\n", col);
				printf("---->comType is: %c\n", compType);
				printf("---->filterNum is: %d\n\n", filterNum);
				/*****************************************/
				//filterExecute( infoTable,  rels_idx[rel], col, compType, (uint64_t) filterNum, intermRes, rel );
				filterExecute( infoTable,  rels_idx[rel], col, compType, (uint64_t) filterNum, intermResT, rel );
				
				/*for(int m = 0; m < intermRes->counts[ rel ]; m++){
					printf(">>>%ld\n", intermRes->rowIds[ rel ][m]);   //Emfanish endiamesou pinaka meta apo predicate filtrou
				}*/
			}
			else{ //kathgorhma suzeu3hs
				printf("Kathgorhma syzeu3hs\n");
				printf("join-> %s\n", preds[p]);
				/** Ginetai parse to join predicate kai meta kaleitai sunarthsh gia thn ektelesh tou **/
				int rel1, rel2, col1, col2, filterNum;
				char *tmpJoin;
				
				tmpJoin = malloc( (strlen(preds[p]) + 1)*sizeof(char) );
				if( tmpJoin == NULL ){
					printf("Malloc failed in executeQuery\n");
					exit(-1);
				}
				strcpy( tmpJoin, preds[p] );
				
				token = strtok(tmpJoin, "."); //Pairnoume thn prwth sxesh tou join
				rel1 = atoi(token);
				if( rel1 == 0 && strcmp(token, "0") != 0 ){
					printf("Wrong input for join predicate\n");
					exit(-1);
				}
				if( rel1 > spaceCount ){
					printf("Wrong input for join predicates for relation\n");
					exit(-1);
				}
				
				token = strtok(NULL, "="); //Pairnoume thn prwth kolwna ths 1hs sxeshs tou join
				col1 = atoi(token);
				if( col1 == 0 && strcmp(token, "0") != 0 ){
					printf("Wrong input for join predicate\n");
					exit(-1);
				}
				if( col1 >= infoTable[ rels_idx[rel1] ].columns ){
					printf("Wrong input for join predicates for column\n");
					exit(-1);					
				}
				
				token = strtok(NULL, "."); //Pairnoume thn deuterh sxesh tou join
				rel2 = atoi(token);
				if( rel2 == 0 && strcmp(token, "0") != 0 ){
					printf("Wrong input for join predicate\n");
					exit(-1);
				}
				if( rel2 > spaceCount ){
					printf("Wrong input for join predicates for relation\n");
					exit(-1);
				}
				
				token = strtok(NULL, "="); //Pairnoume thn deuterh kolwna ths 2hs sxeshs tou join
				col2 = atoi(token);
				if( col2 == 0 && strcmp(token, "0") != 0 ){
					printf("Wrong input for join predicate\n");
					exit(-1);
				}
				if( col2 >= infoTable[ rels_idx[rel2] ].columns ){
					printf("Wrong input for join predicates for column\n");
					exit(-1);					
				}
				
				/**********Idies sxeseis***********/
				predCheckFlg = 0;
				for( c = 0; c < predCheckIdx; c++ ){
					/*****Idies sxeseis kai kolones*****/
					if( (predCheck[c][0] == rel1 && predCheck[c][1] == col1 && predCheck[c][2] == rel2 && predCheck[c][3] == col2) || (predCheck[c][0] == rel2 && predCheck[c][1] == col2 && predCheck[c][2] == rel1 && predCheck[c][3] == col1) ){
						predCheckFlg = 1;
						break;
					}
					
					/** Mono idies sxeseis **/
					else if( (predCheck[c][0] == rel1 && predCheck[c][2] == rel2) || (predCheck[c][0] == rel2 && predCheck[c][2] == rel1) ){
						//intermRes = joinPrevRels(infoTable, intermRes, rels_idx, rel1, rel2, col1, col2);
						intermResT = joinPrevRels(infoTable, intermResT, rels_idx, rel1, rel2, col1, col2);
						predCheckFlg = 2;
						break;
					}
				}
				if( predCheckFlg == 1 || predCheckFlg == 2) continue;
				else{
					predCheck[predCheckIdx][0] = rel1;
					predCheck[predCheckIdx][1] = col1;
					predCheck[predCheckIdx][2] = rel2;
					predCheck[predCheckIdx][3] = col2;
					
					predCheckIdx++;
				}
				/**********Idies sxeseis***********/
				
				
				/***********************/			
				printf("<---->rel1 is: %d\n", rel1);
				printf("<---->col1 is: %d\n", col1);
				printf("<---->rel2 is: %d\n", rel2);
				printf("<---->col2 is: %d\n\n", col2);
				/***********************/
				
				if( rel1 != rel2 ){ /***Join meta3y diaforetikwn sxesewn me th xrhsh ths synarthshs RadixJoin***/
					//intermRes = joinExecute(infoTable, intermRes, rels_idx, rel1, rel2, col1, col2);
					intermResT = joinExecute(infoTable, intermResT, rels_idx, rel1, rel2, col1, col2);
				}
				else{ /***Filtro meta3y ths idias sxeshs alla diaforetikwn kolonwn***/ 
					//intermRes = joinSameRel(infoTable, intermRes, rels_idx, rel1, col1, col2);
					intermResT = joinSameRel(infoTable, intermResT, rels_idx, rel1, col1, col2);
				}
				
				/*for(int m = 0; m < intermRes->counts[ rel1 ]; m++){
					printf(">>>%ld\n", intermRes->rowIds[ rel1 ][m]);   //Emfanish endiamesou pinaka meta apo predicate filtrou
				}*/
				/*for(int x = 0; x < MAX_RELATIONS; x++){
					if( intermRes->counts[x] != -1 ){
						for(int J = 0; J < intermRes->counts[x]; J++){
							printf("intermRes->rowIds[%d][%d] is: %ld\n", x, J, intermRes->rowIds[x][J] );
						}
					}
				}*/
			}
		}
		projectionsResults(infoTable, intermResT->tempTable[0], proj_idx_rel, proj_idx_col, spCount+1, rels_idx, fp);
		
		startNode = startNode->next;
		temporaryResultsDelete( intermResT->tempTable[0] );
		free(intermResT->tempTable);
		free(intermResT);
		intermResT = NULL;
		free(proj_idx_rel);
		free(proj_idx_col);
	}
}

void projectionsResults(infoTableCell *infoTable, temporaryResults *intermRes, int *proj_idx_rel, int *proj_idx_col, int projSize, int *rels_idx, FILE *fp ){
	int i, j;
	uint64_t sum = 0;
	
	printf("\nsum: ");
	for(i = 0; i < projSize; i++){
		for(j = 0; j < intermRes->counts[ proj_idx_rel[i] ]; j++){
			sum = sum + infoTable[ rels_idx[proj_idx_rel[i]] ].colsPtr[ proj_idx_col[i] ][ intermRes->rowIds[ proj_idx_rel[i]][j] ]; /*Athroismata elegxou*/
		}
		if( sum == 0 ){
			printf("NULL ");
			fwrite("NULL", 1, 4, fp);
			if( i != projSize - 1){
				fwrite(" ", 1, 1, fp);
			}
		}
		else{			
			printf("%ld ", sum);
			fprintf(fp, "%ld", sum);
			if( i != projSize - 1){
				fwrite(" ", 1, 1, fp);
			}
			sum = 0;
		}
	}
	fwrite("\n", 1, 1, fp);
	printf("\n\n");
}

tempResultsTable* joinExecute(infoTableCell* infoTable, tempResultsTable* tempResTable, int *rel_idx, int rel1, int rel2, int col1, int col2){
	relation *relA, *relB;
	int i, j, c, results, listSum, write, write1, emptyT0;
	result *JoinResults, *tmpResults, *tmpNext;
	temporaryResults* newTempResTable;
	uint64_t intermTableIdx;
	
	/******** Pointers NULL value **********/
	relA = NULL;
	relB = NULL;
	JoinResults = NULL;
	tmpResults = NULL;
	tmpNext = NULL;
	newTempResTable = NULL;
	
	/******** Pointers NULL value **********/
	
	if( tempResTable->tempTable[0]->counts[ rel1 ] == -1 && tempResTable->tempTable[0]->counts[ rel2 ] == -1 && tempResTable->count == 0 ){ /**1h fora pou oi sxeseis xrhsimopoiountai ston endiameso pinaka*/
		tempResTable->count = 1; 
		printf("Kamia apo tis dyo sxeseis den uparxei ston endiameso pinaka\n");
		tempResTable->tempTable[0]->counts[rel1] = 0;
		tempResTable->tempTable[0]->counts[rel2] = 0;
		uint64_t rowId, value;
		int maxRows = infoTable[ rel_idx[rel1] ].rows;
		
		if( infoTable[ rel_idx[rel2] ].rows > maxRows ) maxRows = infoTable[ rel_idx[rel2] ].rows; //poios pinakas exei tis perissoteres grammes
		
		/* metatroph twn sxesewn apo to infoTable se relation struct */
		relA = initRelation( infoTable[ rel_idx[rel1] ].rows );
		relB = initRelation( infoTable[ rel_idx[rel2] ].rows );
		
		for(i = 0; i < maxRows; i++){
			if( i < infoTable[ rel_idx[rel1] ].rows ){
				rowId = (uint64_t)i; // row id
				value = infoTable[ rel_idx[rel1] ].colsPtr[col1][i];
				intermTableIdx = 0;
				addTuple( relA, rowId, value, intermTableIdx );
			}
			
			if( i < infoTable[ rel_idx[rel2] ].rows ){
				rowId = (uint64_t)i; // row id
				value = infoTable[ rel_idx[rel2] ].colsPtr[col2][i];
				intermTableIdx = 0;
				addTuple( relB, rowId, value, intermTableIdx );
			}
		}
		
		JoinResults = RadixHashJoin( relA, relB );
		//printResultList(JoinResults);	
		tmpResults = JoinResults;
		results = 0;
		while( tmpResults != NULL ){
			results += tmpResults->count;
			tmpResults = tmpResults->next;
		}
		results = results / 2;
		printf("Results are: %d\n\n", results);
		/* desmeush xwrou ston endiameso pinaka gia tis sxeseis rel1 kai rel2 */
		tempResTable->tempTable[0]->rowIds[rel1] = malloc( results*sizeof(uint64_t) ); //desmeuetai xwros gia ta apotelesmata tou endiamesou pinaka mono thn prwth fora
		tempResTable->tempTable[0]->rowIds[rel2] = malloc( results*sizeof(uint64_t) ); //desmeuetai xwros gia ta apotelesmata tou endiamesou pinaka mono thn prwth fora
		
		if( tempResTable->tempTable[0]->rowIds[rel1] == NULL || tempResTable->tempTable[0]->rowIds[rel2] == NULL ){
			printf("Malloc failed in joinExecute\n");
			exit(-2);
		}
		
		tmpResults = JoinResults;
		c = 0;
		while( tmpResults != NULL ){ //Diatrexoume olh th lista twn apotelesmatwn
			for(j = 0; j < tmpResults->count; j+=2){ //grapsimo twn apotelesmatwn ston endiameso pinaka
				tempResTable->tempTable[0]->rowIds[rel1][c] = tmpResults->bufferA[j].key;
				tempResTable->tempTable[0]->counts[rel1]++;
				tempResTable->tempTable[0]->rowIds[rel2][c] = tmpResults->bufferA[j+1].key;
				tempResTable->tempTable[0]->counts[rel2]++;
				c++;
			}
			tmpResults = tmpResults->next;
		}
		
	}
	else if( tempResTable->tempTable[0]->counts[ rel1 ] == -1 && tempResTable->tempTable[0]->counts[ rel2 ] == -1 && tempResTable->count == 1 ){/**den uparxoun ston 1o endiameso( Dhmiourgia 2ou endiamesou)*/
		printf("Kamia ap tis 2 sxeseis ston 1o endiameso (dhmiourgia 2ou)\n");
		
		tempResTable->count++;
		tempResTable->tempTable[1]->counts[rel1] = 0;
		tempResTable->tempTable[1]->counts[rel2] = 0;
		uint64_t rowId, value;
		int maxRows = infoTable[ rel_idx[rel1] ].rows;
		
		if( infoTable[ rel_idx[rel2] ].rows > maxRows ) maxRows = infoTable[ rel_idx[rel2] ].rows; //poios pinakas exei tis perissoteres grammes
		
		/* metatroph twn sxesewn apo to infoTable se relation struct */
		relA = initRelation( infoTable[ rel_idx[rel1] ].rows );
		relB = initRelation( infoTable[ rel_idx[rel2] ].rows );
		
		for(i = 0; i < maxRows; i++){
			if( i < infoTable[ rel_idx[rel1] ].rows ){
				rowId = (uint64_t)i; // row id
				value = infoTable[ rel_idx[rel1] ].colsPtr[col1][i];
				intermTableIdx = 0;
				addTuple( relA, rowId, value, intermTableIdx );
			}
			
			if( i < infoTable[ rel_idx[rel2] ].rows ){
				rowId = (uint64_t)i; // row id
				value = infoTable[ rel_idx[rel2] ].colsPtr[col2][i];
				intermTableIdx = 0;
				addTuple( relB, rowId, value, intermTableIdx );
			}
		}
		
		JoinResults = RadixHashJoin( relA, relB );
		//printResultList(JoinResults);	
		tmpResults = JoinResults;
		results = 0;
		while( tmpResults != NULL ){
			results += tmpResults->count;
			tmpResults = tmpResults->next;
		}
		results = results / 2;
		printf("Results are: %d\n\n", results);
		/* desmeush xwrou ston endiameso pinaka gia tis sxeseis rel1 kai rel2 */
		tempResTable->tempTable[1]->rowIds[rel1] = malloc( results*sizeof(uint64_t) ); //desmeuetai xwros gia ta apotelesmata tou endiamesou pinaka mono thn prwth fora
		tempResTable->tempTable[1]->rowIds[rel2] = malloc( results*sizeof(uint64_t) ); //desmeuetai xwros gia ta apotelesmata tou endiamesou pinaka mono thn prwth fora
		
		if( tempResTable->tempTable[1]->rowIds[rel1] == NULL || tempResTable->tempTable[1]->rowIds[rel2] == NULL ){
			printf("Malloc failed in joinExecute\n");
			exit(-2);
		}
		
		tmpResults = JoinResults;
		c = 0;
		while( tmpResults != NULL ){ //Diatrexoume olh th lista twn apotelesmatwn
			for(j = 0; j < tmpResults->count; j+=2){ //grapsimo twn apotelesmatwn ston endiameso pinaka
				tempResTable->tempTable[1]->rowIds[rel1][c] = tmpResults->bufferA[j].key;
				tempResTable->tempTable[1]->counts[rel1]++;
				tempResTable->tempTable[1]->rowIds[rel2][c] = tmpResults->bufferA[j+1].key;
				tempResTable->tempTable[1]->counts[rel2]++;
				c++;
			}
			tmpResults = tmpResults->next;
		}		
	}
	else if( (tempResTable->tempTable[0]->counts[ rel1 ] != -1 && tempResTable->tempTable[0]->counts[ rel2 ] == -1 && tempResTable->tempTable[1]->counts[ rel2 ] == -1) ||
	(tempResTable->tempTable[1]->counts[ rel1 ] != -1 && tempResTable->tempTable[0]->counts[ rel2 ] == -1 && tempResTable->tempTable[1]->counts[ rel2 ] == -1) ||
	(tempResTable->tempTable[0]->counts[ rel2 ] != -1 && tempResTable->tempTable[0]->counts[ rel1 ] == -1 && tempResTable->tempTable[1]->counts[ rel1 ] == -1) || 
	(tempResTable->tempTable[1]->counts[ rel2 ] != -1 && tempResTable->tempTable[0]->counts[ rel1 ] == -1 && tempResTable->tempTable[1]->counts[ rel1 ] == -1) ){/**Mono mia apo tis 2 sxeseis uparxei se kapoion endiameso pinaka*/
		
		int tempTNum, tempRel, tempCol;
		
		if( tempResTable->tempTable[0]->counts[ rel1 ] != -1 || tempResTable->tempTable[0]->counts[ rel2 ] != -1 ){
			tempTNum = 0;
		}
		else if( tempResTable->tempTable[1]->counts[ rel1 ] != -1 || tempResTable->tempTable[1]->counts[ rel2 ] != -1 ){
			tempTNum = 1;
		}
		
		if( tempResTable->tempTable[0]->counts[ rel2 ] != -1 || tempResTable->tempTable[1]->counts[ rel2 ] != -1 ){
			tempRel = rel1;
			tempCol = col1;
			rel1 = rel2;
			col1 = col2;
			rel2 = tempRel;
			col2 = tempCol;
		}
		
		printf("H prwth sxesh uparxei ston endiameso pinaka\n");
		tempResTable->tempTable[tempTNum]->counts[rel2] = 0;
		uint64_t rowId, value;
		int maxRows = tempResTable->tempTable[tempTNum]->counts[ rel1 ];
		
		if( infoTable[ rel_idx[rel2] ].rows > maxRows ) maxRows = infoTable[ rel_idx[rel2] ].rows; //poios pinakas exei tis perissoteres grammes
		
		/* Metatroph twn sxesewn apo to infoTable se relation struct */
		relA = initRelation( tempResTable->tempTable[tempTNum]->counts[ rel1 ] );
		relB = initRelation( infoTable[ rel_idx[rel2] ].rows );
		
		/* Antigrafh timwn apo endiamesopinaka-infoTable se ena struct tupou relation */
		for(i = 0; i < maxRows; i++){
			if( i < tempResTable->tempTable[tempTNum]->counts[ rel1 ] ){
				rowId = tempResTable->tempTable[tempTNum]->rowIds[rel1][i];
				value = infoTable[ rel_idx[rel1] ].colsPtr[col1][ (int)tempResTable->tempTable[tempTNum]->rowIds[rel1][i] ]; //Antistoixizoume ta rowids tou endiamesou pinaka me ton infoTable gia na paroume ta katallhla payloads
				intermTableIdx = (uint64_t)i;
				addTuple( relA, rowId, value, intermTableIdx );
			}
			if( i < infoTable[ rel_idx[rel2] ].rows ){
				rowId = (uint64_t)i; // row id
				value = infoTable[ rel_idx[rel2] ].colsPtr[col2][i];
				intermTableIdx = 0;
				addTuple( relB, rowId, value, intermTableIdx );
			}
			
		}
		
		listSum = 0;
		JoinResults = RadixHashJoin( relA, relB );
		
		tmpResults = JoinResults;
		while( tmpResults != NULL ){ //metrame to plhthos twn apotelesmatwn tou join
			listSum += tmpResults->count;
			tmpResults = tmpResults->next;
		}
		listSum = listSum/2;
		printf("listSum is: %d\n\n", listSum);
		
		/**Dimiourgia neou endiamesou pinaka**/
		newTempResTable = temporaryResultsInit();
		for(i = 0; i < MAX_RELATIONS; i++){
			if( tempResTable->tempTable[tempTNum]->counts[i] != -1 ){
				newTempResTable->rowIds[i] = malloc( listSum*sizeof(uint64_t) );
				if( newTempResTable->rowIds[i] == NULL ){
					printf("Malloc failed in joinExecute (new result table)\n");
					exit(-1);
				}
				newTempResTable->counts[i] = 0;
			}
		}
		
		/*********Enhmerwsh tou endiamesou pinaka*********/
		tmpResults = JoinResults;
		write = 0;
		while(tmpResults != NULL){
			for(i = 0; i < tmpResults->count; i+=2){
				newTempResTable->rowIds[rel1][write] = tmpResults->bufferA[i].key;
				newTempResTable->counts[rel1]++;
				newTempResTable->rowIds[rel2][write] = tmpResults->bufferA[i+1].key;
				newTempResTable->counts[rel2]++;
				for(j = 0; j < MAX_RELATIONS; j++){
					if( tempResTable->tempTable[tempTNum]->counts[j] > 0 && j != rel1 && j != rel2 ){
						newTempResTable->rowIds[j][write] = tempResTable->tempTable[tempTNum]->rowIds[j][ tmpResults->bufferA[i].intermIdx ];
						newTempResTable->counts[j]++;
					}
				}
				write++;
			}
			tmpResults = tmpResults->next;
		}
		
		/**Diagrafh tou paliou endiamesou pinaka **/
		temporaryResultsDelete( tempResTable->tempTable[tempTNum] );		
		tempResTable->tempTable[tempTNum] = newTempResTable;
	}
	else if( tempResTable->tempTable[0]->counts[ rel1 ] != -1 && tempResTable->tempTable[0]->counts[ rel2 ] != -1 ){/**Kai oi duo sxeseis tou join uparxoun ston prwto endiameso pinaka*/
		printf("Kai oi 2 sxeseis yparxoun ston endiameso pinaka\n");
		uint64_t rowId, value;
		int maxRows = tempResTable->tempTable[0]->counts[ rel2 ];
		
		if( tempResTable->tempTable[0]->counts[rel1] > maxRows ) maxRows = tempResTable->tempTable[0]->counts[rel1]; //poios pinakas exei tis perissoteres grammes
		
		/* Metatroph twn sxesewn apo to infoTable se relation struct */
		relA = initRelation( tempResTable->tempTable[0]->counts[ rel1 ] );
		relB = initRelation( tempResTable->tempTable[0]->counts[ rel2 ] );
		
		/* Antigrafh timwn apo endiameso pinaka-infoTable se ena struct tupou relation */
		for(i = 0; i < tempResTable->tempTable[0]->counts[ rel1 ]; i++){
			intermTableIdx = (uint64_t)i;
			rowId = tempResTable->tempTable[0]->rowIds[rel1][i]; // row id tou relation A
			value = infoTable[ rel_idx[rel1] ].colsPtr[col1][ (int)tempResTable->tempTable[0]->rowIds[rel1][i] ]; //Antistoixizoume ta rowids tou endiamesou pinaka me ton infoTable gia na paroume ta katallhla payloads
			addTuple( relA, rowId, value, intermTableIdx );
			
			rowId = tempResTable->tempTable[0]->rowIds[rel2][i]; // row id tou relation B
			value = infoTable[ rel_idx[rel2] ].colsPtr[col2][ (int)tempResTable->tempTable[0]->rowIds[rel2][i] ]; //Antistoixizoume ta rowids tou endiamesou pinaka me ton infoTable gia na paroume ta katallhla payloads
			addTuple( relB, rowId, value, intermTableIdx );
		}
	
		listSum = 0;
		
		JoinResults = RadixHashJoin( relA, relB );
		
		tmpResults = JoinResults;
		while( tmpResults != NULL ){ //metrame to plhthos twn apotelesmatwn tou join
			listSum += tmpResults->count;
			tmpResults = tmpResults->next;
		}
		listSum = listSum/2;
		printf("listSum is: %d\n\n", listSum);

		/**Dimiourgia neou endiamesou pinaka**/
		newTempResTable = temporaryResultsInit();
		for(i = 0; i < MAX_RELATIONS; i++){
			if( tempResTable->tempTable[0]->counts[i] != -1 ){
				newTempResTable->rowIds[i] = malloc( listSum*sizeof(uint64_t) );
				if( newTempResTable->rowIds[i] == NULL ){
					printf("Malloc failed in joinExecute (new result table)\n");
					exit(-1);
				}
				newTempResTable->counts[i] = 0;
			}
		}
		
		/*********Enhmerwsh tou endiamesou pinaka*********/
		tmpResults = JoinResults;
		write = 0;
		while(tmpResults != NULL){
			for(i = 0; i < tmpResults->count; i+=2){
				newTempResTable->rowIds[rel1][write] = tmpResults->bufferA[i].key;
				newTempResTable->counts[rel1]++;
				newTempResTable->rowIds[rel2][write] = tmpResults->bufferA[i+1].key;
				newTempResTable->counts[rel2]++;
				for(j = 0; j < MAX_RELATIONS; j++){
					if( tempResTable->tempTable[0]->counts[j] > 0 && j != rel1 && j != rel2 ){
						newTempResTable->rowIds[j][write] = tempResTable->tempTable[0]->rowIds[j][ tmpResults->bufferA[i].intermIdx ];
						newTempResTable->counts[j]++;
					}
				}
				write++;
			}
			tmpResults = tmpResults->next;
		}
		
		/**Diagrafh tou paliou endiamesou pinaka **/
		temporaryResultsDelete( tempResTable->tempTable[0] );		
		tempResTable->tempTable[0] = newTempResTable;
	}
	else if( (tempResTable->tempTable[0]->counts[ rel1 ] != -1 && tempResTable->tempTable[1]->counts[ rel2 ] != -1) || (tempResTable->tempTable[0]->counts[ rel2 ] != -1 && tempResTable->tempTable[1]->counts[ rel1 ] != -1) ){
		///H mia sxesh vrisketai ston enan apo tous 2 endiamesous pinakes kai h allh ston allon
		printf("H mia sxesh vrisketai ston enan apo tous 2 endiamesous pinakes kai h allh ston allon\n");
		uint64_t rowId, value;
		int tableA, tableB;
		
		if( tempResTable->tempTable[0]->counts[ rel1 ] != -1 && tempResTable->tempTable[1]->counts[ rel2 ] != -1 ){
			tableA = 0;
			tableB = 1;
		}
		else if( tempResTable->tempTable[0]->counts[ rel2 ] != -1 && tempResTable->tempTable[1]->counts[ rel1 ] != -1 ){
			tableA = 1;
			tableB = 0;
		}
		
		int maxRows = tempResTable->tempTable[tableA]->counts[ rel1 ];
		
		if( tempResTable->tempTable[tableB]->counts[rel2] > maxRows ) maxRows = tempResTable->tempTable[tableB]->counts[rel2]; //poios pinakas exei tis perissoteres grammes
		
		if( maxRows == tempResTable->tempTable[tableA]->counts[ rel1 ] ) printf("maxRows apo Rel1 kai maxRows = %d\n", maxRows);
		if( maxRows == tempResTable->tempTable[tableB]->counts[ rel2 ] ) printf("maxRows apo Rel2 kai maxRows = %d\n", maxRows);
		
		/* Metatroph twn sxesewn apo to infoTable se relation struct */
		relA = initRelation( tempResTable->tempTable[tableA]->counts[ rel1 ] );
		relB = initRelation( tempResTable->tempTable[tableB]->counts[ rel2 ] );
		
		printf("tempResTable->tempTable[tableA]->counts[rel1] = %d\n", tempResTable->tempTable[tableA]->counts[rel1]);
		printf("tempResTable->tempTable[tableB]->counts[rel2] = %d\n", tempResTable->tempTable[tableB]->counts[rel2]);
		
		/* Antigrafh timwn apo endiameso pinaka-infoTable se ena struct tupou relation */
		for(i = 0; i < maxRows; i++){
			if( i < tempResTable->tempTable[tableA]->counts[rel1] ){
				intermTableIdx = (uint64_t)i;
				rowId = tempResTable->tempTable[tableA]->rowIds[rel1][i]; // row id tou relation A
				value = infoTable[ rel_idx[rel1] ].colsPtr[col1][ (int)tempResTable->tempTable[tableA]->rowIds[rel1][i] ]; //Antistoixizoume ta rowids tou endiamesou pinaka me ton infoTable gia na paroume ta katallhla payloads
				addTuple( relA, rowId, value, intermTableIdx );
			}	
			if( i < tempResTable->tempTable[tableB]->counts[rel2] ){
				intermTableIdx = (uint64_t)i;
				rowId = tempResTable->tempTable[tableB]->rowIds[rel2][i]; // row id tou relation B
				value = infoTable[ rel_idx[rel2] ].colsPtr[col2][ (int)tempResTable->tempTable[tableB]->rowIds[rel2][i] ]; //Antistoixizoume ta rowids tou endiamesou pinaka me ton infoTable gia na paroume ta katallhla payloads
				addTuple( relB, rowId, value, intermTableIdx );
			}
		}
	
		listSum = 0;
		
		JoinResults = RadixHashJoin( relA, relB );
		
		tmpResults = JoinResults;
		while( tmpResults != NULL ){ //metrame to plhthos twn apotelesmatwn tou join
			listSum += tmpResults->count;
			tmpResults = tmpResults->next;
		}
		listSum = listSum/2;
		printf("listSum is: %d\n\n", listSum);

		/**Dimiourgia neou endiamesou pinaka**/
		newTempResTable = temporaryResultsInit();
		for(i = 0; i < MAX_RELATIONS; i++){
			if( tempResTable->tempTable[0]->counts[i] != -1 || tempResTable->tempTable[1]->counts[i] != -1 ){
				newTempResTable->rowIds[i] = malloc( listSum*sizeof(uint64_t) );
				if( newTempResTable->rowIds[i] == NULL ){
					printf("Malloc failed in joinExecute (new result table)\n");
					exit(-1);
				}
				newTempResTable->counts[i] = 0;
			}
		}
		
		/*********Enhmerwsh tou endiamesou pinaka*********/
		tmpResults = JoinResults;
		write = 0;
		while(tmpResults != NULL){
			for(i = 0; i < tmpResults->count; i+=2){
				newTempResTable->rowIds[rel1][write] = tmpResults->bufferA[i].key;
				newTempResTable->counts[rel1]++;
				newTempResTable->rowIds[rel2][write] = tmpResults->bufferA[i+1].key;
				newTempResTable->counts[rel2]++;
				
				for(j = 0; j < MAX_RELATIONS; j++){
					if( tempResTable->tempTable[tableA]->counts[j] > 0 && j != rel1 ){
						newTempResTable->rowIds[j][write] = tempResTable->tempTable[tableA]->rowIds[j][ tmpResults->bufferA[i].intermIdx ];
						newTempResTable->counts[j]++;
					}
					else if( tempResTable->tempTable[tableB]->counts[j] > 0 && j != rel2 ){
						newTempResTable->rowIds[j][write] = tempResTable->tempTable[tableB]->rowIds[j][ tmpResults->bufferA[i+1].intermIdx ];
						newTempResTable->counts[j]++;
					}
				}
				write++;
			}
			tmpResults = tmpResults->next;
		}
		
		/**Diagrafh tou paliou endiamesou pinaka **/
		temporaryResultsDelete( tempResTable->tempTable[0] );
		temporaryResultsDelete( tempResTable->tempTable[1] );
		
		tempResTable->count--;
		
		tempResTable->tempTable[0] = newTempResTable;
	}
	
	/// Apodesmeush mnhmhs gia th lista twn apotelesmatwn tou join, tou buffer pou periexei o kathe komvos ths kai tou kathe tuple pou periexei o buffer
	tmpResults = JoinResults;
	int I = 1;
	while( tmpResults != NULL ){
		I++;
		tmpNext = tmpResults->next;
		free(tmpResults->bufferA);
		free(tmpResults);
		tmpResults = tmpNext;
	}

	deleteRelation(relA);
	deleteRelation(relB);
	
	return tempResTable;
}

tempResultsTable* joinSameRel(infoTableCell* infoTable, tempResultsTable* tempResTable, int* rel_idx, int rel, int col1, int col2){
	int rows, i, write, j;
	
	
	rows = infoTable[ rel_idx[rel] ].rows;
	if( tempResTable->tempTable[0]->counts[ rel ] == -1 && tempResTable->count == 0){ //1h fora pou h sxesh xrhsimopoieitai ston 1o endiameso pinaka + yparxei mono enas endiamesos pinakas
		tempResTable->count = 1;
		
		tempResTable->tempTable[0]->counts[ rel ] = 0;
		
		tempResTable->tempTable[0]->rowIds[rel] = malloc( rows*sizeof(uint64_t) ); //desmeuetai xwros gia ta apotelesmata tou endiamesou pinaka mono thn prwth fora
		
		if( tempResTable->tempTable[0]->rowIds[rel] == NULL ){
			printf("Malloc failed in joinSameRel\n");
			exit(-2);
		}
		
		if( col1 = col2 ){ //idies sxeseis kai idies kolones alla h sxesh pou dinetai den yparxei ston endiameso pinaka
			for( i = 0; i < rows; i++){
				tempResTable->tempTable[0]->rowIds[rel][i] = (uint64_t) i;
				tempResTable->tempTable[0]->counts[rel]++;
			}
			return tempResTable;
		}
		
		write = 0;
		for( i = 0; i < rows; i++){
			if( infoTable[ rel_idx[rel] ].colsPtr[col1][i] == infoTable[ rel_idx[rel] ].colsPtr[col2][i] ){
				tempResTable->tempTable[0]->rowIds[rel][write] = (uint64_t) i;
				tempResTable->tempTable[0]->counts[rel]++;
				write++;
			}
		}
		printf("Rows remaining are: %d\n", write);
	}
	else if( tempResTable->tempTable[0]->counts[ rel ] == -1 && tempResTable->count == 1 ){ //h sxesh den periexetai ston prwto endiameso pinaka + o prwtos endiamesos periexei alles sxeseis
		tempResTable->count++;
		
		tempResTable->tempTable[1]->counts[rel] = 0;
		
		tempResTable->tempTable[1]->rowIds[rel] = malloc( rows*sizeof(uint64_t) ); //desmeuetai xwros gia ta apotelesmata tou endiamesou pinaka mono thn prwth fora
		
		if( tempResTable->tempTable[1]->rowIds[rel] == NULL ){
			printf("Malloc failed in joinSameRel\n");
			exit(-2);
		}
		
		if( col1 = col2 ){ //idies sxeseis kai idies kolones alla h sxesh pou dinetai den yparxei ston endiameso pinaka
			for( i = 0; i < rows; i++){
				tempResTable->tempTable[1]->rowIds[rel][i] = (uint64_t) i;
				tempResTable->tempTable[1]->counts[rel]++;
			}
			return tempResTable;
		}
		
		write = 0;
		for( i = 0; i < rows; i++){
			if( infoTable[ rel_idx[rel] ].colsPtr[col1][i] == infoTable[ rel_idx[rel] ].colsPtr[col2][i] ){
				tempResTable->tempTable[1]->rowIds[rel][write] = (uint64_t) i;
				tempResTable->tempTable[1]->counts[rel]++;
				write++;
			}
		}
		printf("Rows remaining are: %d\n", write);
	}
	else if( tempResTable->tempTable[0]->counts[ rel ] != -1 || tempResTable->tempTable[1]->counts[ rel ] != -1 ){ //h sxesh rel yparxei se kapoion endiameso pinaka kai epishs kai alles sxeseis
		int tableNum;
		
		if( tempResTable->tempTable[0]->counts[ rel ] != -1 ){
			tableNum = 0;
		}
		else{
			tableNum = 1;
		}
		
		if( col1 = col2 ) return tempResTable; //idies sxeseis kai idies kolones
		write = 0;
		for( i = 0; i < tempResTable->tempTable[tableNum]->counts[rel]; i++ ){
			if( infoTable[ rel_idx[rel] ].colsPtr[col1][ tempResTable->tempTable[tableNum]->rowIds[rel][i] ] == infoTable[ rel_idx[rel] ].colsPtr[col2][ tempResTable->tempTable[tableNum]->rowIds[rel][i] ] ){
				tempResTable->tempTable[tableNum]->rowIds[rel][write] = tempResTable->tempTable[tableNum]->rowIds[rel][i];
				for(j = 0; j < MAX_RELATIONS; j++){
					if( tempResTable->tempTable[tableNum]->counts[j] > 0 && j != rel ){
						tempResTable->tempTable[tableNum]->rowIds[j][write] = tempResTable->tempTable[tableNum]->rowIds[j][i];
					}
				}
				write++;
			}
		}
		for(j = 0; j < MAX_RELATIONS; j++){
			if( tempResTable->tempTable[tableNum]->counts[j] > 0 ){
				tempResTable->tempTable[tableNum]->counts[j] = write;
			}
		}
		printf("Rows remaining are: %d\n", write);
	}
	return tempResTable;
}

tempResultsTable* joinPrevRels(infoTableCell* infoTable, tempResultsTable* tempResTable, int *rel_idx, int rel1, int rel2, int col1, int col2){
	int i, j, write, tableNum;
	
	if( tempResTable->tempTable[0]->counts[rel1] != -1 ){
		tableNum = 0;
	}
	else if( tempResTable->tempTable[1]->counts[rel1] != -1 ){
		tableNum = 1;
	}
	
	write = 0;
	for( i = 0; i < tempResTable->tempTable[tableNum]->counts[rel1]; i++ ){
		if( infoTable[ rel_idx[rel1] ].colsPtr[col1][ tempResTable->tempTable[tableNum]->rowIds[rel1][i] ] == infoTable[ rel_idx[rel2] ].colsPtr[col2][ tempResTable->tempTable[tableNum]->rowIds[rel2][i] ] ){
			tempResTable->tempTable[tableNum]->rowIds[rel1][write] = tempResTable->tempTable[tableNum]->rowIds[rel1][i];
			tempResTable->tempTable[tableNum]->rowIds[rel2][write] = tempResTable->tempTable[tableNum]->rowIds[rel2][i];
			for(j = 0; j < MAX_RELATIONS; j++){
				if( tempResTable->tempTable[tableNum]->counts[j] > 0 && j != rel1 && j != rel2 ){
					tempResTable->tempTable[tableNum]->rowIds[j][write] = tempResTable->tempTable[tableNum]->rowIds[j][i];
				}
			}
			write++;
		}
	}
	for(j = 0; j < MAX_RELATIONS; j++){
		if( tempResTable->tempTable[tableNum]->counts[j] > 0 ){
			tempResTable->tempTable[tableNum]->counts[j] = write;
		}
	}
	printf("Rows remaining between relations are: %d\n", write);
	return tempResTable;
}

void filterExecute(infoTableCell* infoTable, int infoTidx, int infoTcol, char filterType, uint64_t filterNum, tempResultsTable* tempResTable, int rel){
	int rows, i, foundFlg = 0, idx = 0, j;
	int read = 0, write = 0;
	
	rows = infoTable[ infoTidx ].rows;
	if( tempResTable->tempTable[0]->counts[ rel ] == -1 ){  //1h fora pou h sxesh xrhsimopoieitai ston endiameso pinaka
		tempResTable->tempTable[0]->counts[ rel ] = 0;
		
		tempResTable->tempTable[0]->rowIds[rel] = malloc( rows*sizeof(uint64_t) ); //desmeuetai xwros gia ta apotelesmata tou endiamesou pinaka mono thn prwth fora 
		
		if( tempResTable->tempTable[0]->rowIds[rel] == NULL ){
			printf("Malloc failed in filterExecute\n");
			exit(-2);
		}
		
		if( filterType == '=' ){
			for( i = 0; i < rows; i++){
				if( infoTable[ infoTidx ].colsPtr[ infoTcol ][i] == filterNum ){
					foundFlg = 1;
					/*eisagwgh ston endiameso pinaka*/
					tempResTable->tempTable[0]->rowIds[ rel ][ idx ] = (uint64_t) i;
					tempResTable->tempTable[0]->counts[ rel ]++;
					idx++;				
				}
			}
		}
		else if( filterType == '<' ){
			for( i = 0; i < rows; i++){
				if( infoTable[ infoTidx ].colsPtr[ infoTcol ][i] < filterNum){
					foundFlg = 1;
					/*eisagwgh ston endiameso pinaka*/
					tempResTable->tempTable[0]->rowIds[ rel ][ idx ] = (uint64_t) i;
					tempResTable->tempTable[0]->counts[ rel ]++;
					idx++;
				}
			}
		}
		else if( filterType == '>' ){
			for( i = 0; i < rows; i++){
				if( infoTable[ infoTidx ].colsPtr[ infoTcol ][i] > filterNum){
					foundFlg = 1;
					/*eisagwgh ston endiameso pinaka*/
					tempResTable->tempTable[0]->rowIds[ rel ][ idx ] = (uint64_t) i;
					tempResTable->tempTable[0]->counts[ rel ]++;
					idx++;
				}
			}
		}
		else{
			printf("Wrong filter type given\n");
			exit(-1);
		}
		printf("Rows remaining after filter are: %d\n", idx);
	}
	else{  //h sxesh exei hdh xrhsimpoihthei ston endiameso pinaka
		if( filterType == '=' ){
			for( read = 0; read < tempResTable->tempTable[0]->counts[ rel ]; read++){
				if( infoTable[infoTidx].colsPtr[infoTcol][ tempResTable->tempTable[0]->rowIds[rel][read] ] == filterNum ){
					foundFlg = 1;
					/*eisagwgh ston endiameso pinaka*/
					tempResTable->tempTable[0]->rowIds[rel][write] = tempResTable->tempTable[0]->rowIds[rel][read];
					for(j = 0; j < MAX_RELATIONS; j++){
						if( tempResTable->tempTable[0]->counts[j] > 0 && j != rel ){
							tempResTable->tempTable[0]->rowIds[j][write] = tempResTable->tempTable[0]->rowIds[j][read];
						}
					}
					write++;
				}
			}
			for(j = 0; j < MAX_RELATIONS; j++){
				if( tempResTable->tempTable[0]->counts[j] > 0 ){
					tempResTable->tempTable[0]->counts[j] = write;
				}
			}
		}
		else if( filterType == '<' ){
			for( read = 0; read < tempResTable->tempTable[0]->counts[ rel ]; read++){
				if( infoTable[infoTidx].colsPtr[infoTcol][ tempResTable->tempTable[0]->rowIds[rel][read] ] < filterNum ){
					foundFlg = 1;
					/*eisagwgh ston endiameso pinaka*/
					tempResTable->tempTable[0]->rowIds[rel][write] = tempResTable->tempTable[0]->rowIds[rel][read];
					for(j = 0; j < MAX_RELATIONS; j++){
						if( tempResTable->tempTable[0]->counts[j] > 0 && j != rel ){
							tempResTable->tempTable[0]->rowIds[j][write] = tempResTable->tempTable[0]->rowIds[j][read];
						}
					}
					write++;
				}
			}
			for(j = 0; j < MAX_RELATIONS; j++){
				if( tempResTable->tempTable[0]->counts[j] > 0 ){
					tempResTable->tempTable[0]->counts[j] = write;
				}
			}
		}
		else if( filterType == '>' ){
			for( read = 0; read < tempResTable->tempTable[0]->counts[ rel ]; read++){
				if( infoTable[infoTidx].colsPtr[infoTcol][ tempResTable->tempTable[0]->rowIds[rel][read] ] > filterNum ){
					foundFlg = 1;
					/*eisagwgh ston endiameso pinaka*/
					tempResTable->tempTable[0]->rowIds[rel][write] = tempResTable->tempTable[0]->rowIds[rel][read];
					for(j = 0; j < MAX_RELATIONS; j++){
						if( tempResTable->tempTable[0]->counts[j] > 0 && j != rel ){
							tempResTable->tempTable[0]->rowIds[j][write] = tempResTable->tempTable[0]->rowIds[j][read];
						}
					}
					write++;
				}
			}
			for(j = 0; j < MAX_RELATIONS; j++){
				if( tempResTable->tempTable[0]->counts[j] > 0 ){
					tempResTable->tempTable[0]->counts[j] = write;
				}
			}
		}
		else{
			printf("Wrong filter type given\n");
			exit(-1);
		}
		printf("Rows remaining after filter are: %d\n", write);
	}
	printf("\n");
	if( foundFlg == 0 ){
		printf(">>> NOTHING FOUND!\n");
	}
	
	tempResTable->count = 1; //uparxei enas endiamesos pinakas
	
}

char** optPredOrder( infoTableCell* infoTable, char** preds, int predCount, int* rel_idx ){
	
	int i, j, rel1, rel2, col1, col2, filterNum;
	char *tempPred, *token, dot[2] = ".";
	uint32_t distValues[predCount];
	
	
	
	for( i = 0; i < predCount; i++){
		tempPred = malloc( (strlen(preds[i])+1)*sizeof(char) );
		if( tempPred == NULL ){
			printf("Malloc failed in optPredOrder\n");
			exit(-1);
		}
		strcpy(tempPred, preds[i]);
		
		
		char *ret;
		ret = strstr( preds[i], dot );
		if( ret == NULL ){
			printf("No relations given in predicate\n");
		}
		ret = strstr( ret+1, dot );
			
		if( ret == NULL ){ // kathgorhma filtrou
			
			token = strtok(tempPred, ".");
			rel1 = atoi(token);
			if( rel1 == 0 && strcmp(token, "0") != 0){
				printf("Wrong input for relation 1 (optPreds)\n");
				exit(-1);
			}
			token = strtok(NULL, "<=>");
			col1 = atoi(token);
			if( col1 == 0 && strcmp(token, "0") != 0){
				printf("Wrong input for column 1 (optPreds)\n");
				exit(-1);
			}
			
			token = strtok(NULL, ".");
			filterNum = atoi(token);
			if( filterNum == 0 && strcmp(token, "0") != 0){
				printf("Wrong input for filter (optPreds)\n");
				exit(-1);
			}
			
			//distValues[i] = infoTable[ rel_idx[rel1] ].distinctVal[col1]; //H prohgoumenh version pou ypologize tis distinct times mias kolonas mias sxeshs
			distValues[i] = 100; //mia tyxaia minimum timh prokeimenou na ektelestei to filtro panta prwto
			
		}
		else{ //kathgorhma suzeu3hs
			token = strtok(tempPred, ".");
			rel1 = atoi(token);
			if( rel1 == 0 && strcmp(token, "0") != 0){
				printf("Wrong input for relation 1 (optPreds)\n");
				exit(-1);
			}
			token = strtok(NULL, "=");
			col1 = atoi(token);
			if( col1 == 0 && strcmp(token, "0") != 0){
				printf("Wrong input for column 1 (optPreds)\n");
				exit(-1);
			}
			
			token = strtok(NULL, ".");
			rel2 = atoi(token);
			if( rel2 == 0 && strcmp(token, "0") != 0){
				printf("Wrong input for column 1 (optPreds)\n");
				exit(-1);
			}
			
			token = strtok(NULL, "=");
			col2 = atoi(token);
			if( col2 == 0 && strcmp(token, "0") != 0){
				printf("Wrong input for column 1 (optPreds)\n");
				exit(-1);
			}
			
			distValues[i] = infoTable[ rel_idx[rel1] ].distinctVal[col1] + infoTable[ rel_idx[rel2] ].distinctVal[col2];
			
		}
		free(tempPred);	
	}
	
	
	uint32_t swapDist;
	char swapPred[10], **newPreds;
	
	newPreds = malloc( predCount*sizeof(char*) );
	if( newPreds == NULL ){
		printf("Malloc failed in optPred\n");
		exit(-1);
	}
	for( i = 0; i < predCount; i++){
		newPreds[i] = malloc( 15*sizeof(char) );
		memset(newPreds[i], '\0', 15);
		strcpy(newPreds[i], preds[i]);
	}
	
	for( i = 0; i < predCount; i++){
		printf("distValues[%d] is: %d - newPreds[%d] is: %s\n", i, distValues[i], i, newPreds[i]);
	}
	
	for( i = 0; i < predCount-1; i++){
		for( j = 0; j < predCount-i-1; j++){
			if( distValues[j] > distValues[j+1]){
				swapDist = distValues[j];
				distValues[j] = distValues[j+1];
				distValues[j+1] = swapDist;
				
				strcpy(swapPred, newPreds[j]);
				strcpy(newPreds[j], newPreds[j+1]);
				strcpy(newPreds[j+1], swapPred);
				
			}
		}
	}
	
	for( i = 0; i < predCount; i++){
		printf("distValues[%d] is: %d - newPreds[%d] is: %s\n", i, distValues[i], i, newPreds[i]);
	}
	printf("\n");
	//delete to preds
	return newPreds;
	
}

int* relsParse(char* relations, int infoTSize, int *spaceCount){
	int *rels_idx, i = 1, tmpComp, space_Count = 0, k;
	char *token, *tmpRel;
	
	tmpRel = malloc( (strlen(relations)+1)*sizeof(char) );
	strcpy(tmpRel, relations);
	
	for(k = 0; k < strlen(tmpRel); k++){  //ta relations tha isountai me ta kena pou yparxoun meta3y tous + 1
		if(tmpRel[k] == ' '){
			space_Count++;
		}
	}
	rels_idx = malloc((space_Count+1)*sizeof(int)); //akrivhs arithmos thesewn pou desmeuontai gia ola ta relations
 
	token = strtok(tmpRel, " ");
	tmpComp = atoi(token);
	
	if(tmpComp == 0 && strcmp(token, "0") != 0){
		printf("Wrong input for the relations\n");
		exit(-5);
	}
	if(tmpComp >= infoTSize){
		printf("Invalid number for the relation at relsParse 1\n");
		exit(-6);
	}
	rels_idx[0] = tmpComp;
	while(token != NULL){
		token = strtok(NULL, " ");
		if(token == NULL) continue;
		tmpComp = atoi(token);
		
		if(tmpComp == 0 && strcmp(token, "0") != 0){
			printf("Wrong input for the relations\n");
			exit(-5);
		}
		if(tmpComp >= infoTSize){
			printf("Invalid number for the relation at relsParse 2\n");
			exit(-6);
		}
		rels_idx[i] = tmpComp;
		i++;
	}
	*spaceCount = space_Count;
	return rels_idx;
}

int projParse( char* projections, int* proj_idx_rel, int* proj_idx_col, infoTableCell* infoTable, int relsCount, int* rels_idx ){
	
	int j = 1, tmpComp2, k;
	char *tmpProj, *token;
	
	int spaceCount = 0;
	
	tmpProj = malloc( (strlen(projections)+1)*sizeof(char) );
	strcpy(tmpProj, projections);
	
	token = strtok(tmpProj, ".");
	tmpComp2 = atoi(token);

	if(tmpComp2 == 0 && strcmp(token, "0") != 0){
		printf("Wrong input for the relations\n");
		exit(-7);
	}
	if(tmpComp2 > relsCount){
		printf("Invalid number for the relation at projParse 1\n");
		exit(-8);
	}
	
	proj_idx_rel[0] = tmpComp2;
	
	token = strtok(NULL, " ");
	tmpComp2 = atoi(token);

	if(tmpComp2 == 0 && strcmp(token, "0") != 0){
		printf("Wrong input for the columns\n");
		exit(-9);
	}
	if(tmpComp2 >= infoTable[ rels_idx[ proj_idx_rel[0] ] ].columns){
		printf("Invalid number for the columns at projParse 1\n");
		exit(-10);
	}
	
	proj_idx_col[0] = tmpComp2;
	
	while(token != NULL){
		token = strtok(NULL, ".");
		if(token == NULL) continue;
		tmpComp2 = atoi(token);

		if(tmpComp2 == 0 && strcmp(token, "0") != 0){
			printf("Wrong input for the relations\n");
			exit(-7);
		}
		if(tmpComp2 > relsCount){
			printf("Invalid number for the relation at projParse 2\n");
			exit(-8);
		}
		
		proj_idx_rel[j] = tmpComp2;
		
		token = strtok(NULL, " ");
		tmpComp2 = atoi(token);

		if(tmpComp2 == 0 && strcmp(token, "0") != 0){
			printf("Wrong input for the columns\n");
			exit(-9);
		}
		if(tmpComp2 >= infoTable[ rels_idx[ proj_idx_rel[j] ] ].columns){
			printf("Invalid number for the columns at projParse 2\n");
			exit(-10);
		}
		
		proj_idx_col[j] = tmpComp2;
		j++;
	}
	
	return spaceCount+1;
}

char** predParse(char* predicates, int *predCount ){
	char *tmpPred, **preds, *token;
	int pred_Count = 0; //metraei posa '&' uparxoun sta predicates
	int c = 1, k;
	char dot[2] = ".";
	 
	tmpPred = malloc( (strlen(predicates)+1)*sizeof(char) ); //antigrafh tou string me ta predicates se mia temp metavlith
	if( tmpPred == NULL ){
		printf("Malloc failed in executeQuery\n");
		exit(-1);
	}
	strcpy( tmpPred, predicates );
	
	for(k = 0; k < strlen(tmpPred); k++){  //ta predicates tha isountai me ta '&' pou yparxoun meta3y tous + 1
		if(tmpPred[k] == '&'){
			pred_Count++;
		}
	}
	pred_Count++;
	
	preds = malloc( pred_Count*sizeof( char *) );
	
	token = strtok( tmpPred, "&" );
	if( token == NULL ){
		printf("Wrong input for predicates\n");
		exit(-1);
	}
	preds[0] = malloc( (strlen(token) + 1)*sizeof(char) );
	strcpy(preds[0], token);
	while( token != NULL ){
		token = strtok( NULL, "&" );
		if( token == NULL ) continue;		
		preds[c] = malloc( (strlen(token) + 1)*sizeof(char) );
		strcpy(preds[c], token);
		c++;
	}
	
	*predCount = pred_Count;
	return preds;
}

tempResultsTable* tempResultsTableInit(){
	tempResultsTable* newTable;
	newTable = malloc( sizeof(tempResultsTable) );
	if( newTable == NULL ){
		printf("Malloc failed in tempResultsTableInit\n");
		exit(-1);
	}
	
	
	newTable->tempTable = malloc( 2*sizeof(temporaryResults *) );
	if( newTable == NULL ){
		printf("Malloc failed in tempResultsTableInit\n");
		exit(-1);
	}
	newTable->tempTable[0] = NULL;
	newTable->tempTable[1] = NULL;
	newTable->count = 0;
	return newTable;
}

temporaryResults* temporaryResultsInit(){
	temporaryResults* tmpRes;
	tmpRes = malloc( sizeof(temporaryResults) );
	if( tmpRes == NULL ){
		printf("Malloc failed in temporaryResultsInit\n");
		exit(-1);
	}
	
	tmpRes->rowIds = malloc( MAX_RELATIONS*sizeof(uint64_t*) );
	if( tmpRes->rowIds == NULL ){
		printf("Malloc failed in temporaryResultsInit\n");
		exit(-1);
	}
	
	
	tmpRes->counts = malloc( MAX_RELATIONS*sizeof(int) );
	if( tmpRes->counts == NULL ){
		printf("Malloc failed in temporaryResultsInit\n");
		exit(-1);
	}
	
	for(int i = 0; i < MAX_RELATIONS; i++){
		tmpRes->counts[i] = -1;
	}
	tmpRes->tableSize = MAX_RELATIONS;
	return tmpRes;
}

void temporaryResultsDelete( temporaryResults* tempResTable ){
	for( int i = 0; i < tempResTable->tableSize; i++){
		if( tempResTable->counts[i] != -1 && tempResTable->counts[i] != 0 ){
			tempResTable->counts[i] = -1;
			free(tempResTable->rowIds[i]);
		}
	}
	free(tempResTable->counts);
}

