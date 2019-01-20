#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<stdint.h>
#include "queryTranslation.h"

queryListNode* queryListInit(){
	queryListNode* newListNode;
	
	newListNode = malloc( sizeof(queryListNode) );
	if( newListNode == NULL ){
		printf("Malloc failed in queryListInit\n");
		exit(-4);
	}
	newListNode->next = NULL;
	newListNode->relations = NULL;
	return newListNode;
}


void addQuery( queryListNode* start, char* line ){
	queryListNode *tmpNode, *newNode;
	char* token;
	tmpNode = start;
	
	/*token = strtok(line, "|");
	while( token != NULL ){
		printf("token is: %s\n", token );
		token = strtok( NULL, "\n|"); 
	}*/
	//getchar();
	
	if( start->relations == NULL ){ //eisagwgh ston 1o komvo
		
		token = strtok(line, "|");
		start->relations = malloc( (strlen(token)+1)*sizeof(char) );
		strcpy( start->relations, token ); 
		
		token = strtok(NULL, "|");
		start->predicates = malloc( (strlen(token)+1)*sizeof(char) );
		strcpy( start->predicates, token ); 
		
		token = strtok(NULL, "\n");
		start->projections = malloc( (strlen(token)+1)*sizeof(char) );
		strcpy( start->projections, token ); 
		
		return;
	}
	
	while( tmpNode->next != NULL ){
		tmpNode = tmpNode->next;
	}
	
	newNode = queryListInit();
	
	token = strtok(line, "|");
	newNode->relations = malloc( (strlen(token)+1)*sizeof(char) );
	strcpy( newNode->relations, token ); 
		
	token = strtok(NULL, "|");
	newNode->predicates = malloc( (strlen(token)+1)*sizeof(char) );
	strcpy( newNode->predicates, token ); 
		
	token = strtok(NULL, "\n");
	newNode->projections = malloc( (strlen(token)+1)*sizeof(char) );
	strcpy( newNode->projections, token );
	
	tmpNode->next = newNode;
	return;
	
}

void queryListDelete( queryListNode* startNode ){
	queryListNode* tmp;
	
	if( startNode->next == NULL ){
		free( startNode->relations );
		free( startNode->predicates );
		free( startNode->projections );
		return;
	}
	
	
	tmp = startNode;
	//startNode = startNode->next;
	while( tmp != NULL ){
		
		free( tmp->relations );
		free( tmp->predicates );
		free( tmp->projections );
		
		startNode = startNode->next;
		tmp = startNode;
		//startNode = startNode->next;
		
	}
}

//////////
void printList( queryListNode* startNode ){
	queryListNode* tmp;
	tmp = startNode;
	
	while( tmp != NULL ){
		printf("relation: %s , predicate: %s , projection: %s\n", tmp->relations, tmp->predicates, tmp->projections );
		tmp = tmp->next;
	}
}
