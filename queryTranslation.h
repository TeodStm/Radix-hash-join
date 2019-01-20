#ifndef QUERYTRANSLATION_H
#define QUERYTRANSLATION_H

typedef struct queryListNode{
	char* relations;
	char* predicates;
	char* projections;
	struct queryListNode* next;
}queryListNode;


queryListNode* queryListInit();
void queryListDelete( queryListNode* startNode );
void addQuery( queryListNode* start, char* line );
void printList( queryListNode* startNode );

#endif
