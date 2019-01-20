all: mainP2

mainP2:
	gcc -o mainP2 -g  mainP2.c infoTable.c queryTranslation.c queryManagement.c histogram.c relation.c result.c JobQueue.c -lpthread

main2:
	gcc -o main2 main2.c histogram.c relation.c result.c -lpthread

clean:
	rm -rf *o mainP2 main2
