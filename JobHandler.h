//
// Created by ofir.tziter on 5/4/19.
//

#ifndef OSEX3_JOBHANDLER_H
#define OSEX3_JOBHANDLER_H

#include "MapReduceFramework.h"
#include "ThreadContext.h"
#include "Barrier.h"
#include <atomic>
#include <queue>

using namespace std;

class JobHandler
{
public:
	JobHandler(const MapReduceClient & client, pthread_t *threads, vector<ThreadContext> &contexts, int numOfThreads,
		           JobState state, const InputVec & inputVec, OutputVec & outputVec) :
			client(client), threads(threads), contexts(contexts), numOfThreads(numOfThreads),
			state(state), inputVec(inputVec), outputVec(outputVec){ }
			
	const MapReduceClient & client;
	pthread_t *threads;
	vector<ThreadContext> contexts;
	int numOfThreads;
	JobState state;
	const InputVec & inputVec;
	OutputVec & outputVec;
	queue<IntermediateVec> shuffledVectors= {};
	
	static bool compareK2(IntermediatePair & Pair1, IntermediatePair & Pair2);
	
	static bool equalK2(IntermediatePair & Pair1, IntermediatePair & Pair2);
	
	void shuffle();
	
	int generateK2Vector(int maxKeyIndex);
	
	bool isResultVectorsEmpty();
	
	/**
	* The function each thread runs
	* @param context
	* @return
	*/
	static void* jobToExecute(void *context);
	
};
#endif //OSEX3_JOBHANDLER_H
