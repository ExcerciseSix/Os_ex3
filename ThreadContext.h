//
// Created by ofir.tziter on 5/4/19.
//

#ifndef OSEX3_JOBCONTEXT_H
#define OSEX3_JOBCONTEXT_H

#include "MapReduceFramework.h"
#include "Barrier.h"
#include <atomic>
#include <vector>
#include <semaphore.h>

using namespace std;

class ThreadContext
{
public:
	ThreadContext(int threadID, Barrier *barrier, atomic<int> *map_stage_counter, void *jh, sem_t &semaphore): threadID(threadID),
				barrier(barrier), map_stage_counter(map_stage_counter), jobHandler(jh), semaphore(semaphore) { }
	
	void *jobHandler; //each thread know it's boss
	int threadID;
	Barrier *barrier;
	atomic<int> *map_stage_counter;
	IntermediateVec mapResultVector = {};
	pthread_mutex_t inputVecMutex = PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_t shuffleMutex = PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_t outputVecMutex = PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_t reduceQueueMutex = PTHREAD_MUTEX_INITIALIZER;
	sem_t semaphore;
};


#endif //OSEX3_JOBCONTEXT_H
