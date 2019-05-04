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
	ThreadContext(int threadID, Barrier *barrier, atomic<int> *atomic_counter, void *jh, sem_t &semaphore): threadID(threadID),
				barrier(barrier), atomic_counter(atomic_counter), jobHandler(jh), semaphore(semaphore) { }
	
	void *jobHandler; //each thread know it's boss
	int threadID;
	Barrier *barrier;
	atomic<int> *atomic_counter; // amount of job done by threads so far
	IntermediateVec mapResultVector;
	pthread_mutex_t inputMutexVec = PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_t shuffleMutex = PTHREAD_MUTEX_INITIALIZER;
	pthread_mutex_t outputMutexVec = PTHREAD_MUTEX_INITIALIZER;
	sem_t semaphore;
};


#endif //OSEX3_JOBCONTEXT_H
