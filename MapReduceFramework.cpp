#include "MapReduceFramework.h"
#include "JobHandler.h"
#include "ThreadContext.h"
#include "Barrier.h"
#include <pthread.h>
#include <vector>
#include <atomic>
#include <algorithm>

using namespace std;


JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec,
							OutputVec& outputVec, int multiThreadLevel) {
	
	pthread_t threads[multiThreadLevel];
	vector<ThreadContext> contexts;
    auto * barrier = new Barrier(multiThreadLevel);
	JobState state = {UNDEFINED_STAGE, 0};
	atomic<int> atomic_counter(0);
	sem_t semaphore;
	sem_init(&semaphore, 0, 0); //TODO free semaphore
	//TODO free JobHandler
	JobHandler* jh = new JobHandler(client, threads, contexts, multiThreadLevel, state, inputVec, outputVec);
	for (int i = 0; i < multiThreadLevel; ++i) {
		jh->contexts.emplace_back(i, barrier, &atomic_counter, jh, semaphore); //TODO check if context updated
	}
	
	for (int i = 0; i < multiThreadLevel; ++i) {
		pthread_create(&threads[i], NULL, JobHandler::jobToExecute, &jh->contexts[i]);
	}
	
	jh->state = {MAP_STAGE, 0}; //TODO check if its the place
	return jh;
}


void emit2(K2 *key, V2 *value, void *context)
{
	IntermediateVec* mapResultVector = (IntermediateVec*) context;
	IntermediatePair pair = {key, value};
	(*mapResultVector).push_back(pair);
}

void emit3(K3 *key, V3 *value, void *context)
{
	ThreadContext* tc = (ThreadContext*) context;
	pthread_mutex_lock(&tc->outputMutexVec);
	auto jh = (JobHandler*) tc->jobHandler;
	OutputPair pair = {key, value};
	jh->outputVec.push_back(pair);
	pthread_mutex_unlock(&tc->outputMutexVec);
}

void waitForJob(JobHandle job)
{
	//TODO what this functions do?
	auto jh = (JobHandler*) job;
	for (int i = 0; i < jh->numOfThreads; ++i) {
		pthread_join(jh->threads[i], NULL);
	}
}

void getJobState(JobHandle job, JobState *state)
{
	*state = ((JobHandler*)job)->state;
	//TODO - understand how to update state, precentage
}

void closeJobHandle(JobHandle job)
{
	//TODO : call wait for job if job is not done
}
