#include "MapReduceFramework.h"
#include "JobHandler.h"
#include "ThreadContext.h"
#include "Barrier.h"
#include <pthread.h>
#include <vector>
#include <atomic>
#include <algorithm>
#include <iostream>

using namespace std;


JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec,
							OutputVec& outputVec, int multiThreadLevel) {
	
	pthread_t threads[multiThreadLevel];
	vector<ThreadContext> contexts;
    auto * barrier = new Barrier(multiThreadLevel);
	JobState state = {UNDEFINED_STAGE, 0};
	atomic<int> map_stage_counter(0);
	sem_t semaphore;
	sem_init(&semaphore, 0, 0); //TODO free semaphore
	
	JobHandler* jh = new JobHandler(client, threads, contexts, multiThreadLevel, state, inputVec, outputVec);
	for (int i = 0; i < multiThreadLevel; ++i) {
		jh->contexts.emplace_back(i, barrier, &map_stage_counter, jh, semaphore);
	}
	
	for (int i = 0; i < multiThreadLevel; ++i) {
		if (pthread_create(&threads[i], NULL, JobHandler::jobToExecute, &jh->contexts[i]) != 0)
		{
			cerr<<"MapReduceFramework Failure: pthread_create failed"<<endl;
			exit(1);
		}
	}
	jh->state = {MAP_STAGE, 0};
	
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
	auto jh = (JobHandler*) tc->jobHandler;
	pthread_mutex_lock(&tc->outputVecMutex);
	jh->outputVec.push_back({key, value});
	pthread_mutex_unlock(&tc->outputVecMutex);
}

void waitForJob(JobHandle job)
{
	//TODO what this functions do?
	JobHandler* jh = (JobHandler*) job;
	if (!jh->wasJoined)
	{
		for (int i = 0; i < jh->numOfThreads; i++)
		{
			sem_post(&jh->contexts[i].semaphore);
			pthread_join(jh->threads[i], NULL);
		}
		jh->wasJoined = true;
	}
}

void getJobState(JobHandle job, JobState *state)
{
	*state = ((JobHandler*)job)->state;
}

void closeJobHandle(JobHandle job)
{
	JobHandler* jh = (JobHandler*) job;
	if (!jh->wasJoined)
	{
		waitForJob(job);
	}
	delete jh->contexts[0].barrier;
	delete jh;
}
