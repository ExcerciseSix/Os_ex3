#include "MapReduceFramework.h"
#include "Barrier.h"
#include <pthread.h>
#include <vector>
#include <atomic>

using namespace std;

void* jobToExecute(void* context);

class JobContext
{
public:
	JobContext(int numOfThreads, int threadID,	JobState state,	Barrier* barrier, const MapReduceClient& client,
			const InputVec& inputVec, OutputVec& outputVec, atomic<int>* atomic_counter) :
			numOfThreads(numOfThreads), threadID(threadID), state(state), barrier(barrier), client(client),
			inputVec(inputVec), outputVec(outputVec), atomic_counter(atomic_counter) {}
	int numOfThreads;
	int threadID;
	JobState state;
	Barrier* barrier;
	const MapReduceClient& client;
	const InputVec& inputVec;
	OutputVec& outputVec;
	atomic<int>* atomic_counter;
};


JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec,
							OutputVec& outputVec, int multiThreadLevel) {
	
	pthread_t threads[multiThreadLevel];
	vector<JobContext> contexts;
	Barrier barrier(multiThreadLevel);
	JobState state ={UNDEFINED_STAGE, 0};
	atomic<int> atomic_counter(0);
	
	for (int i = 0; i < multiThreadLevel; ++i) {
		contexts.push_back(JobContext(multiThreadLevel, i, state, &barrier, client, inputVec, outputVec, &atomic_counter));
	}
	
	for (int i = 0; i < multiThreadLevel; ++i) {
		pthread_create(&threads[i], NULL, jobToExecute, &contexts[i]);
	}
	
	for (int i = 0; i < multiThreadLevel; ++i) {
		pthread_join(threads[i], NULL);
	}
}


void* jobToExecute(void* context)
{
	JobContext* jc = (JobContext*) context;
	int old_value = (*(jc->atomic_counter))++;
	jc->client.map(jc->inputVec[old_value].first, jc->inputVec[old_value].second, context);
	
	jc->barrier->barrier();
	
}


void emit2(K2 *key, V2 *value, void *context)
{

}

void emit3(K3 *key, V3 *value, void *context)
{

}

void waitForJob(JobHandle job)
{

}

void getJobState(JobHandle job, JobState *state)
{
	*state = ((JobContext*)job)->state;
}

void closeJobHandle(JobHandle job)
{

}
