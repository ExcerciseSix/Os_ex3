#include "MapReduceFramework.h"
#include "Barrier.h"
#include <pthread.h>
#include <vector>
#include <atomic>
#include <algorithm>
#include <queue>
#include <iostream>
#include "semaphore.h"


using namespace std;
class ThreadContext;
class JobHandler;

template <class Pair>
struct comparator {
	bool operator()(Pair pair1, Pair pair2) const {
		return *(pair1.first) < *(pair2.first);
	}
};

//-------------------------------------Thread's context class-------------------------------------//

class ThreadContext
{
public:
	ThreadContext() {}
	ThreadContext(int threadID, JobHandler *jh): threadID(threadID), jobHandler(jh)
	{
		mapResultVector= IntermediateVec();
	}
	int threadID;
	JobHandler *jobHandler; //each thread know it's boss
	IntermediateVec mapResultVector;
};

//---------------------------------------Job Handler class---------------------------------------//

class JobHandler
{
public:

	JobHandler(const MapReduceClient &client, int numOfThreads, const InputVec &inputVec, OutputVec &outputVec);
	const MapReduceClient *client;
	vector<pthread_t> threads;
	vector<ThreadContext> contexts;
	int numOfThreads;
	const InputVec *inputVec;
	OutputVec *outputVec;
	JobState state;

	bool wasJoined;
	bool shuffleFinished;
	bool semaphoreRealised;
	queue<IntermediateVec*> shuffledVectors;

	int inputPairsCounter;
	atomic<int> mapStageCounter;
	atomic<int> reduceStageCounter;
	Barrier barrier;
	sem_t semaphore;
	pthread_mutex_t shuffleMutex;
	pthread_mutex_t outputVecMutex;
	pthread_mutex_t reduceQueueMutex;
	pthread_mutex_t stateMutex;
	pthread_mutex_t semaphoreMutex;
};

JobHandler::JobHandler(const MapReduceClient &client, int numOfThreads, const InputVec &inputVec, OutputVec &outputVec):
		client(&client), threads((unsigned long)numOfThreads),
		numOfThreads(numOfThreads), inputVec(&inputVec), outputVec(&outputVec),
		contexts((unsigned long)numOfThreads), state({UNDEFINED_STAGE, 0}),
		wasJoined(false), shuffleFinished(false), semaphoreRealised(false), inputPairsCounter(0),
		mapStageCounter(0), reduceStageCounter(0), barrier(numOfThreads), shuffleMutex(PTHREAD_MUTEX_INITIALIZER),
		outputVecMutex(PTHREAD_MUTEX_INITIALIZER), reduceQueueMutex(PTHREAD_MUTEX_INITIALIZER),
		stateMutex(PTHREAD_MUTEX_INITIALIZER), semaphoreMutex(PTHREAD_MUTEX_INITIALIZER)
{
	if (sem_init(&semaphore, 0, 0))
	{
		cout << "initializing of the semaphore failed" << endl;
	}
}


//---------------------------------------additional functions---------------------------------------//

void mapStage(ThreadContext *tc);
void shuffleStage(ThreadContext *tc);
void reduceStage(ThreadContext *tc);
void freeSemaphore(ThreadContext *tc);
void generateK2Vector(ThreadContext *tc, int maxKeyIndex);
bool isResultVectorsEmpty(ThreadContext *tc);
void error(string const &error);

void* jobToExecute(void* threadContext)
{
	auto tc = static_cast<ThreadContext*> (threadContext);
	mapStage(tc);

	if (tc->threadID == 0) {
		for (int i = 0; i < tc->jobHandler->numOfThreads; ++i) //calculating total num of pairs in all mapped vectors
		{
			tc->jobHandler->inputPairsCounter += tc->jobHandler->contexts[i].mapResultVector.size();
		}
		tc->jobHandler->state = {REDUCE_STAGE, 0};
		shuffleStage(tc);
		tc->jobHandler->shuffleFinished = true;
	}

	while (!tc->jobHandler->shuffleFinished || !tc->jobHandler->shuffledVectors.empty())
	{
		reduceStage(tc);
	}

	freeSemaphore(tc);
	return nullptr;
}

bool equalK2(IntermediatePair & Pair1, IntermediatePair & Pair2)
{
	return (*Pair1.first < *Pair2.first) && (*Pair2.first < *Pair1.first);
}

void mapStage(ThreadContext *tc)
{
	int old_value = (tc->jobHandler->mapStageCounter)++;
	// all threads use map on elements form input vector
	while(old_value < (*tc->jobHandler->inputVec).size())
	{
		cout << tc->threadID << old_value << endl;
		auto inputPair = (*tc->jobHandler->inputVec)[old_value];
		tc->jobHandler->client->map(inputPair.first, inputPair.second, tc);
		old_value = (tc->jobHandler->mapStageCounter)++;
	}
	tc->jobHandler->barrier.barrier();
	sort(tc->mapResultVector.begin(), tc->mapResultVector.end(), comparator<IntermediatePair>());
	tc->jobHandler->barrier.barrier(); // everything is mapped and sorted
}

void shuffleStage(ThreadContext *tc)
{
	while (!isResultVectorsEmpty(tc))
	{
		int firstNotEmptyIndex = 0;
		while (tc->jobHandler->contexts[firstNotEmptyIndex].mapResultVector.empty()){
			++firstNotEmptyIndex;
		}
		generateK2Vector(tc, firstNotEmptyIndex);
		if (sem_post(&tc->jobHandler->semaphore))
		{
			error("sem_post error in shuffle stage");
		}
	}
}

void reduceStage(ThreadContext *tc)
{
	sem_wait(&tc->jobHandler->semaphore); //wait for shuffled vector
	if (pthread_mutex_lock(&(tc->jobHandler->reduceQueueMutex))) //take one vector from shuffled vectors
	{
		error("pthread_mutex_lock error in reduce stage");
	}
	auto shuffledVec = tc->jobHandler->shuffledVectors.front();
	tc->jobHandler->shuffledVectors.pop();
	if (pthread_mutex_unlock(&(tc->jobHandler->reduceQueueMutex)))
	{
		error("pthread_mutex_unlock error in reduce stage");
	}
	tc->jobHandler->client->reduce(shuffledVec, tc);
	tc->jobHandler->reduceStageCounter += shuffledVec->size();
	delete shuffledVec;
}

void freeSemaphore(ThreadContext *tc)
{
	if (pthread_mutex_lock(&(tc->jobHandler->semaphoreMutex)))
	{
		error("pthread_mutex_lock error in semaphore free");
	}
	if (!tc->jobHandler->semaphoreRealised)
	{
		for (int i=0; i < tc->jobHandler->numOfThreads; ++i)
		{
			if (sem_post(&tc->jobHandler->semaphore))
			{
				error("sem_post error in shuffle stage");
			}
		}
		tc->jobHandler->semaphoreRealised = true;
	}
	if (pthread_mutex_unlock(&(tc->jobHandler->semaphoreMutex)))
	{
		error("pthread_mutex_unlock error in semaphore free");
	}
}

void generateK2Vector(ThreadContext *tc, int maxKeyIndex)
{
//    IntermediateVec shuffledVector;
	for(int i = maxKeyIndex; i < tc->jobHandler->numOfThreads; ++i){
		if (!tc->jobHandler->contexts[i].mapResultVector.empty())
		{
			if ( tc->jobHandler->contexts[maxKeyIndex].mapResultVector.back() <
				 tc->jobHandler->contexts[i].mapResultVector.back())
			{
				maxKeyIndex = i;
			}
		}
	}

	auto shuffledVector = new IntermediateVec; //the maxKeyIndex lead the way  //TODO maybe should be deleted
	shuffledVector->push_back(tc->jobHandler->contexts[maxKeyIndex].mapResultVector.back());
	tc->jobHandler->contexts[maxKeyIndex].mapResultVector.pop_back();

	for(int i = 0; i < tc->jobHandler->numOfThreads; ++i)  //add all the others
	{
		if (!tc->jobHandler->contexts[i].mapResultVector.empty())
		{
			auto pair = tc->jobHandler->contexts[i].mapResultVector.back(); //TODO maybe should be deleted
			while (equalK2(pair, shuffledVector->front()))
			{
				shuffledVector->push_back(tc->jobHandler->contexts[i].mapResultVector.back());
				tc->jobHandler->contexts[i].mapResultVector.pop_back();
			}
		}
	}
	if (pthread_mutex_lock(&(tc->jobHandler->shuffleMutex)))
	{
		error("pthread_mutex_lock error in adding shuffled vector to all shuffled vectors");
	}
	tc->jobHandler->shuffledVectors.push(shuffledVector); //TODO hope no valgrind errors
	if (pthread_mutex_unlock(&(tc->jobHandler->shuffleMutex)))
	{
		error("pthread_mutex_unlock error in adding shuffled vector to all shuffled vectors");
	}
}

bool isResultVectorsEmpty(ThreadContext *tc)
{
	int index = 0;
	while (tc->jobHandler->contexts[index].mapResultVector.empty()) ++index;
	return index == tc->jobHandler->numOfThreads;
}

void error(string const &error)
{
	cout << "Library error: " << error << endl;
	exit(1);
}
//---------------------------------------library functions---------------------------------------//

JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec& inputVec,
							OutputVec& outputVec, int multiThreadLevel) {

	auto jh = new JobHandler(client, multiThreadLevel, inputVec, outputVec);
	for (int i = 0; i < multiThreadLevel; ++i) {
		jh->contexts[i] = ThreadContext(i, jh);

		if (pthread_create(&(jh->threads[i]), nullptr, jobToExecute, &(jh->contexts[i])))
		{
			error("pthread_create failed");
		}
	}

	jh->state = {MAP_STAGE, 0};
	return (JobHandle )jh;
}


void emit2(K2 *key, V2 *value, void *context)
{
	auto tc = static_cast<ThreadContext*> (context);
	tc->mapResultVector.emplace_back(IntermediatePair{key,value});
}

void emit3(K3 *key, V3 *value, void *context)
{
	auto tc = static_cast<ThreadContext*> (context);
	if (pthread_mutex_lock(&(tc->jobHandler->outputVecMutex)))
	{
		error("pthread_mutex_lock error in reduce stage, in emit3");
	}
	tc->jobHandler->outputVec->emplace_back(OutputPair{key, value});
	if (pthread_mutex_unlock(&(tc->jobHandler->outputVecMutex)))
	{
		error("pthread_mutex_unlock error in reduce stage, in emit3");
	}
}

void waitForJob(JobHandle job)
{
	auto jh = static_cast<JobHandler*> (job);
	if (!jh->wasJoined)
	{
		for (int i = 0; i < jh->numOfThreads; i++)
		{
			pthread_join(jh->threads[i], nullptr);
		}
		jh->wasJoined = true;
	}
}


void getJobState(JobHandle job, JobState *state)
{
	auto jh = static_cast<JobHandler*> (job);
	if (pthread_mutex_lock(&(jh->stateMutex)))
	{
		error("update state percentage failed ");
	}
	if (jh->state.stage == MAP_STAGE)
	{
//		cout << "mapcounter " << jh->mapStageCounter << "input size: " << jh->inputVec->size() << endl;
		jh->state.percentage = ((jh->mapStageCounter) / jh->inputVec->size()) * 100;
	}
	if (jh->state.stage == REDUCE_STAGE)
	{
		jh->state.percentage = ((jh->reduceStageCounter) / jh->inputPairsCounter) * 100; //TODO maybe the inputPair counter is not a solution here
	}
	if (pthread_mutex_unlock(&(jh->stateMutex)))
	{
		error("update state percentage failed ");
	}
	*state = jh->state;
}


void closeJobHandle(JobHandle job)
{
	waitForJob(job);
	auto jh = static_cast<JobHandler*> (job);
	delete jh;
}