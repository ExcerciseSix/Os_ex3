//
// Created by ofir.tziter on 5/4/19.
//

#include "JobHandler.h"
#include <algorithm>
#include <iostream>

using namespace std;

void* JobHandler::jobToExecute(void* context)
{
	
	auto tc = (ThreadContext*) context;
	auto jh = (JobHandler*) tc->jobHandler;
	int old_value = (*(tc->atomic_counter))++;
	
	// all threads use map on elements form input vector
	while(old_value < jh->inputVec.size()){
		
		pthread_mutex_lock(&(tc->inputMutexVec));
		InputPair pair = jh->inputVec.at((unsigned int)old_value);
		jh->client.map(pair.first, pair.second, &(tc->mapResultVector));
		old_value = (*(tc->atomic_counter))++;
		pthread_mutex_unlock(&(tc->inputMutexVec));
		
	}
	sort(tc->mapResultVector.begin(), tc->mapResultVector.end(), &JobHandler::compareK2);
	
	cout<< tc->threadID << " thread id " << endl;
	tc->barrier->barrier();
	
	jh->state = {REDUCE_STAGE, 0};
	if (tc->threadID == 0) {
		jh->shuffle();
	}
	
	while (jh->state.percentage < 100)
	{
		sem_wait(&tc->semaphore); //wait for shuffled vector
		pthread_mutex_lock(&tc->shuffleMutex); //lock the shuffle vector to take job
		IntermediateVec reduceVector = jh->shuffledVectors.front();
		jh->shuffledVectors.pop();
		pthread_mutex_unlock(&tc->shuffleMutex);
		jh->client.reduce(&reduceVector, tc);
		//TODO sem_post?
	}
	return nullptr;
}

bool JobHandler::compareK2(IntermediatePair & Pair1, IntermediatePair & Pair2)
{
	return *Pair1.first < *Pair2.first;
}

bool JobHandler::equalK2(IntermediatePair & Pair1, IntermediatePair & Pair2)
{
	return (*Pair1.first < *Pair2.first) && (*Pair2.first < *Pair1.first);
}


void JobHandler::shuffle()
{
	while (!isResultVectorsEmpty())
	{
		int firstNotEmptyIndex = 0;
		while (contexts[firstNotEmptyIndex].mapResultVector.empty() ) ++firstNotEmptyIndex;
		generateK2Vector(firstNotEmptyIndex);
		sem_post(&contexts[0].semaphore);
	}
}


int JobHandler::generateK2Vector(int maxKeyIndex)
{
	IntermediateVec shuffledVector;
	
	for(int i = 1; i<numOfThreads; ++i){
		if (!contexts[i].mapResultVector.empty())
		{
			if (contexts[i].mapResultVector.back().first > contexts[maxKeyIndex].mapResultVector.back().first)
			{
				maxKeyIndex = i;
			}
		}
	}
	// let the max lead the shuffled vector
	shuffledVector.push_back(contexts[maxKeyIndex].mapResultVector.back());
	contexts[maxKeyIndex].mapResultVector.pop_back();
	
	//add all the others
	for(int i = 0; i<numOfThreads; ++i){
		if (!contexts[i].mapResultVector.empty())
		{
			while (equalK2(contexts[i].mapResultVector.back(), contexts[maxKeyIndex].mapResultVector.back()))
			{
				shuffledVector.push_back(contexts[i].mapResultVector.back());
				contexts[i].mapResultVector.pop_back();
			}
		}
	}
	pthread_mutex_lock(&contexts[0].shuffleMutex);
	shuffledVectors.push(shuffledVector);
	pthread_mutex_unlock(&contexts[0].shuffleMutex);
	return 0;
}


bool JobHandler::isResultVectorsEmpty()
{
	int index = 0;
	while (contexts[index].mapResultVector.empty() && index < numOfThreads) ++index;
	return index == numOfThreads;
}
