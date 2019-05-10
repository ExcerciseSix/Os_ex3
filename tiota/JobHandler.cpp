//
// Created by ofir.tziter on 5/4/19.
//

#include "JobHandler.h"
#include <map>
#include <algorithm>
#include <iostream>

using namespace std;

void* JobHandler::jobToExecute(void* context)
{
	
	auto tc = (ThreadContext*) context;
	auto jh = (JobHandler*) tc->jobHandler;
	int old_value = (*(tc->map_stage_counter))++;
	jh->state = {MAP_STAGE, 0};
	
	// all threads use map on elements form input vector
	while(old_value < jh->inputVec.size()){
		
		InputPair pair = jh->inputVec.at((unsigned int)old_value);
		jh->client.map(pair.first, pair.second, &(tc->mapResultVector));
//		cout << endl <<"nthread num " << tc->threadID << " , size of map result: " << tc->mapResultVector.size() << endl;
		jh->state.percentage = (old_value / float(jh->inputVec.size())) * 100;
		old_value = (*(tc->map_stage_counter))++;
		
	}
	sort(tc->mapResultVector.begin(), tc->mapResultVector.end(), &JobHandler::compareK2);

	tc->barrier->barrier();
	
	if (tc->threadID == 0) {
		cout <<"nthread num " << tc->threadID << endl;
		jh->shuffle();
	}
	jh->state = {REDUCE_STAGE, 0};
	
	int vectors_processed = 0;
	while (!jh->shuffledVectors.empty() || !jh->end_shuffle)
	{
		sem_wait(&tc->semaphore); //wait for shuffled vector

		if (jh->shuffledVectors.empty() && jh->end_shuffle)
		{
			sem_post(&tc->semaphore);
			break;
		}
		else
		{
			pthread_mutex_lock(&tc->shuffleMutex); //lock the shuffle vector to take job
			jh->client.reduce(&jh->shuffledVectors.front(), tc);
			jh->shuffledVectors.pop();
			pthread_mutex_unlock(&tc->shuffleMutex);
			vectors_processed++;
			jh->state.percentage = (vectors_processed / float(jh->inputVec.size())) * 100;
			//TODO sem_post?
		}

		if(jh->shuffledVectors.empty() && jh->end_shuffle){
			sem_post(&tc->semaphore);
		}
	}
	
	return jh;
}

bool JobHandler::compareK2(IntermediatePair &Pair1, IntermediatePair &Pair2)
{
	return *Pair1.first < *Pair2.first;
}

bool JobHandler::equalK2(IntermediatePair & Pair1, IntermediatePair & Pair2)
{
	return !(*Pair1.first < *Pair2.first) && !(*Pair2.first < *Pair1.first);
}


void JobHandler::shuffle()
{
	while (!isResultVectorsEmpty())
	{
		int firstNotEmptyIndex = 0;
		while (contexts[firstNotEmptyIndex].mapResultVector.empty()){
			++firstNotEmptyIndex;
		}
		generateK2Vector(firstNotEmptyIndex);
		sem_post(&contexts[0].semaphore);
	}
}


int JobHandler::generateK2Vector(int maxKeyIndex)
{
	IntermediateVec shuffledVector;
	for(int i = (maxKeyIndex + 1); i < numOfThreads; ++i){
		if (!contexts[i].mapResultVector.empty())
		{
			if ( compareK2(contexts[maxKeyIndex].mapResultVector.back(), contexts[i].mapResultVector.back()) )
			{
				maxKeyIndex = i;
			}
		}
	}
	
	// let the max lead the shuffled vector
	shuffledVector.push_back(contexts[maxKeyIndex].mapResultVector.back());
	contexts[maxKeyIndex].mapResultVector.pop_back();
	cout << "first entry, current size: " << shuffledVector.size() << endl;

	//add all the others
	for(int i = 0; i<numOfThreads; ++i){
		if (!contexts[i].mapResultVector.empty())
		cout << " more values " << endl ;
		{
			while (equalK2(contexts[i].mapResultVector.back(), shuffledVector.front()))
			{
				cout << "equal: " << endl;
				shuffledVector.push_back(contexts[i].mapResultVector.back());
				contexts[i].mapResultVector.pop_back();
				cout << "size: " << shuffledVector.size() << endl;
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
	while (contexts[index].mapResultVector.empty()) ++index;
	return index == numOfThreads;
}
