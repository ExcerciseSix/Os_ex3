/**
 *
 * @author: Hadas Jacobi, 2018.
 *
 * This client maps 14 ints to even or odd and sums both groups separately.
 *
 */

#include "MapReduceFramework.h"
#include <cstdio>
#include <iostream>

class Vint : public V1 {
public:
    explicit Vint(int content) : content(content) { }
	int content;
};

class Kbool : public K2, public K3{
public:
    explicit Kbool(bool i) : key(i) { }
	virtual bool operator<(const K2 &other) const {
		return key < static_cast<const Kbool&>(other).key;
	}
	virtual bool operator<(const K3 &other) const {
		return key < static_cast<const Kbool&>(other).key;
	}
	bool key;
};

class Vsum : public V2, public V3{
public:
	explicit Vsum(int sum) : sum(sum) { }
	int sum;
};


class modsumClient : public MapReduceClient {
public:
    // maps to even or odd
	void map(const K1* key, const V1* value, void* context) const {
        int c = static_cast<const Vint*>(value)->content;
        Kbool* k2;
        Vsum* v2 = new Vsum(c);

        if(c % 2 == 0){
            k2 = new Kbool(0);
        } else {
            k2 = new Kbool(1);
        }
        emit2(k2, v2, context);
	}

    // sums all evens and all odds
	virtual void reduce(const IntermediateVec* pairs, void* context) const {
		const bool key = static_cast<const Kbool*>(pairs->at(0).first)->key;
		int sum = 0;
		for(const IntermediatePair& pair: *pairs) {
			sum += static_cast<const Vsum*>(pair.second)->sum;
			delete pair.first;
			delete pair.second;
		}
		Kbool* k3 = new Kbool(key);
		Vsum* v3 = new Vsum(sum);
		emit3(k3, v3, context);
	}
};


//int main(int argc, char** argv)
//{
//	modsumClient client;
//	InputVec inputVec;
//	OutputVec outputVec;
//
//	Vint s1(86532);
//	Vint s2(657);
//	Vint s3(5);
//	Vint s4(546);
//	Vint s5(54);
//	Vint s6(7);
//	Vint s7(8888);
//	Vint s8(86532);
//	Vint s9(657);
//	Vint s10(5);
//	Vint s11(546);
//	Vint s12(54);
//	Vint s13(7);
//	Vint s14(8888);
//
//	inputVec.push_back({nullptr, &s1});
//	inputVec.push_back({nullptr, &s2});
//	inputVec.push_back({nullptr, &s3});
//	inputVec.push_back({nullptr, &s4});
//	inputVec.push_back({nullptr, &s5});
//	inputVec.push_back({nullptr, &s6});
//	inputVec.push_back({nullptr, &s7});
//	inputVec.push_back({nullptr, &s8});
//	inputVec.push_back({nullptr, &s9});
//	inputVec.push_back({nullptr, &s10});
//	inputVec.push_back({nullptr, &s11});
//	inputVec.push_back({nullptr, &s12});
//	inputVec.push_back({nullptr, &s13});
//	inputVec.push_back({nullptr, &s14});
//
//
//	waitForJob(startMapReduceJob(client, inputVec, outputVec, 4));
//
//	for (OutputPair& pair: outputVec) {
//		bool key = ((const Kbool*)pair.first)->key;
//		int sum = ((const Vsum*)pair.second)->sum;
//		printf("The sum of numbers where mod2 == %d is %d\n", key, sum);
//		delete pair.first;
//		delete pair.second;
//	}
//
//	return 0;
//}

