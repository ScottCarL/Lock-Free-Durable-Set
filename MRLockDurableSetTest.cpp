// MRLock Durable Set Test

#include <iostream>
#include <atomic>
#include <vector>
#include <thread>
#include <chrono>
#include <random>
#include <cctype>
#include <string>
#include <cstdint>
#include "MemoryManager.h"
#include "MRLockDurableSet.h"

/*****************************************************/
// Frozen Global Variables
int numThreads = 4;        // MIN of 4 / MAX of 4
int abortTime = 15;        // (milliseconds)
int itemRange = 10;        // item (integer)
// MIN_KEY = -100000;
// MAX_KEY = 100000;

// CMDL Arguements
int numOps = 0;          // MIN of 5 / MAX of 150000
int insertChance = 0;    // 1-5 :: 5 out of 10 inserts
int removeChance = 0;    // 6-8 :: 3 out of 10 removes
/*****************************************************/

// Prototype
bool incorrect_args(int, char* []);
long hash(int);

// Thread will run until out of operations
void runThread(int id, std::vector<int>* controlVector,
                       std::vector<int>* itemsVector,
                       MRLockDurableSet<int>* durableSet, int& delta, std::atomic<bool>* abortFlag) {

    int controlValue = 0;
    int item = 0;
    for (int i = 0; i < numOps; i++) {
        controlValue = controlVector->at(i);
        item = itemsVector->at(i);

        if (controlValue <= insertChance) {

            // Abort check (For abort testing only)
            // if (abortFlag->load() == true) break;

            if (durableSet->insert(hash(item), item, id))
                delta += 1;

            // For testing with few operations
            // std::cout  << "Thread " << id << " inserting " << item << std::endl;
            // if (durableSet->insert(hash(item), item, id)) {
            //     delta += 1;
            //     std::cout << "insert successful" << std::endl;
            // }
            // else
            //     std::cout << "insert unsuccessful" << std::endl;

        } else if (controlValue <= removeChance) {

            // Abort check (For abort testing only)
            // if (abortFlag->load() == true) break;

            if (durableSet->remove(hash(item), id))
                delta -= 1;

            // For testing with few operations
            // std::cout  << "Thread " << id << " removing " << item << std::endl;
            // if (durableSet->remove(hash(item), id)) {
            //     delta -= 1;
            //     std::cout << "remove successful" << std::endl;
            // }
            // else
            //     std::cout << "remove unsuccessful" << std::endl;

        } else {

            // Abort check (For abort testing only)
            // if (abortFlag->load() == true) break;

            durableSet->contains(hash(item));

            // For testing with few operations
            // std::cout  << "Thread " << id << " contains " << item << std::endl;
            // if (durableSet->contains(hash(item)))
            //     std::cout << "contains successful" << std::endl;
            // else
            //     std::cout << "contains unsuccessful" << std::endl;

        }

    }
}

// Three arguements (number of operations, chance of insert and remove)
int main(int argc, char* argv[]) {

    std::atomic<bool>* abortFlag = new std::atomic<bool>(false);

    if (incorrect_args(argc, argv)) return 0;

    // Prepare the decision and items vectors for each thread
    std::vector<std::vector<int>*> insertORdeleteThreads = std::vector<std::vector<int>*>(numThreads);
    std::vector<std::vector<int>*> itemsThreads = std::vector<std::vector<int>*>(numThreads);
    for (int i = 0; i < numThreads; i++) {
        insertORdeleteThreads.at(i) = new std::vector<int>(numOps);
        itemsThreads.at(i) = new std::vector<int>(numOps);
    }

    // Populate (randomly) the decision and items vectors
    std::mt19937 rng(std::random_device{}());
    int rand;
    for (int i = 0; i < numThreads; i++) {
        for (int j = 0; j < numOps; j++) {
            rand = std::uniform_int_distribution<>{1, 10}(rng);
            insertORdeleteThreads.at(i)->at(j) = int(rand);
            rand = std::uniform_int_distribution<>{0, itemRange}(rng);
            itemsThreads.at(i)->at(j) = int(rand);
        }
    }

    // Determine the number of writeOps for each thread (optimize memory use)
    int maxWriteOps = 0;
    std::vector<int>* writeOpsVector = new std::vector<int>(numThreads);  // Copied by the durable set class
    for (int i = 0; i < numThreads; i++) {
        writeOpsVector->at(i) = 0;
        for (int j = 0; j < numOps; j++) {
            if (insertORdeleteThreads.at(i)->at(j) <= insertChance) {
                writeOpsVector->at(i) += 1;
            }
        }
        if (writeOpsVector->at(i) > maxWriteOps) maxWriteOps = writeOpsVector->at(i);
    }

    // Used for testing, verifies correct number of operations (assume no crash)
    std::vector<int> delta = std::vector<int>(numThreads);
    for (int i = 0; i < numThreads; i++) delta.at(i) = 0;
    

    // Construct Memory Manager (item type integer)
    MemoryManager<int>* mem = new MemoryManager<int>(numThreads, maxWriteOps);
    
    // Construct the Set
    MRLockDurableSet<int>* durableSet = new MRLockDurableSet<int>(mem, abortFlag, numThreads, writeOpsVector);

    // Start timer
    auto start = std::chrono::steady_clock::now();

    // Start threads
    std::vector<std::thread> opThreads;
    for (int i = 0; i < numThreads; i++) {
        opThreads.push_back(std::thread(runThread, i, insertORdeleteThreads.at(i),
                                                      itemsThreads.at(i),
                                                      durableSet,
                                                      std::ref(delta.at(i)),
                                                      abortFlag));
    }

    // Abort timer
    // auto beginTime = std::chrono::steady_clock::now();
    // auto currentTime = std::chrono::steady_clock::now();
    // while (true) {
    //     currentTime = std::chrono::steady_clock::now();
    //     if (std::chrono::duration_cast<std::chrono::milliseconds>(currentTime-beginTime).count() >= abortTime) {
    //         abortFlag->store(true, std::memory_order_release);
    //         break;
    //     }
    // }

    // Wait for threads
    for (int i = 0; i < numThreads; i++) {
        opThreads.at(i).join();
    }

    // Stop timer
    auto end = std::chrono::steady_clock::now();

    // Used for testing
    int totalDelta = 0;
    for (int i = 0; i < numThreads; i++) totalDelta += delta.at(i);

    // Output (testing)
    std::cout << "Computational runtime was "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end-start).count()
              << " milliseconds"
              << std::endl;
    std::cout << "Total of " << numThreads << " Threads: insert(), remove() and contains() operations" << std::endl;
    std::cout << "Total of " << numOps << " operations for each thread" << std::endl;
    std::cout << "Total delta: " << totalDelta << " should equal size of the set (disregard for abort tests)" << std::endl;

    // durableSet->printSet();  // Useful for only small test cases (not for abort test)
    durableSet->printSetSize();

    //durableSet->recover(writeOpsVector);  // (For abort testing only)
    //durableSet->printRecovery();          // (For abort testing only)

    // Clean up
    for (int i = 0; i < numThreads; i++) {
        delete insertORdeleteThreads.at(i);
        delete itemsThreads.at(i);
    }
    delete abortFlag;
    delete writeOpsVector;
    delete mem;         // No loose memory in mem
    durableSet->FREE();
    delete durableSet;  // No loose memory in durableSet

    return 0;
}

// Parses the argument
bool incorrect_args(int argc, char* argv[]) {
    if (argc != 4) {
        std::cout << "Incorrect number of arguments." << std::endl;
        return true;
    }
    std::string str1 = argv[1];
    std::string str2 = argv[2];
    std::string str3 = argv[3];
    int len1 = str1.length();
    int len2 = str2.length();
    int len3 = str3.length();
    for (int i = 0; i < len1; i++) {
        if (!std::isdigit(str1[i])) {
            std::cout << "One of the Arguments is not a positive integer." << std::endl;
            return true;
        }
    }
    for (int i = 0; i < len2; i++) {
        if (!std::isdigit(str2[i])) {
            std::cout << "One of the Arguments is not a positive integer." << std::endl;
            return true;
        }
    }
    for (int i = 0; i < len3; i++) {
        if (!std::isdigit(str3[i])) {
            std::cout << "One of the Arguments is not a positive integer." << std::endl;
            return true;
        }
    }
    numOps = std::stoi(str1);
    insertChance = std::stoi(str2);
    removeChance = std::stoi(str3);
    if (numOps > 150000 || numOps < 5) {
        std::cout << "Second arguement is not an integer from 5 to 150000." << std::endl;
        return true;
    }
    if (insertChance > 10 || insertChance < 3) {
        std::cout << "Third arguement is not an integer from 3 to 10." << std::endl;
        return true;
    }
    if (removeChance > 6 || removeChance < 0) {
        std::cout << "Third arguement is not an integer from 0 to 6." << std::endl;
        return true;
    }
    if ((insertChance + removeChance) > 10) {
        std::cout << "The third and fourth arguements" << std::endl;
        std::cout << "do not add to an integer less than 10." << std::endl;
        return true;
    }
    removeChance = insertChance + removeChance;
    return false;
}

// Dummy Hash function for testing purposes
// One-to-one relationship between the items and keys.
long hash(int item) {
    return (long) item;
}
