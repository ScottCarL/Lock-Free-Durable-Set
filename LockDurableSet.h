#ifndef LOCK_DURABLE_SET_H
#define LOCK_DURABLE_SET_H

// Lock Durable Set Class

#include <iostream>
#include <atomic>
#include <mutex>
#include <vector>
#include <cstdint>
#include "MemoryManager.h"

long MIN_KEY = -100000;
long MAX_KEY = 100000;

template <typename T>
class LockDurableSet {

  public:

      // Similar field to the node in MemoryManager
      // (except) durableAddress(Pre/Post)fix
      // (except) resourceID
      struct Node {

          long key;
          T item;
          int validBits;             // Used for validiting insert
          Node* next;                // Marked for logical delete
          std::mutex mtx;

          // These are for the simulation only
          int durableAddressPrefix;      // Is the threads id
          int durableAddressPostfix;     // Is the element index in the memPool

          // Constructor
          Node(void) {
              this->key = 0;
              this->item = (T) 0;
              this->validBits = 0;
              this->next = nullptr;
              this->durableAddressPrefix = -1;
              this->durableAddressPostfix = -1;
          }

          bool isNextMarked(void) {
              return ((std::uintptr_t) this->next) & 1;
          }

          Node* getNextRef(void) {
              return (Node*) (((std::uintptr_t) this->next) & ~1);
          }

          Node* mark(void) {
              return (Node*) (((std::uintptr_t) this) | 1);
          }

          void flipV1(void) {
              this->validBits = this->validBits | 1;
          }

          void makeValid(void) {
              this->validBits = this->validBits | 2;
          }

          // FLUSH
          void FLUSH_INSERT(MemoryManager<T>* mem) {
              mem->FLUSH(this->key,  // This call is always the same for a given node
                         this->item,
                         this->validBits,
                         true,   // insertValidFlag  // Memory Manager expects a bool
                         false,  // deleteValidFlag  // Memory Manager expects a bool
                         (std::uintptr_t) this->next,
                         this->durableAddressPrefix,
                         this->durableAddressPostfix);
          }

          // FLUSH
          void FLUSH_DELETE(MemoryManager<T>* mem) {
              mem->FLUSH(this->key,  // This call is always the same for a given node
                         this->item,
                         this->validBits,
                         true,  // insertValidFlag  // Memory Manager expects a bool
                         true,  // deleteValidFlag  // Memory Manager expects a bool
                         (std::uintptr_t) this->next,
                         this->durableAddressPrefix,
                         this->durableAddressPostfix);
          }

      }; // aligned(cache line size);

  private:

      Node* head;
      Node* tail;

      // These are for the simulation only
      MemoryManager<T>* mem;
      std::atomic<bool>* abortFlag;
      std::vector<std::vector<Node*>> preAllocatedNodes;
      std::vector<int> allocIndices;  // Points current free node for insert
      std::vector<int> maxIndices;
      std::vector<long> keysVolatileRecovered;
      std::vector<long> keysDurableRecovered;
      int numIDs;

      // Gets memory address from permanent storage and ties it with prealloc node
      Node* allocFromArea(int id) {
          Node* newNode = this->preAllocatedNodes.at(id).at(this->allocIndices.at(id));
          // Retrieve durable address
          int durAddr = this->mem->retrieveAddress(id);
          if (durAddr == -1) {
              return nullptr;
          }
          // set the durableAddress(Pre/Post)fix
          newNode->durableAddressPrefix = id;
          newNode->durableAddressPostfix = durAddr;
          return newNode;
      }

      // Insertion was successful move the indices
      void updateAlloc(int id) {
          this->allocIndices.at(id) -= 1;
          this->mem->updateAddress(id);
      }

      // Common function to traverse the linked list
      Node* find(Node** curr, long key) {
          Node* previous = this->head;
          Node* current = previous->getNextRef();
          while (true) {

              // Abort Check (For abort testing only)
              // if (this->abortFlag->load() == true) return nullptr;

              if (current->key >= key) break;
              previous = current;
              current = previous->getNextRef();

          }
          *curr = current;
          return previous;
      }

  public:

      // Constructor
      // Will not be called concurrently
      LockDurableSet(MemoryManager<T>* mem, std::atomic<bool>* abortFlag, int numIDs, std::vector<int>* writeOpsVector) {
          this->allocIndices = std::vector<int>(numIDs);
          this->maxIndices = std::vector<int>(numIDs);
          this->preAllocatedNodes = std::vector<std::vector<Node*>>(numIDs);
          for (int i = 0; i < numIDs; i++) {
              this->allocIndices.at(i) = writeOpsVector->at(i) - 1;
              this->maxIndices.at(i) = writeOpsVector->at(i);
          }
          for (int i = 0; i < numIDs; i++) {
              this->preAllocatedNodes.at(i) = std::vector<Node*>(writeOpsVector->at(i));
              for (int j = 0; j < writeOpsVector->at(i); j++) {
                  this->preAllocatedNodes.at(i).at(j) = new Node();
              }
          }
          this->numIDs = numIDs;
          this->head = new Node();
          this->tail = new Node();
          this->head->next = this->tail;
          this->head->key = MIN_KEY;  // Make sure keys are not less than
          this->tail->key = MAX_KEY;  // Make sure keys are not greater than
          this->abortFlag = abortFlag;
          this->mem = mem;
          this->keysVolatileRecovered = std::vector<long>();
          this->keysDurableRecovered = std::vector<long>();
      }

      // Free the durable sets preallocated nodes
      void FREE() {
          delete this->head;
          delete this->tail;
          for (int i = 0; i < this->numIDs; i++) {
              for (int j = 0; j < this->maxIndices.at(i); j++) {
                  delete this->preAllocatedNodes.at(i).at(j);
              }
          }
      }

      // Inserts a key at a designated spot in the list
      // Between previous and current
      bool insert(long key, T item, int id) {
          Node *previous = nullptr;
          Node *current = nullptr;
          while (true) {
              previous = this->find(&current, key);

              previous->mtx.lock();   // Lock previous
              current->mtx.lock();    // Lock current

              // Validate the nodes are still valid
              if (previous->next != current || current->isNextMarked()) {
                  previous->mtx.unlock();   // Unlock previous
                  current->mtx.unlock();    // Unlock current
                  continue;
              }
              // Already present
              if (current->key == key) {
                  previous->mtx.unlock();   // Unlock previous
                  current->mtx.unlock();    // Unlock current
                  return false;
              }
              // Insert
              Node* newNode = this->allocFromArea(id);
              if (newNode == nullptr) {
                  previous->mtx.unlock();   // Unlock previous
                  current->mtx.unlock();    // Unlock current
                  return false; // No memory available
              }
              newNode->flipV1();
              newNode->key = key;
              newNode->item = item;
              newNode->next = current;
              previous->next = newNode;
              this->updateAlloc(id);
              newNode->makeValid();

              // Abort Check (For abort testing only)
              // if (this->abortFlag->load() == true) return true;

              newNode->FLUSH_INSERT(this->mem);

              previous->mtx.unlock();   // Unlock previous
              current->mtx.unlock();    // Unlock current

              break;
          }
          return true;
      }

      // Searched for key
      bool contains(long key) {
          Node* current = this->head->getNextRef();
          while (current->key < key)
              current = current->getNextRef();
          if (current->key != key || current->isNextMarked()) return false;
          return true;
      }

      // Finds the node with the key
      // Remove current between previous and successor
      bool remove(long key, int id) {
          Node *previous = nullptr;
          Node *current = nullptr;
          Node* successor = nullptr;
          while (true) {
              previous = find(&current, key);

              previous->mtx.lock();   // Lock previous
              current->mtx.lock();    // Lock current

              // Validate the nodes are still valid
              if (previous->next != current || current->isNextMarked()) {
                  previous->mtx.unlock();   // Unlock previous
                  current->mtx.unlock();    // Unlock current
                  continue;
              }
              // Not present
              if (current->key != key) {
                  previous->mtx.unlock();   // Unlock previous
                  current->mtx.unlock();    // Unlock current
                  return false;
              }
              // Remove
              successor = current->next;
              current->next = successor->mark();
              previous->next = successor;

              // Abort Check (For abort testing only)
              // if (this->abortFlag->load() == true) return true;

              current->FLUSH_DELETE(this->mem);

              previous->mtx.unlock();   // Unlock previous
              current->mtx.unlock();    // Unlock current

              break;
          }
          return true;
      }

      // Deletes all of the nodes
      // Reads and resets memory
      // Re-adds the valid nodes read from memory
      // Will not be called concurrently
      void recover(std::vector<int>* writeOpsVector) {

          // Read Memory Manager
          std::vector<long>* keys = new std::vector<long>();
          std::vector<T>* items = new std::vector<T>();
          std::vector<int>* durableAddressPrefixes = new std::vector<int>();
          std::vector<int>* activeNodes = new std::vector<int>(this->numIDs);
          for (int i = 0; i < this->numIDs; i++) activeNodes->at(i) = 0;
          int numActiveNodes = this->mem.recoverMemory(keys, items, durableAddressPrefixes, activeNodes);

          // Record volatile memory (For testing only)
          this->keysVolatileRecovered = std::vector<long>();
          Node* current = this->head->next.load();
          while (current->next.load() != nullptr) {  // Only tail->next == nullptr
              this->keysVolatileRecovered.push_back(current->key);
              current = current->getNextRef();
          }

          // Record durable memory (For testing only)
          this->keysDurableRecovered = std::vector<long>();
          for (int i = 0; i < numActiveNodes; i++) {
              this->keysDurableRecovered.push_back(keys->at(i));
          }

          // Rejuvenate all of the nodes
          this->FREE();
          this->head = new Node();
          this->tail = new Node();
          this->head->next = this->tail;
          this->head->key = MIN_KEY;  // Make sure keys are not less than
          this->tail->key = MAX_KEY;  // Make sure keys are not greater than
          for (int i = 0; i < numIDs; i++) {
              this->allocIndices.at(i) = writeOpsVector->at(i) + activeNodes->at(i) - 1;
              this->maxIndices.at(i) = writeOpsVector->at(i) + activeNodes->at(i);
          }
          int numNodes = 0;
          for (int i = 0; i < this->numIDs; i++) {
              numNodes = writeOpsVector->at(i) + activeNodes->at(i);
              this->preAllocatedNodes.at(i) = std::vector<Node*>(numNodes);
              for (int j = 0; j < numNodes; j++) {
                  this->preAllocatedNodes.at(i).at(j) = new Node();
              }
          }

          // String the nodes together
          for (int i = 0; i < numActiveNodes; i++) {
              this->insert(keys->at(i), items->at(i), durableAddressPrefixes->at(i));
          }

          delete keys;
          delete items;
          delete durableAddressPrefixes;
          delete activeNodes;

      }

      // For testing (not run concurrentlly)
      void printSet(void) {
          Node* previous = this->head;
          std::cout << "Set keys" << std::endl;
          std::cout << "key: " << previous->key << std::endl;
          Node* current = previous->next;
          while (current != nullptr) {
              std::cout << "key: " << current->key << std::endl;
              previous = current;
              current = current->next;
          }
      }

      // For testing (not run concurrentlly)
      void printSetSize(void) {
          int count = 0;
          Node* previous = this->head;
          Node* current = previous->next;
          while (current != nullptr) {
              count += 1;
              previous = current;
              current = current->next;
          }
          count -= 1;  // Adjust for counting the tail node
          std::cout << "Set size: " << count << std::endl;
      }

      // For testing (not run concurrentlly) (For abort testing only)
      void printRecovery(void) {

          // Print the keys recovered from volatile memory
          std::cout << "Volatile Set keys" << std::endl;
          int numVolatileRecovered = this->keysVolatileRecovered.size();
          for (int i = 0; i < numVolatileRecovered; i++)
              std::cout << "Key: " << this->keysVolatileRecovered.at(i) << std::endl;

          // Print the keys recovered from durable memory
          std::cout << "Durable Set keys" << std::endl;
          int numDurableRecovered = this->keysDurableRecovered.size();
          for (int i = 0; i < numDurableRecovered; i++)
              std::cout << "Key: " << this->keysDurableRecovered.at(i) << std::endl;

      }

};

#endif
