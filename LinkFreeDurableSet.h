#ifndef LINK_FREE_DURABLE_SET_H
#define LINK_FREE_DURABLE_SET_H

// Link-Free Durable Set Class

#include <atomic>
#include <vector>
#include <cstdint>
#include "MemoryManager.h"

long MIN_KEY = -100000;
long MAX_KEY = 100000;

template <typename T>
class LinkFreeDurableSet {

  public:

      // Similar field to the node in MemoryManager
      // (except) durableAddress(Pre/Post)fix
      struct Node {

          long key;
          T item;
          std::atomic<int> validBits;         // Used for validiting insert
          std::atomic<bool> insertValidFlag;  // Optimization to reduce the number of FLUSH_INSERT
          std::atomic<bool> deleteValidFlag;  // Optimization to reduce the number of FLUSH_DELETE
          std::atomic<Node*> next;            // Marked for logical delete

          // These are for the simulation only
          int durableAddressPrefix;      // Is the threads id
          int durableAddressPostfix;     // Is the element index in the memPool

          // Constructor
          Node(void) {
              this->key = 0;
              this->item = (T) 0;
              this->validBits.store(0);
              this->insertValidFlag.store(false);
              this->deleteValidFlag.store(false);
              this->next.store(nullptr);
              this->durableAddressPrefix = -1;
              this->durableAddressPostfix = -1;
          }

          bool isNextMarked(void) {
              return (((std::uintptr_t) this->next.load()) & 1);  // Linearization
          }

          Node* getNextRef(void) {
              return (Node*) (((std::uintptr_t) this->next.load()) & ~1);  // Linearization
          }

          Node* mark(void) {
              return (Node*) (((std::uintptr_t) this) | 1);
          }

          void flipV1(void) {
              this->validBits.store((this->validBits.load() | 1), std::memory_order_release);  // Linearization
          }

          void makeValid(void) {
              this->validBits.store((this->validBits.load() | 2), std::memory_order_release);  // Linearization
          }

          void FLUSH_INSERT(MemoryManager<T>* mem) {
              if (this->insertValidFlag.load() == false) {  // Optimzation
                  mem->FLUSH(this->key,  // This call is always the same for a given node
                             this->item,
                             this->validBits.load(),
                             this->insertValidFlag.load(),
                             this->deleteValidFlag.load(),
                             (std::uintptr_t) this->next.load(),
                             this->durableAddressPrefix,
                             this->durableAddressPostfix);
                  this->insertValidFlag.store(true, std::memory_order_release);
              }
          }

          void FLUSH_DELETE(MemoryManager<T>* mem) {
              if (this->deleteValidFlag.load() == false) {  // Optimzation
                  mem->FLUSH(this->key,  // This call is always the same for a given node
                             this->item,
                             this->validBits.load(),
                             this->insertValidFlag.load(),
                             this->deleteValidFlag.load(),
                             (std::uintptr_t) this->next.load(),
                             this->durableAddressPrefix,
                             this->durableAddressPostfix);
                  this->deleteValidFlag.store(true, std::memory_order_release);
              }
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

      // Takes two nodes and removes current
      // Assume current has already been marked as valid
      // Assume current has a marked successor
      // So a new node won't be inserted behind current
      bool trim(Node* previous, Node* current) {
          current->FLUSH_DELETE(this->mem);
          Node *successor = current->getNextRef();
          return previous->next.compare_exchange_strong(current, successor);
      }

      // Common function to traverse the linked list
      // Trims logically deleted nodes that have yet to be removed
      Node* find(Node** curr, long key) {
          Node* previous = this->head;
          Node* current = previous->next.load();
          while (true) {

              // Abort Check (For abort testing only)
              // if (this->abortFlag->load() == true) return nullptr;

              if (!current->isNextMarked()) {      // Make sure not logically deleted
                  if (current->key >= key) break;
                  previous = current;
              } else {                             // Remove the logically deleted node
                  trim(previous, current);
              }
              current = current->getNextRef();
          }
          *curr = current;
          return previous;
      }

  public:

      // Constructor
      // Will not be called concurrently
      LinkFreeDurableSet(MemoryManager<T>* mem, std::atomic<bool>* abortFlag, int numIDs, std::vector<int>* writeOpsVector) {
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
          this->head->next.store(this->tail);
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
      // If key already present help flush
      // Loop until key is added or already found
      bool insert(long key, T item, int id) {
          Node *previous = nullptr;
          Node *current = nullptr;
          while (true) {
              previous = this->find(&current, key);

              // Abort Check (For abort testing only)
              // if (this->abortFlag->load() == true) return false;

              if (current->key == key) {
                  current->makeValid();
                  current->FLUSH_INSERT(this->mem);
                  return false;
              }
              Node* newNode = this->allocFromArea(id);
              if (newNode == nullptr) return false; // No memory available
              newNode->flipV1();
              std::atomic_thread_fence(std::memory_order_release);
              newNode->key = key;
              newNode->item = item;
              newNode->next.store(current, std::memory_order_relaxed);
              if (previous->next.compare_exchange_strong(current, newNode)) {  // Linearization point
                  this->updateAlloc(id);
                  newNode->makeValid();

                  // Abort Check (For abort testing only)
                  // if (this->abortFlag->load() == true) return true;

                  newNode->FLUSH_INSERT(this->mem);
                  return true;
              }
          }
      }

      // Searched for key
      // Skips over logically deleted nodes
      // If key is set for deletion will help remove
      // Always attempts to flush the node if present
      bool contains(long key) {
          Node* current = this->head->next.load();
          while (current->key < key) {
              current = current->getNextRef();
          }
          if (current->key != key) return false;

          // Abort Check (For abort testing only)
          // if (this->abortFlag->load() == true) return false;

          if (current->isNextMarked()) {
              current->FLUSH_DELETE(this->mem);
              return false;
          }
          current->makeValid();
          current->FLUSH_INSERT(this->mem);
          return true;
      }

      // Loops until node with key is removed
      // Finds the node with the key
      // Grabs its successor (marks)
      // validates the node, incase needed
      // CAS with a marked successor node
      bool remove(long key) {
          Node* previous = nullptr;
          Node* current = nullptr;
          bool result = false;
          while (!result) {
              previous = find(&current, key);

              // Abort Check (For abort testing only)
              // if (this->abortFlag->load() == true) return false;

              if (current->key != key) return false;
              Node* successor = current->getNextRef();
              Node* markedSuccessor = successor->mark();
              current->makeValid();
              result = current->next.compare_exchange_strong(successor, markedSuccessor);

              // Abort Check (For abort testing only)
              // if (this->abortFlag->load() == true) return true;

          }
          // current has been validated and logically deleted
          trim(previous, current);
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
              if (!current->isNextMarked())
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
          this->head->next.store(this->tail);
          this->head->key = MIN_KEY;  // Make sure keys are not less than
          this->tail->key = MAX_KEY;  // Make sure keys are not greater than
          for (int i = 0; i < numIDs; i++) {
              this->allocIndices.at(i) = writeOpsVector->at(i) + activeNodes->at(i) - 1;
              this->maxIndices.at(i) = writeOpsVector->at(i) + activeNodes->at(i);
          }
          int numNodes = 0;
          for (int i = 0; i < numIDs; i++) {
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

          return;
      }

      // For testing (not run concurrentlly)
      void printSet(void) {
          Node* previous = this->head;
          std::cout << "Set keys" << std::endl;
          std::cout << "key: " << previous->key << std::endl;
          Node* current = previous->next.load();
          while (current != nullptr) {
              if (!current->isNextMarked()) {      // Make sure not logically deleted (incase)
                  std::cout << "key: " << current->key << std::endl;
                  previous = current;
              } else {                             // Logically deleted nodes should not be found
                  std::cout << "key: A marked node was found" << std::endl;
                  trim(previous, current);
              }
              current = current->getNextRef();
          }
      }

      // For testing (not run concurrentlly)
      void printSetSize(void) {
          int count = 0;
          Node* previous = this->head;
          Node* current = previous->next.load();
          while (current != nullptr) {
              if (!current->isNextMarked()) {      // Make sure not logically deleted (incase)
                  count += 1;
                  previous = current;
              } else {                             // Logically deleted nodes should not be found
                  std::cout << "key: A marked node was found" << std::endl;
                  trim(previous, current);
              }
              current = current->getNextRef();
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
