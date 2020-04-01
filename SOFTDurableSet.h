#ifndef SOFT_DURABLE_SET_H
#define SOFT_DURABLE_SET_H

// Link-Free Durable Set Class

#include <atomic>
#include <vector>
#include <cstdint>
#include "SOFTMemoryManager.h"

long MIN_KEY = -100000;
long MAX_KEY = 100000;

template <typename T>
class SOFTDurableSet {

  public:

      // Similar field to the MemCell in MemoryManager
      struct PNode {

          std::atomic<long> key;
          std::atomic<T> item;
          std::atomic<bool> validStart;
          std::atomic<bool> validEnd;
          std::atomic<bool> deleted;

          // These are for the simulation only
          int durableAddressPrefix;      // Is the threads id
          int durableAddressPostfix;     // Is the element index in the memPool

          PNode (void) {
              this->key.store(0);
              this->item.store((T) 0);
              this->validStart.store(false);
              this->validEnd.store(false);
              this->deleted.store(false);
              this->durableAddressPrefix = -1;
              this->durableAddressPostfix = -1;
          }

          void FLUSH(SOFTMemoryManager<T>* mem) {
              mem->FLUSH(this->key.load(),  // This call is always the same for a given node
                         this->item.load(),
                         this->validStart.load(),
                         this->validEnd.load(),
                         this->deleted.load(),
                         this->durableAddressPrefix,
                         this->durableAddressPostfix);
          }

          void create(long key, T item, SOFTMemoryManager<T>* mem) {
              this->validStart.store(true, std::memory_order_relaxed);
              std::atomic_thread_fence(std::memory_order_release);
              this->key.store(key, std::memory_order_relaxed);
              this->item.store(item, std::memory_order_relaxed);
              this->validEnd.store(true, std::memory_order_release);
              this->FLUSH(mem);
          }

          void destroy(SOFTMemoryManager<T>* mem) {
              this->deleted.store(true, std::memory_order_release);
              this->FLUSH(mem);
          }

      }; // aligned(cache line size);

      struct Node {

          long key;
          T item;
          PNode* PNodePointer;      // bool validity of pnodes is true
          std::atomic<Node*> next;  // Marked for logical delete

          // Constructor
          Node(void) {
              this->key = 0;
              this->item = (T) 0;
              this->PNodePointer = new PNode();  // Each Node has an associated pNode
              this->next.store(nullptr);
          }

      };

  private:

      Node* head;
      Node* tailOne;
      Node* tailTwo;
      int INTEND_TO_INSERT = 0;
      int INSERTED = 1;
      int INTEND_TO_DELETE = 2;
      int DELETED = 3;

      // These are for the simulation only
      SOFTMemoryManager<T>* mem;
      std::atomic<bool>* abortFlag;
      std::vector<std::vector<Node*>> preAllocatedNodes;
      std::vector<int> allocIndices;  // Points current free node for insert
      std::vector<int> maxIndices;
      std::vector<long> keysVolatileRecovered;
      std::vector<long> keysDurableRecovered;
      int numIDs;

      // Gets memory address from permanent storage and ties it with prealloc node
      Node* allocFromArea(long key, T item, int id) {
          Node* newNode = this->preAllocatedNodes.at(id).at(this->allocIndices.at(id));
          // Retrieve durable address
          int durAddr = this->mem->retrieveAddress(id);
          if (durAddr == -1) {
              return nullptr;
          }
          // Set the durableAddress(Pre/Post)fix
          newNode->PNodePointer->durableAddressPrefix = id;
          newNode->PNodePointer->durableAddressPostfix = durAddr;
          // Set the newNode
          newNode->key = key;
          newNode->item = item;
          return newNode;
      }

      // Insertion was successful move the indices
      void updateAlloc(int id) {
          this->allocIndices.at(id) -= 1;
          this->mem->updateAddress(id);
      }

      Node* createRef(Node* node, int state) {
          return (Node*) (((std::uintptr_t) node) | state);
      }

      Node* getRef(Node* node) {
          return (Node*) (((std::uintptr_t) node) & ~3);
      }

      int getState(Node* node) {
          return (int) (((std::uintptr_t) node) & 3);
      }

      // node is assumed to be a valid reference
      bool stateCAS(Node* node, int oldState, int newState) {
          Node* successorReference = this->getRef(node->next.load());
          Node* oldStateReference = this->createRef(successorReference, oldState);
          Node* newStateReference = this->createRef(successorReference, newState);
          return node->next.compare_exchange_strong(oldStateReference, newStateReference);
      }

      // Takes two nodes and removes current
      bool trim(Node* previous, Node* current) {
          int previousState = this->getState(current);
          Node* previousReference = this->getRef(previous);
          Node* currentReference = this->getRef(current);
          Node* successor = currentReference->next.load();
          Node* successorReference = this->getRef(successor);
          return previousReference->next.compare_exchange_strong(current, this->createRef(successorReference, previousState));
      }

      // Common function to traverse the linked list
      // Trims logically deleted nodes that have yet to be removed
      Node* find(Node** curr, long key, int* currentStatePtr) {
          Node* previous = this->head;
          Node* previousReference = this->getRef(previous);
          Node* current = previousReference->next.load();
          Node* currentReference = this->getRef(current);
          int previousState = this->getState(current);
          Node* successor = nullptr;
          Node* successorReference = nullptr;
          int currentState = 0;
          while (true) {
              // Abort Check (For abort testing only)
              // if (this->abortFlag->load() == true) return nullptr;

              successor = currentReference->next.load();
              successorReference = this->getRef(successor);
              currentState = this->getState(successor);
              if (currentState != this->DELETED) {
                  if (currentReference->key >= key) {
                      break;
                  }
                  // Move current forward
                  previous = current;
                  previousReference = currentReference;
                  previousState = currentState;
                  current = previousReference->next.load();;
                  currentReference = this->getRef(current);
              }
              else {
                  this->trim(previous, current);
                  current = previousReference->next.load();
                  currentReference = this->getRef(current);
              }
          }
          *currentStatePtr = currentState;
          *curr = current;
          return previous;
      }

  public:

      // Constructor
      // Will not be called concurrently
      SOFTDurableSet(SOFTMemoryManager<T>* mem, std::atomic<bool>* abortFlag, int numIDs, std::vector<int>* writeOpsVector) {
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
          this->tailOne = new Node();
          this->tailTwo = new Node();
          this->head->key = MIN_KEY;     // Make sure keys are not less than
          this->tailOne->key = MAX_KEY;  // Make sure keys are not greater than
          this->tailTwo->key = MAX_KEY+1;  // Make sure keys are not greater than

          this->tailOne->next.store(this->createRef(this->tailTwo, this->INSERTED));
          this->head->next.store(this->createRef(this->tailOne, this->INSERTED));
          this->abortFlag = abortFlag;
          this->mem = mem;
          this->keysVolatileRecovered = std::vector<long>();
          this->keysDurableRecovered = std::vector<long>();
      }

      // Free the durable sets preallocated nodes
      void FREE() {
          delete this->head;
          delete this->tailOne;
          delete this->tailTwo;
          for (int i = 0; i < this->numIDs; i++) {
              for (int j = 0; j < this->maxIndices.at(i); j++) {
                  delete this->preAllocatedNodes.at(i).at(j);
              }
          }
      }

      // Inserts a key at a designated spot in the list
      bool insert(long key, T item, int id) {
          Node* previous = nullptr;
          Node* previousReference = nullptr;
          Node* current = nullptr;
          Node* currentReference = nullptr;
          Node* resultNode = nullptr;
          int previousState;
          int currentState;
          bool result = false;
          while (true) {
              previous = this->find(&current, key, &currentState);
              previousReference = this->getRef(previous);
              currentReference = this->getRef(current);
              previousState = this->getState(current);

              // Abort Check (For abort testing only)
              // if (this->abortFlag->load() == true) return false;

              if (currentReference->key == key) {
                  if (currentState != this->INTEND_TO_INSERT)
                      return false;
                  resultNode = currentReference;
                  break;
              }
              else {
                  Node* newNode = this->allocFromArea(key, item, id);
                  if (newNode == nullptr) return false; // No memory available
                  newNode->next.store(this->createRef(currentReference, this->INTEND_TO_INSERT), std::memory_order_relaxed);
                  if (!previousReference->next.compare_exchange_strong(current, this->createRef(newNode, previousState)))
                      continue;
                  resultNode = newNode;
                  this->updateAlloc(id);
                  result = true;
                  break;
              }
          }
          // resultNode will always be a valid reference
          resultNode->PNodePointer->create(resultNode->key, resultNode->item, this->mem);
          while (this->getState(resultNode->next.load()) == this->INTEND_TO_INSERT)
              this->stateCAS(resultNode, this->INTEND_TO_INSERT, this->INSERTED);
          return result;
      }

      // Searched for key
      // Doesn't help with trimming logically deleted nodes or flushing
      bool contains(long key) {

          Node* currentReference = this->getRef(this->head->next.load());
          int currentState = 0;
          while (currentReference->key < key)
              currentReference = this->getRef(currentReference->next.load());

          currentState = this->getState(currentReference->next.load());
          if (currentReference->key != key) return false;

          // Abort Check (For abort testing only)
          // if (this->abortFlag->load() == true) return false;

          if (currentState == this->DELETED || currentState == this->INTEND_TO_INSERT) {
              return false;
          }
          return true;

      }

      // Loops until node with key is removed
      bool remove(long key) {
          Node* previous = nullptr;
          Node* previousReference = nullptr;
          Node* current = nullptr;
          Node* currentReference = nullptr;
          int previousState;
          int currentState;
          bool result = false;

          previous = this->find(&current, key, &currentState);
          currentReference = this->getRef(current);

          if (currentReference->key != key) return false;
          if (currentState == this->INTEND_TO_INSERT) return false;

          // Makes INTEND_TO_DELETE result becomes true
          while (!result && this->getState(currentReference->next.load()) == this->INSERTED) {
              result = this->stateCAS(currentReference, this->INSERTED, this->INTEND_TO_DELETE);

              // Abort Check (For abort testing only)
              // if (this->abortFlag->load() == true) return true;
          }

          // Help flush and then flip the state to deleted
          currentReference->PNodePointer->destroy(this->mem);
          while (this->getState(currentReference->next.load()) == this->INTEND_TO_DELETE)
             this->stateCAS(currentReference, this->INTEND_TO_DELETE, this->DELETED);

          if (result) this->trim(previous, current);
          return result;
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
          this->tailOne = new Node();
          this->tailTwo = new Node();
          this->head->key = MIN_KEY;     // Make sure keys are not less than
          this->tailOne->key = MAX_KEY;  // Make sure keys are not greater than
          this->tailTwo->key = MAX_KEY;  // Make sure keys are not greater than
          this->tailOne->next.store(this->createRef(this->tailTwo, this->INSERTED));
          this->head->next.store(this->createRef(this->tailOne, this->INSERTED));
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
      // Element is in the set if its state is INSERTED or INTEND_TO_DELETE
      void printSet(void) {
          Node* successor;
          Node* currentReference = this->getRef(this->head->next.load());
          int currentState;
          std::cout << "Set keys" << std::endl;
          std::cout << "key: " << this->head->key << std::endl;
          while (currentReference != nullptr) {
              // Check current state
              successor = currentReference->next.load();
              currentState = this->getState(successor);
              std::cout << "key: " << currentReference->key
                        << " state: " << currentState << std::endl;
              currentReference = this->getRef(successor);  // Update
          }
      }

      // For testing (not run concurrentlly)
      // Element is in the set if its state is INSERTED or INTEND_TO_DELETE
      void printSetSize(void) {
          int count = 0;
          Node* successor;
          Node* currentReference = this->getRef(this->head->next.load());
          int currentState;
          while (currentReference != nullptr) {
              // Check current state
              successor = currentReference->next.load();
              currentState = this->getState(successor);
              count += 1;
              currentReference = this->getRef(successor);  // Update
          }
          count -= 2;  // Adjust for counting the tail nodes
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
