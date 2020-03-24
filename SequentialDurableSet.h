#ifndef SEQUENTIAL_DURABLE_SET_H
#define SEQUENTIAL_DURABLE_SET_H

// Sequential Durable Set Class

#include <atomic>
#include <vector>
#include <cstdint>
#include "MemoryManager.h"

long MIN_KEY = -100000;
long MAX_KEY = 100000;

template <typename T>
class SequentialDurableSet {

  public:

      // Similar field to the node in MemoryManager
      // (except) durableAddress(Pre/Post)fix
      struct Node {

          long key;
          T item;
          int validBits;         // Used for validiting insert
          Node* next;            // Marked for logical delete

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
      std::vector<Node*> preAllocatedNodes;
      int allocIndices;  // Points current free node for insert
      int maxIndices;
      std::vector<long> keysVolatileRecovered;
      std::vector<long> keysDurableRecovered;
      int sequential;  // Used by Memory Manager, only one thread

      // Gets memory address from permanent storage and ties it with prealloc node
      Node* allocFromArea(void) {
          Node* newNode = this->preAllocatedNodes.at(this->allocIndices);
          // Retrieve durable address
          int durAddr = this->mem->retrieveAddress(this->sequential);
          if (durAddr == -1) {
              return nullptr;
          }
          // set the durableAddress(Pre/Post)fix
          newNode->durableAddressPrefix = this->sequential;
          newNode->durableAddressPostfix = durAddr;
          return newNode;
      }

      // Insertion was successful move the indices
      void updateAlloc(void) {
          this->allocIndices -= 1;
          this->mem->updateAddress(this->sequential);
      }

      // Common function to traverse the linked list
      Node* find(Node** curr, long key) {
          Node* previous = this->head;
          Node* current = previous->next;
          while (true) {

              // Abort Check (For abort testing only)
              // if (this->abortFlag->load() == true) return nullptr;

              if (current->key >= key) break;
              previous = current;
              current = current->next;

          }
          *curr = current;
          return previous;
      }

  public:

      // Constructor
      SequentialDurableSet(MemoryManager<T>* mem, std::atomic<bool>* abortFlag, int maxWriteOps) {
          this->allocIndices = maxWriteOps - 1;
          this->maxIndices = maxWriteOps;
          this->preAllocatedNodes = std::vector<Node*>(maxWriteOps);
          for (int i = 0; i < maxWriteOps; i++) {
              this->preAllocatedNodes.at(i) = new Node();
          }
          this->sequential = 0;  // Used by Memory Manager, only one thread
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
          for (int i = 0; i < this->maxIndices; i++) {
              delete this->preAllocatedNodes.at(i);
          }
      }

      // Inserts a key at a designated spot in the list
      // Between previous and current
      bool insert(long key, T item) {
          Node *previous = nullptr;
          Node *current = nullptr;

          previous = this->find(&current, key);

          // Abort Check (For abort testing only)
          // if (this->abortFlag->load() == true) return false;

          // Already present
          if (current->key == key)
              return false;

          // Insert
          Node* newNode = this->allocFromArea();
          if (newNode == nullptr) return false; // No memory available
          newNode->flipV1();
          newNode->key = key;
          newNode->item = item;
          newNode->next = current;
          previous->next = newNode;
          this->updateAlloc();
          newNode->makeValid();

          // Abort Check (For abort testing only)
          // if (this->abortFlag->load() == true) return true;

          newNode->FLUSH_INSERT(this->mem);
          return true;
      }

      // Searched for key
      bool contains(long key) {
          Node* current = this->head->next;
          while (current->key < key)
              current = current->next;
          if (current->key != key) return false;
          return true;
      }

      // Finds the node with the key then remove
      // Remove current between previous and successor
      bool remove(long key) {
          Node* previous = nullptr;
          Node* current = nullptr;
          Node* successor = nullptr;

          previous = find(&current, key);

          // Abort Check (For abort testing only)
          // if (this->abortFlag->load() == true) return false;

          if (current->key != key) return false;
          successor = current->next;
          current->next = successor->mark();
          previous->next = successor;

          // Abort Check (For abort testing only)
          // if (this->abortFlag->load() == true) return true;

          current->FLUSH_DELETE(this->mem);
          return true;
      }

      // Deletes all of the nodes
      // Reads and resets memory
      // Re-adds the valid nodes read from memory
      void recover(int maxWriteOps) {

          // Read Memory Manager
          std::vector<long>* keys = new std::vector<long>();
          std::vector<T>* items = new std::vector<T>();
          std::vector<int>* durableAddressPrefixes = new std::vector<int>();  // Should all be 0 for each
          std::vector<int>* activeNodes = new std::vector<int>(this->sequential);
          for (int i = 0; i < this->sequential; i++) activeNodes->at(i) = 0;
          int numActiveNodes = this->mem.recoverMemory(keys, items, durableAddressPrefixes, activeNodes);

          // Record volatile memory (For testing only)
          this->keysVolatileRecovered = std::vector<long>();
          Node* current = this->head->next;
          while (current->next != nullptr) {  // Only tail->next == nullptr
              this->keysVolatileRecovered.push_back(current->key);
              current = current->next;
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
          this->allocIndices = maxWriteOps + activeNodes->at(this->sequential) - 1;
          this->maxIndices = maxWriteOps + activeNodes->at(this->sequential);
          this->preAllocatedNodes = std::vector<Node*>(this->maxIndices);
          for (int i = 0; i < this->maxIndices; i++) {
              this->preAllocatedNodes.at(i) = new Node();
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

      // For testing
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

      // For testing
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

      // For testing (For abort testing only)
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
