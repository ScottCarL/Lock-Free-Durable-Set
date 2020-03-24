#ifndef MEMORY_MANAGER_H
#define MEMORY_MANAGER_H

// Memory Management Class

#include <vector>
#include <cstdint>

template <typename T>
class MemoryManager {

  public:

      // Similar fields to the node in MemoryManager
      // (except) durableAddress(Pre/Post)fix
      struct MemCell {

          long key;
          T item;
          int validBits;
          bool insertValidFlag;  // Not needed to be stored
          bool deleteValidFlag;  // Not needed to be stored
          std::uintptr_t next;        // Used to indicate logical delete

          // Constructor
          MemCell(void) {
              this->key = 0;
              this->item = (T) 0;
              this->validBits = 0;
              this->insertValidFlag = false;
              this->deleteValidFlag = false;
              this->next = (std::uintptr_t) nullptr;
          }

          // Used by FLUSH to update the cell
          void COPY(long key,
                    T item,
                    int validBits,
                    bool insertValidFlag,
                    bool deleteValidFlag,
                    std::uintptr_t next) {
              this->key = key;
              this->item = item;
              this->validBits = validBits;
              this->insertValidFlag = insertValidFlag;
              this->deleteValidFlag = deleteValidFlag;
              this->next = next;
          }

          // Used by readResetMemory to determine the cells that
          // have been successful inserted or removed
          bool isValid(void) {
              if ((this->validBits & 3) != 3)  // Cell incomplete or blank
                  return false;
              if ((bool) ((this->next) | 0))   // Cell logically deleted
                  return false;
              return true;
          }

      };

  private:

      int numMemPoolSections;
      int memPoolSectionSize;
      std::vector<std::vector<MemCell>> memPool;
      std::vector<int> freeListIndex;

  public:

      // Constructor
      MemoryManager(int numIDs, int numOps) {

          // Create vectors of size numIDs
          this->memPool = std::vector<std::vector<MemCell>>(numIDs);
          this->freeListIndex = std::vector<int>(numIDs);

          // Allocate the memPool
          for (int i = 0; i < numIDs; i++) {
              this->memPool.at(i) = std::vector<MemCell>(numOps);
              for (int j = 0; j < numOps; j++)
                  this->memPool.at(i).at(j) = MemCell();
          }

          // Set the current index for each thread
          for (int i = 0; i < numIDs; i++)
              this->freeListIndex.at(i) = numOps - 1;

          this->numMemPoolSections = numIDs;
          this->memPoolSectionSize = numOps;

      }

      // Each thread recieves from their own section of cells
      // Once a cell is used it is never reused
      int retrieveAddress(int sectionID) {
          return this->freeListIndex.at(sectionID);
      }

      // On successful insert, update index to next cell
      void updateAddress(int sectionID) {
          this->freeListIndex.at(sectionID) -= 1;
      }

      // Update Memory on both Insert and Remove
      void FLUSH(long key,
                 T item,
                 int validBits,
                 bool insertValidFlag,
                 bool deleteValidFlag,
                 std::uintptr_t next,
                 int durableAddressPrefix,
                 int durableAddressPostfix) {
          this->memPool.at(durableAddressPrefix)
                       .at(durableAddressPostfix).COPY(key, item, validBits,
                                                       insertValidFlag, deleteValidFlag, next);
      }

      // Reads all of the cells of memory and checks if the cell is valid or not
      // If the cell is valid, store it, in any case reset the values of the cell
      // Will not be called concurrently
      int readResetMemory(std::vector<long>* keys,
                          std::vector<T>* items,
                          std::vector<int>* durableAddressPrefixes,
                          std::vector<int>* activeNodes) {
          int count = 0;
          // Scan through memPool sections
          for (int i = 0; i < this->numMemPoolSections; i++) {
              this->freeListIndex.at(i) = 0;
              for (int j = 0; j < this->memPoolSectionSize; i++) {
                  if (this->memPool.at(i).at(j).isValid()) {
                      // Collect the indices of valid nodes
                      keys->push_back(this->memPool.at(i).at(j).key);
                      items->push_back(this>memPool.at(i).at(j).item);
                      durableAddressPrefixes->push_back(i);
                      activeNodes->at(i) += 1;  // Records active cells for a thread
                      count += 1;               // Records active cells for all threads
                  }
                  this->memPool.at(i).at(j).COPY(0, (T) 0, 0, false, false, (std::uintptr_t) nullptr);
                  this->freeListIndex.at(i) += 1;
              }
          }
          return count;
      }

};

#endif
