#ifndef SOFT_MEMORY_MANAGER_H
#define SOFT_MEMORY_MANAGER_H

// Memory Management Class

#include <vector>
#include <cstdint>

template <typename T>
class SOFTMemoryManager {

  public:

      // Similar fields to the PNode in SOFTDurableSet.h
      struct MemCell {

          long key;
          T item;
          bool validStart;
          bool validEnd;
          bool deleted ;

          // Constructor
          MemCell(void) {
              this->key = 0;
              this->item = (T) 0;
              this->validStart = false;
              this->validEnd = false;
              this->deleted = false;
          }

          // Used by FLUSH to update the cell
          void COPY(long key,
                    T item,
                    bool validStart,
                    bool validEnd,
                    bool deleted) {
              this->key = key;
              this->item = item;
              this->validStart = validStart;
              this->validEnd = validEnd;
              this->deleted = deleted;
          }

          // Used by readResetMemory to determine the cells that
          // have been successful inserted or removed
          bool isValid(void) {
              if (this->deleted == true)  // Cell was deleted
                  return false;
              if (this->validStart == false || this->validEnd == false)   // Cell incomplete
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
      SOFTMemoryManager(int numIDs, int numOps) {

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
                 bool validStart,
                 bool validEnd,
                 bool deleted,
                 int durableAddressPrefix,
                 int durableAddressPostfix) {
          this->memPool.at(durableAddressPrefix)
                       .at(durableAddressPostfix).COPY(key, item, validStart,
                                                       validEnd, deleted);
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
                  this->memPool.at(i).at(j).COPY(0, (T) 0, false, false, false);
                  this->freeListIndex.at(i) += 1;
              }
          }
          return count;
      }

};

#endif
