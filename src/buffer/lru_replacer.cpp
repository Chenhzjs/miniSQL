#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : max_size(num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  mtx.lock();
  if (victims.empty()) {
    frame_id = nullptr;
    mtx.unlock();
    return false;
  }
  *frame_id = victims.back();
  lru_hash.erase(*frame_id);
  victims.pop_back();
  mtx.unlock();
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  mtx.lock();
  if(lru_hash.count(frame_id) == 0) {
    mtx.unlock();
    return ;
  }
  victims.erase(lru_hash[frame_id]);
  lru_hash.erase(frame_id);
  mtx.unlock();
  return ;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  mtx.lock();
  if(lru_hash.count(frame_id) != 0) {
    mtx.unlock();
    return ;
  }
  if(victims.size() >= max_size) {
    mtx.unlock();
    return ;
  }
  victims.push_front(frame_id);
  lru_hash.emplace(frame_id, victims.begin());
  mtx.unlock();
  return ;
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return victims.size();
}