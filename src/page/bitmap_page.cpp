#include "page/bitmap_page.h"
#include <cmath>
#include <iostream>
#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  if (page_allocated_ == MAX_CHARS * 8) return false;
  page_offset = next_free_page_;
//  std::cout << IsPageFree(next_free_page_) << std::endl;
  bytes[page_offset / 8] += pow(2, page_offset % 8);
//  std::cout << IsPageFree(next_free_page_) << std::endl;
  page_allocated_ ++;
  if (page_allocated_ == MAX_CHARS * 8) {
    next_free_page_ = -1;
    return true;
  }
  while(IsPageFree(next_free_page_) == false) {
    next_free_page_ ++;
    if (next_free_page_ == MAX_CHARS * 8) next_free_page_ = 0;
  }
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (IsPageFree(page_offset) == false)
    bytes[page_offset / 8] -= pow(2, page_offset % 8);
  else
    return false;
  page_allocated_ --;
  if (next_free_page_ == -1)
  {
    next_free_page_ = page_offset;
  }
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if (IsPageFreeLow(page_offset / 8, page_offset % 8) == true) return true;
  else return false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  if ((bytes[byte_index] >> bit_index) % 2 == 0) return true;
  else return false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;