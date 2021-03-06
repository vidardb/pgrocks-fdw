/* Copyright 2019-present VidarDB Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef KV_POSIX_H_
#define KV_POSIX_H_

#include <sys/mman.h>
#include <unistd.h>
#include <semaphore.h>


/*
 * SharedMemory
 */
extern int  ShmOpen(const char* name, int flag, mode_t mode, const char* func);
extern void ShmUnlink(const char* name, const char* func);

/*
 * MemoryMapped
 */
extern void* Mmap(void* addr, size_t len, int prot, int flag, int fd,
                  off_t offset, const char* func);
extern void  Munmap(void* addr, size_t len, const char* func);

/*
 * File OPs
 */
extern void Ftruncate(int fd, off_t length, const char* func);
extern void Fclose(int fd, const char* func);

/*
 * Semaphore
 */
extern void SemInit(volatile sem_t* sem, int pshared, unsigned int value,
                    const char* func);
extern void SemDestroy(volatile sem_t* sem, const char* func);
extern void SemPost(volatile sem_t* sem, const char* func);
extern int  SemWait(volatile sem_t* sem, const char* func);
extern int  SemTryWait(volatile sem_t* sem, const char* func);

#endif  /* KV_POSIX_H_ */
