/* Copyright 2019 VidarDB Inc.
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

#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>
#include <semaphore.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Pthread
 */
extern void PthreadCreate(pthread_t *__restrict __newthread,
                          const pthread_attr_t *__restrict __attr,
                          void *(*__start_routine)(void *),
                          void *__restrict __arg,
                          const char *fun);

extern void PthreadJoin(pthread_t __th, void **__thread_return, const char *fun);

extern void PthreadCancel(pthread_t __th, const char *fun);

extern void PthreadSetCancelState(int __state, int *__oldstate, const char *fun);

extern void PthreadSetCancelType(int __type, int *__oldtype, const char *fun);

/*
 * SharedMemory
 */
extern int ShmOpen(const char *__name,
                   int __oflag,
                   mode_t __mode,
                   const char *fun);

extern void ShmUnlink(const char *__name, const char *fun);

/*
 * MemoryMapped
 */
extern void *Mmap(void *__addr,
                  size_t __len,
                  int __prot,
                  int __flags,
                  int __fd,
                  off_t __offset,
                  const char *fun);

extern void Munmap(void *__addr, size_t __len, const char *fun);

/*
 * File OPs
 */
extern void Ftruncate(int __fd, off_t __length, const char *fun);

extern void Fclose(int __fd, const char *fun);

/*
 * Semaphore
 */
extern void SemInit(volatile sem_t *__sem,
                    int __pshared,
                    unsigned int __value,
                    const char *fun);

extern void SemDestroy(volatile sem_t *__sem, const char *fun);

extern void SemPost(volatile sem_t *__sem, const char *fun);

extern int SemWait(volatile sem_t *__sem, const char *fun);

extern int SemTryWait(volatile sem_t *__sem, const char *fun);

#ifdef __cplusplus
}
#endif

#endif
