
#ifndef KV_POSIX_H_
#define KV_POSIX_H_

#include <pthread.h>
#include <sys/mman.h>
#include <unistd.h>
#include <semaphore.h>


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
#ifdef __APPLE__
                  off_t __offset,
#else
                  __off_t __offset,
#endif
                  const char *fun);

extern void Munmap(void *__addr, size_t __len, const char *fun);


/*
 * File OPs
 */
extern void Ftruncate(int __fd,
#ifdef __APPLE__
                      off_t __length,
#else
                      __off_t __length,
#endif
                      const char *fun);

extern void Fclose(int __fd, const char *fun);


/*
 * Semaphore
 */
extern void SemInit(sem_t *__sem,
                    int __pshared,
                    unsigned int __value,
                    const char *fun);

extern void SemDestroy(sem_t *__sem, const char *fun);

extern void SemPost(sem_t *__sem, const char *fun);

extern int SemWait(sem_t *__sem, const char *fun);

extern int SemTryWait(sem_t *__sem, const char *fun);

#endif
