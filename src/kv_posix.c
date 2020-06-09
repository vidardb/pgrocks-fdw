
#include "kv_posix.h"

#include "postgres.h"


void PthreadCreate(pthread_t *__restrict __newthread,
                   const pthread_attr_t *__restrict __attr,
                   void *(*__start_routine)(void *),
                   void *__restrict __arg,
                   const char *fun) {
    int status = pthread_create(__newthread, __attr, __start_routine, __arg);
    if (status != 0) {
        ereport(ERROR, (errmsg("%s %s error number: %d", fun, __func__, status)));
    }
}

void PthreadJoin(pthread_t __th, void **__thread_return, const char *fun) {
    int status = pthread_join(__th, __thread_return);
    if (status != 0) {
        ereport(ERROR, (errmsg("%s %s error number: %d", fun, __func__, status)));
    }
}

void PthreadCancel(pthread_t __th, const char *fun) {
    int status = pthread_cancel(__th);
    if (status != 0) {
        ereport(ERROR, (errmsg("%s %s error number: %d", fun, __func__, status)));
    }
}

void PthreadSetCancelState(int __state, int *__oldstate, const char *fun) {
    int status = pthread_setcancelstate(__state, __oldstate);
    if (status != 0) {
        ereport(ERROR, (errmsg("%s %s error number: %d", fun, __func__, status)));
    }
}

void PthreadSetCancelType(int __type, int *__oldtype, const char *fun) {
    int status = pthread_setcanceltype(__type, __oldtype);
    if (status != 0) {
        ereport(ERROR, (errmsg("%s %s error number: %d", fun, __func__, status)));
    }
}

int ShmOpen(const char *__name, int __oflag, mode_t __mode, const char *fun) {
    int fd = shm_open(__name, __oflag, __mode);
    if (fd == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
    return fd;
}

void ShmUnlink(const char *__name, const char *fun) {
    if (shm_unlink(__name) == -1) {
        ereport(WARNING, (errmsg("%s %s failed", fun, __func__),
                          errhint("maybe no shared memory to unlink")));
    }
}

void *Mmap(void *__addr,
           size_t __len,
           int __prot,
           int __flags,
           int __fd,
           __off_t  __offset,
           const char *fun) {
    caddr_t memptr = mmap(__addr, __len, __prot, __flags, __fd, __offset);
    if (memptr == MAP_FAILED) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
    return memptr;
}

void Munmap(void *__addr, size_t __len, const char *fun) {
    if (munmap(__addr, __len) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

void Ftruncate(int __fd, __off_t __length, const char *fun) {
    if (ftruncate(__fd, __length) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

void Fclose(int __fd, const char *fun) {
    if (close(__fd) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

void SemInit(sem_t *__sem, int __pshared, unsigned int __value, const char *fun) {
    if (sem_init(__sem, __pshared, __value) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

void SemDestroy(sem_t *__sem, const char *fun) {
    if (sem_destroy(__sem) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

void SemPost(sem_t *__sem, const char *fun) {
    if (sem_post(__sem) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

int SemWait(sem_t *__sem, const char *fun) {
    if (sem_wait(__sem) == -1) {
        if (errno == EINTR) {
            return -1;
        }
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
    return 0;
}

int SemTryWait(sem_t *__sem, const char *fun) {
    int ret = sem_trywait(__sem);
    if (ret == -1 && errno != EAGAIN) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
    return ret;
}
