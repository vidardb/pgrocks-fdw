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

#include "kv_posix.h"

#include "postgres.h"


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

void *Mmap(void *__addr, size_t __len, int __prot, int __flags, int __fd,
           off_t  __offset, const char *fun) {
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

void Ftruncate(int __fd, off_t __length, const char *fun) {
    if (ftruncate(__fd, __length) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

void Fclose(int __fd, const char *fun) {
    if (close(__fd) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

void SemInit(volatile sem_t *__sem, int __pshared, unsigned int __value,
    const char *fun) {
    if (sem_init((sem_t*) __sem, __pshared, __value) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

void SemDestroy(volatile sem_t *__sem, const char *fun) {
    if (sem_destroy((sem_t*) __sem) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

void SemPost(volatile sem_t *__sem, const char *fun) {
    if (sem_post((sem_t*) __sem) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

int SemWait(volatile sem_t *__sem, const char *fun) {
    if (sem_wait((sem_t*) __sem) == -1) {
        if (errno == EINTR) {
            return -1;
        }
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
    return 0;
}

int SemTryWait(volatile sem_t *__sem, const char *fun) {
    int ret = sem_trywait((sem_t*) __sem);
    if (ret == -1 && errno != EAGAIN) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
    return ret;
}
