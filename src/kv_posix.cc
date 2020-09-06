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

#include "kv_posix.h"

extern "C" {
#include "postgres.h"
}


int ShmOpen(const char *name, int oflag, mode_t mode, const char *fun) {
    int fd = shm_open(name, oflag, mode);
    if (fd == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
    return fd;
}

void ShmUnlink(const char *name, const char *fun) {
    if (shm_unlink(name) == -1) {
        ereport(WARNING, (errmsg("%s %s failed", fun, __func__),
                          errhint("maybe no shared memory to unlink")));
    }
}

void *Mmap(void *addr, size_t len, int prot, int flags, int fd, off_t offset,
           const char *fun) {
    void *memptr = mmap(addr, len, prot, flags, fd, offset);
    if (memptr == MAP_FAILED) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
    return memptr;
}

void Munmap(void *addr, size_t len, const char *fun) {
    if (munmap(addr, len) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

void Ftruncate(int fd, off_t length, const char *fun) {
    if (ftruncate(fd, length) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

void Fclose(int fd, const char *fun) {
    if (close(fd) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

void SemInit(volatile sem_t *sem, int pshared, unsigned int value,
             const char *fun) {
    if (sem_init((sem_t*) sem, pshared, value) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

void SemDestroy(volatile sem_t *sem, const char *fun) {
    if (sem_destroy((sem_t*) sem) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

void SemPost(volatile sem_t *sem, const char *fun) {
    if (sem_post((sem_t*) sem) == -1) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
}

int SemWait(volatile sem_t *sem, const char *fun) {
    if (sem_wait((sem_t*) sem) == -1) {
        if (errno == EINTR) {
            return -1;
        }
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
    return 0;
}

int SemTryWait(volatile sem_t *sem, const char *fun) {
    int ret = sem_trywait((sem_t*) sem);
    if (ret == -1 && errno != EAGAIN) {
        ereport(ERROR, (errmsg("%s %s failed", fun, __func__)));
    }
    return ret;
}
