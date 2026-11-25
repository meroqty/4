#define _POSIX_C_SOURCE 200809L
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <stdlib.h>

#define INCREMENTS 300000
#define THREADS 2

static volatile unsigned long unsafe_counter = 0;
static unsigned long safe_counter = 0;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static void *without_mutex(void *arg) {
    (void)arg;
    for (unsigned long i = 0; i < INCREMENTS; i++) {
        unsafe_counter++;
        if ((i & 127) == 0) sched_yield();
    }
    return NULL;
}

static void *with_mutex(void *arg) {
    (void)arg;
    for (unsigned long i = 0; i < INCREMENTS; i++) {
        pthread_mutex_lock(&mutex);
        safe_counter++;
        pthread_mutex_unlock(&mutex);
        if ((i & 127) == 0) sched_yield();
    }
    return NULL;
}

static int run(void *(*function)(void *)) {
    pthread_t threads[THREADS];
    int started = 0;
    for (int i = 0; i < THREADS; i++) {
        if (pthread_create(&threads[i], NULL, function, NULL) != 0) break;
        started++;
    }
    for (int i = 0; i < started; i++) pthread_join(threads[i], NULL);
    return started == THREADS ? 0 : 1;
}

int main(void) {
    unsigned long expected = (unsigned long)INCREMENTS * THREADS;
    printf("Expected result: %lu\n", expected);
    for (int attempt = 1; attempt <= 3; attempt++) {
        unsafe_counter = 0;
        if (run(without_mutex)) return 1;
        printf("Without mutex, run %d: %lu, lost: %lu\n",
               attempt, unsafe_counter, expected - unsafe_counter);
    }
    for (int attempt = 1; attempt <= 3; attempt++) {
        safe_counter = 0;
        if (run(with_mutex)) return 1;
        printf("With mutex, run %d: %lu, lost: %lu\n",
               attempt, safe_counter, expected - safe_counter);
    }
    pthread_mutex_destroy(&mutex);
    return 0;
}
