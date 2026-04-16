// omp.h — stub for builds without OpenMP support
// Provides no-op implementations of common OpenMP functions.
#pragma once

#if defined(TURBOLYNX_WASM) || defined(TURBOLYNX_USE_OMP_STUB)

static inline int omp_get_thread_num() { return 0; }
static inline int omp_get_num_threads() { return 1; }
static inline int omp_get_max_threads() { return 1; }
static inline void omp_set_num_threads(int) {}
static inline int omp_in_parallel() { return 0; }

// Ignore OpenMP pragmas
#define _OPENMP 0

#else
#error "This header should only be included when the OpenMP stub is enabled"
#endif
