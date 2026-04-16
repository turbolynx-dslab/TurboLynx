#pragma once

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>

#if defined(__APPLE__)
#include <sys/fcntl.h>
#endif

#if defined(TURBOLYNX_WASM) || defined(TURBOLYNX_PORTABLE_DISK_IO) || defined(__APPLE__)
#ifndef O_DIRECT
#define O_DIRECT 0
#endif
#if defined(__APPLE__) || defined(TURBOLYNX_WASM)
using off64_t = off_t;
#endif
#ifndef mmap64
#define mmap64 mmap
#endif
#ifndef lseek64
#define lseek64 lseek
#endif
#ifndef ftruncate64
#define ftruncate64 ftruncate
#endif
#endif

namespace turbolynx::platform_io {

inline int ApplyDirectIOFlag(int flags) {
#if defined(__APPLE__)
	return flags | O_SYNC;
#else
	return flags | O_DIRECT;
#endif
}

inline void ConfigureDirectIO(int fd, bool direct_io) {
#if defined(__APPLE__)
	if (direct_io) {
#ifdef F_NOCACHE
		(void)fcntl(fd, F_NOCACHE, 1);
#endif
	}
#else
	(void)fd;
	(void)direct_io;
#endif
}

inline int OpenFile(const char *path, int flags, mode_t mode, bool direct_io) {
	int open_flags = flags;
	if (direct_io) {
		open_flags = ApplyDirectIOFlag(open_flags);
	}
	int fd = open(path, open_flags, mode);
	if (fd >= 0) {
		ConfigureDirectIO(fd, direct_io);
	}
	return fd;
}

inline int Preallocate(int fd, off_t len) {
#if defined(__APPLE__)
#ifdef F_PREALLOCATE
	fstore_t store = {};
	store.fst_flags = F_ALLOCATECONTIG;
	store.fst_posmode = F_PEOFPOSMODE;
	store.fst_offset = 0;
	store.fst_length = len;
	store.fst_bytesalloc = 0;
	if (fcntl(fd, F_PREALLOCATE, &store) == -1) {
		store.fst_flags = F_ALLOCATEALL;
		if (fcntl(fd, F_PREALLOCATE, &store) == -1) {
			return ftruncate(fd, len);
		}
	}
#endif
	return ftruncate(fd, len);
#elif defined(__linux__) && !defined(TURBOLYNX_PORTABLE_DISK_IO)
	return fallocate(fd, 0, 0, len);
#else
	struct stat st;
	if (fstat(fd, &st) != 0) {
		return -1;
	}
	if (st.st_size >= len) {
		return 0;
	}
	return ftruncate(fd, len);
#endif
}

inline ssize_t PReadAll(int fd, void *buffer, size_t size, off_t offset) {
	auto ptr = static_cast<char *>(buffer);
	size_t total = 0;
	while (total < size) {
		ssize_t nread = pread(fd, ptr + total, size - total, offset + total);
		if (nread == 0) {
			break;
		}
		if (nread < 0) {
			if (errno == EINTR) {
				continue;
			}
			return -1;
		}
		total += static_cast<size_t>(nread);
	}
	return static_cast<ssize_t>(total);
}

inline ssize_t PWriteAll(int fd, const void *buffer, size_t size, off_t offset) {
	auto ptr = static_cast<const char *>(buffer);
	size_t total = 0;
	while (total < size) {
		ssize_t nwritten = pwrite(fd, ptr + total, size - total, offset + total);
		if (nwritten < 0) {
			if (errno == EINTR) {
				continue;
			}
			return -1;
		}
		total += static_cast<size_t>(nwritten);
	}
	return static_cast<ssize_t>(total);
}

} // namespace turbolynx::platform_io
