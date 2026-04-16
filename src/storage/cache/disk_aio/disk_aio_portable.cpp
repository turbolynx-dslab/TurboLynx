#include "storage/cache/disk_aio/disk_aio_factory.hpp"

#include "storage/cache/platform_io.hpp"

#include <algorithm>
#include <thread>

#ifdef TURBOLYNX_PORTABLE_DISK_IO

using namespace diskaio;

namespace {

constexpr int DEFAULT_MAX_ONGOING = PER_THREAD_MAXIMUM_ONGOING_DISK_AIO * 2;

int ClampMaxOngoing(int max_num_ongoing) {
	return max_num_ongoing > 0 ? max_num_ongoing : DEFAULT_MAX_ONGOING;
}

bool ExecuteSyncIo(int fd, DiskAioRequest *req, int io_type) {
	if (io_type == DISK_AIO_READ) {
		ssize_t nread = turbolynx::platform_io::PReadAll(fd, req->cb.u.c.buf, req->cb.u.c.nbytes, req->cb.u.c.offset);
		return nread == static_cast<ssize_t>(req->cb.u.c.nbytes);
	}
	ssize_t nwritten = turbolynx::platform_io::PWriteAll(fd, req->cb.u.c.buf, req->cb.u.c.nbytes, req->cb.u.c.offset);
	return nwritten == static_cast<ssize_t>(req->cb.u.c.nbytes);
}

void RunCallbackIfPresent(DiskAioInterface *interface, DiskAioRequest *req) {
	a_callback_t func = reinterpret_cast<a_callback_t>(req->GetFunc());
	if (!func && interface->func_) {
		func = reinterpret_cast<a_callback_t>(interface->func_);
	}
	if (func) {
		func(req);
	}
}

} // namespace

DiskAioFactory *DiskAioFactory::ptr = nullptr;

DiskAioFactory::DiskAioFactory(int &res, int, int) {
	assert(ptr == nullptr);
	daio = new diskaio::DiskAio(0, 0);
	DiskAioFactory::ptr = this;
	res = true;
}

DiskAioFactory::~DiskAioFactory() {
	for (int i = 0; i < MAX_NUM_PER_THREAD_DATASTRUCTURE; ++i) {
		if (per_thread_aio_interface.view(i) != nullptr) {
			diskaio::DiskAioInterface *&interface = per_thread_aio_interface.get(i);
			delete interface;
			interface = nullptr;
		}
	}
	DiskAioFactory::ptr = nullptr;
	delete daio;
}

int DiskAioFactory::OpenAioFile(const char *file_path, int flag) {
	int direct_flag = turbolynx::platform_io::ApplyDirectIOFlag(0);
	bool direct_io = (flag & direct_flag) != 0;
	if (direct_io) {
		flag &= ~direct_flag;
	}
	int fd = turbolynx::platform_io::OpenFile(file_path, flag, S_IRWXU | S_IRWXG | S_IRWXO, direct_io);
	if (fd < 0) {
		perror("OpenAioFile");
	}
	return fd;
}

void DiskAioFactory::RemoveAioFile(int) {
}

void DiskAioFactory::CloseAioFile(int file_id, bool rm) {
	assert(file_id >= 0);
	daio->CloseAioFile(file_id);
	if (rm) {
		RemoveAioFile(file_id);
	}
}

std::size_t DiskAioFactory::GetAioFileSize(int file_id) {
	assert(file_id >= 0);
	struct stat st;
	if (fstat(file_id, &st) != 0) {
		return 0;
	}
	return static_cast<std::size_t>(st.st_size);
}

int DiskAioFactory::Getfd(int file_id) {
	assert(file_id >= 0);
	return daio->Getfd(file_id);
}

diskaio::DiskAioInterface *DiskAioFactory::CreateAioInterface(int max_num_ongoing, int tid) {
	return daio->CreateAioInterface(ClampMaxOngoing(max_num_ongoing), tid);
}

void DiskAioFactory::CreateAioInterfaces(int max_num_ongoing) {
	int slots = std::max<int64_t>(1, std::min<int64_t>(DiskAioParameters::NUM_THREADS, MAX_NUM_PER_THREAD_DATASTRUCTURE));
	for (int i = 0; i < slots; ++i) {
		if (per_thread_aio_interface.view(i) == nullptr) {
			per_thread_aio_interface.get(i) = daio->CreateAioInterface(ClampMaxOngoing(max_num_ongoing), 0);
		}
	}
}

diskaio::DiskAioInterface *DiskAioFactory::GetAioInterface(int tid) {
	int slot = tid >= 0 ? tid : 0;
	slot = std::min(slot, MAX_NUM_PER_THREAD_DATASTRUCTURE - 1);
	if (per_thread_aio_interface.view(slot) == nullptr) {
		per_thread_aio_interface.get(slot) = daio->CreateAioInterface(DEFAULT_MAX_ONGOING, 0);
	}
	return per_thread_aio_interface.get(slot);
}

int DiskAioFactory::ARead(AioRequest &req, diskaio::DiskAioInterface *my_io) {
	if (!my_io) {
		my_io = GetAioInterface(0);
	}
	return my_io->Request(req.user_info.file_id, req.start_pos, req.io_size, req.buf, DISK_AIO_READ, req.user_info);
}

int DiskAioFactory::AWrite(AioRequest &req, diskaio::DiskAioInterface *my_io) {
	if (!my_io) {
		my_io = GetAioInterface(0);
	}
	return my_io->Request(req.user_info.file_id, req.start_pos, req.io_size, req.buf, DISK_AIO_WRITE, req.user_info);
}

int DiskAioFactory::AAppend(AioRequest &req, diskaio::DiskAioInterface *my_io) {
	if (!my_io) {
		my_io = GetAioInterface(0);
	}
	return my_io->Request(req.user_info.file_id, req.start_pos, req.io_size, req.buf, DISK_AIO_APPEND, req.user_info);
}

int DiskAioFactory::WaitForAllResponses(diskaio::DiskAioInterface **my_io) {
	if (!my_io) {
		return 0;
	}
	int completed = 0;
	for (auto io_mode : {READ_IO, WRITE_IO}) {
		if (my_io[io_mode]) {
			completed += my_io[io_mode]->WaitForResponses();
		}
	}
	return completed;
}

int DiskAioFactory::GetNumOngoing(diskaio::DiskAioInterface **my_io) {
	if (!my_io) {
		return 0;
	}
	int num_ongoing = 0;
	for (auto io_mode : {READ_IO, WRITE_IO}) {
		if (my_io[io_mode]) {
			num_ongoing += my_io[io_mode]->GetNumOngoing();
		}
	}
	return num_ongoing;
}

diskaio::DiskAioStats DiskAioFactory::GetStats() {
	return {};
}

void DiskAioFactory::ResetStats() {
}

namespace diskaio {

void DiskAioThread::RegisterInterface(void *) {
}

void DiskAioInterface::Register(DiskAioThread *) {
	disk_aio_thread_ = nullptr;
}

int DiskAioInterface::ProcessResponses() {
	return 0;
}

DiskAioRequest *DiskAioInterface::PackRequest(int fd, off_t offset, ssize_t iosize, char *buf, int io_type) {
	DiskAioRequest *req = nullptr;
	if (request_allocator_.fetch(&req, 1) == 0) {
		return nullptr;
	}
	if (io_type == DISK_AIO_READ) {
		io_prep_pread(&req->cb, fd, buf, iosize, offset);
	} else {
		io_prep_pwrite(&req->cb, fd, buf, iosize, offset);
	}
	req->cb.data = this;
	return req;
}

bool DiskAioInterface::Request(int fid, off_t offset, ssize_t iosize, char *buf, int io_type, void *func) {
	DiskAioRequestUserInfo user_info;
	user_info.file_id = fid;
	user_info.func = func;
	return Request(fid, offset, iosize, buf, io_type, user_info);
}

bool DiskAioInterface::Request(int fid, off_t offset, ssize_t iosize, char *buf, int io_type,
                               DiskAioRequestUserInfo &user_info) {
	while (request_allocator_.num_entries() == 0) {
		std::this_thread::yield();
	}
	DiskAioRequest *req = PackRequest(fid, offset, iosize, buf, io_type);
	if (!req) {
		return false;
	}
	req->SetUserInfo(user_info);
	num_ongoing_++;
	if (req->user_info.do_user_only_req_cb) {
		num_write_ongoing_++;
	}

	int fd = static_cast<DiskAio *>(system_)->Getfd(fid);
	bool success = ExecuteSyncIo(fd, req, io_type);
	if (success) {
		RunCallbackIfPresent(this, req);
	}

	request_allocator_.push(&req, 1);
	num_ongoing_--;
	if (req->user_info.do_user_only_req_cb) {
		num_write_ongoing_--;
	}
	return success;
}

int DiskAioInterface::GetNumOngoing() {
	return num_ongoing_;
}

int DiskAioInterface::WaitForResponses() {
	return 0;
}

int DiskAioInterface::WaitForResponses(int) {
	return 0;
}

} // namespace diskaio

int DiskAioRequest::Complete() {
	DiskAioInterface *interface = reinterpret_cast<DiskAioInterface *>(cb.data);
	if (!interface) {
		return 1;
	}
	DiskAioRequest *itself = this;
	return interface->complete_queue_.push(&itself, 1);
}

__thread int64_t core_id::my_core_id_ = 0;
int64_t core_id::core_counts_ = 0;

void core_id::set_core_ids(int nTs) {
	core_counts_ = std::max<int64_t>(1, nTs);
	my_core_id_ = 0;
}

#endif
