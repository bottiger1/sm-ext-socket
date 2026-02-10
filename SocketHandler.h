#ifndef INC_SEXT_SOCKETHANDLER_H
#define INC_SEXT_SOCKETHANDLER_H

#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include <boost/asio.hpp>
#include <boost/thread.hpp>

#include "Socket.h"

struct SocketWrapper {
	SocketWrapper(std::shared_ptr<void> socket, SM_SocketType socketType)
		: socket(std::move(socket)), socketType(socketType) {}

	std::shared_ptr<void> socket;
	SM_SocketType socketType;
};

class SocketHandler {
public:
	SocketHandler();
	~SocketHandler();

	void Shutdown();
	void StartProcessing();

	template <class SocketType>
	std::pair<std::shared_ptr<Socket<SocketType>>, SocketWrapper*> CreateSocket(SM_SocketType st);

	std::pair<std::shared_ptr<void>, SocketWrapper*> CreateSocketFromAccepted(
		SM_SocketType st, boost::asio::ip::tcp::socket&& acceptedSocket);

	void DestroySocket(SocketWrapper* sw);
	void SetChildHandle(SocketWrapper* sw, int32_t handle);

	boost::asio::io_context& GetIoContext() { return ioContext_; }

private:
	void RunIoService();

	boost::asio::io_context ioContext_;
	std::unique_ptr<boost::asio::io_context::work> ioWork_;

	std::unordered_map<SocketWrapper*, std::unique_ptr<SocketWrapper>> sockets_;
	std::mutex socketsMutex_;

	std::unique_ptr<boost::thread> ioThread_;
	bool ioThreadStarted_ = false;
};

extern SocketHandler socketHandler;

#endif
