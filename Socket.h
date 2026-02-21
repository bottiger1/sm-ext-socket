#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <boost/asio.hpp>

#include "sdk/smsdk_ext.h"
#include "Define.h"

class SocketHandler;
struct SocketWrapper;

template <class SocketType>
class Socket : public std::enable_shared_from_this<Socket<SocketType>> {
public:
	static std::shared_ptr<Socket<SocketType>> Create(boost::asio::io_context& ioc, SM_SocketType st);
	static std::shared_ptr<Socket<SocketType>> CreateFromAccepted(boost::asio::io_context& ioc, SM_SocketType st,
	                                                               typename SocketType::socket&& acceptedSocket);

	~Socket() = default;

	bool IsOpen();

	bool Bind(const char* hostname, uint16_t port);
	void SetSendqueueEmptyCallback(IPluginFunction* func);
	bool Connect(const char* hostname, uint16_t port);
	bool Disconnect();
	bool Listen();
	bool Send(const std::string& data);
	bool SendTo(const std::string& data, const char* hostname, uint16_t port);
	bool SetOption(SM_SocketOption so, int value);
	void Destroy();
	void StartReceive();

	// These fields are written from the game thread and read from the IO thread's strand
	// handlers, so they must be atomic to avoid data races.
	std::atomic<IPluginFunction*> connectCallback{nullptr};
	std::atomic<IPluginFunction*> incomingCallback{nullptr};
	std::atomic<IPluginFunction*> receiveCallback{nullptr};
	std::atomic<IPluginFunction*> sendqueueEmptyCallback{nullptr};
	std::atomic<IPluginFunction*> disconnectCallback{nullptr};
	std::atomic<IPluginFunction*> errorCallback{nullptr};

	std::atomic<int32_t> smHandle{0};
	std::atomic<int32_t> smCallbackArg{0};
	std::atomic<unsigned int> sendQueueLength{0};
	// Mirrors socket_->is_open() but safe to read from the game thread without a strand.
	// Written from the game thread (InitializeSocket) or IO thread (close handlers).
	std::atomic<bool> open_{false};

	SocketWrapper* wrapper_ = nullptr;

private:
	Socket(boost::asio::io_context& ioc, SM_SocketType st);
	Socket(boost::asio::io_context& ioc, SM_SocketType st, typename SocketType::socket&& acceptedSocket);

	void DoSend();
	void DoReceive();

	void DoAcceptLoop();
	void HandleAccept(std::shared_ptr<boost::asio::ip::tcp::socket> newAsioSocket,
	                  const boost::system::error_code& ec);

	void InitializeSocket();
	void ApplyPendingOptions();

	template <typename Settable>
	bool ApplyOption(SM_SocketOption so, int value, Settable& target);

	SM_SocketType smSocketType_;
	std::vector<std::pair<SM_SocketOption, int>> pendingOptions_;

	boost::asio::io_context& ioc_;
	boost::asio::io_context::strand strand_;

	std::unique_ptr<typename SocketType::socket> socket_;
	std::unique_ptr<typename SocketType::endpoint> localEndpoint_;
	std::unique_ptr<boost::asio::ip::tcp::acceptor> tcpAcceptor_;

	std::deque<std::vector<char>> sendQueue_;
	std::vector<char> receiveBuffer_;
	bool writing_ = false;

	std::atomic<bool> destroyed_{false};
};
