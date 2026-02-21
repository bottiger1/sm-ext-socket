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
	bool Connect(const char* hostname, uint16_t port);
	bool Disconnect();
	bool Listen();
	bool Send(const std::string& data);
	bool SendTo(const std::string& data, const char* hostname, uint16_t port);
	bool SetOption(SM_SocketOption so, int value);
	void Destroy();
	void StartReceive();

	IPluginFunction* connectCallback = nullptr;
	IPluginFunction* incomingCallback = nullptr;
	IPluginFunction* receiveCallback = nullptr;
	IPluginFunction* sendqueueEmptyCallback = nullptr;
	IPluginFunction* disconnectCallback = nullptr;
	IPluginFunction* errorCallback = nullptr;

	int32_t smHandle = 0;
	int32_t smCallbackArg = 0;
	std::atomic<unsigned int> sendQueueLength{0};

	SocketWrapper* wrapper_ = nullptr;

private:
	Socket(boost::asio::io_context& ioc, SM_SocketType st);
	Socket(boost::asio::io_context& ioc, SM_SocketType st, typename SocketType::socket&& acceptedSocket);

	void DoSend();
	void DoReceive(std::shared_ptr<std::vector<char>> buf);

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

	std::deque<std::shared_ptr<std::vector<char>>> sendQueue_;
	bool writing_ = false;

	std::atomic<bool> destroyed_{false};
};
