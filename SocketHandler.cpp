#include "SocketHandler.h"

#include "CallbackHandler.h"

using namespace boost::asio::ip;

SocketHandler::SocketHandler() {}

SocketHandler::~SocketHandler() {
	if (!sockets_.empty() || ioThreadStarted_) {
		Shutdown();
	}
}

void SocketHandler::Shutdown() {
	{
		std::lock_guard<std::mutex> lock(socketsMutex_);

		for (auto& pair : sockets_) {
			SocketWrapper* sw = pair.second.get();
			switch (sw->socketType) {
				case SM_SocketType_Tcp:
					std::static_pointer_cast<Socket<tcp>>(sw->socket)->Destroy();
					break;
				case SM_SocketType_Udp:
					std::static_pointer_cast<Socket<udp>>(sw->socket)->Destroy();
					break;
			}
		}
		sockets_.clear();
	}

	if (ioThreadStarted_) {
		ioContext_.stop();
		ioThread_->join();
		ioThreadStarted_ = false;
		ioThread_.reset();
		ioWork_.reset();
	}

	callbackHandler.Flush();
}

void SocketHandler::StartProcessing() {
	ioThread_ = std::make_unique<boost::thread>(&SocketHandler::RunIoService, this);
	ioThreadStarted_ = true;
}

void SocketHandler::RunIoService() {
	ioWork_ = std::make_unique<boost::asio::io_context::work>(ioContext_);
	ioContext_.run();
}

template <class SocketType>
std::pair<std::shared_ptr<Socket<SocketType>>, SocketWrapper*> SocketHandler::CreateSocket(SM_SocketType st) {
	std::lock_guard<std::mutex> lock(socketsMutex_);

	auto socket = Socket<SocketType>::Create(ioContext_, st);
	auto wrapper = std::make_unique<SocketWrapper>(socket, st);
	SocketWrapper* rawPtr = wrapper.get();
	socket->wrapper_ = rawPtr;
	sockets_[rawPtr] = std::move(wrapper);

	return {socket, rawPtr};
}

std::pair<std::shared_ptr<void>, SocketWrapper*> SocketHandler::CreateSocketFromAccepted(
	SM_SocketType st, tcp::socket&& acceptedSocket) {
	std::lock_guard<std::mutex> lock(socketsMutex_);

	auto socket = Socket<tcp>::CreateFromAccepted(ioContext_, st, std::move(acceptedSocket));
	auto wrapper = std::make_unique<SocketWrapper>(socket, st);
	SocketWrapper* rawPtr = wrapper.get();
	socket->wrapper_ = rawPtr;
	sockets_[rawPtr] = std::move(wrapper);

	return {socket, rawPtr};
}

void SocketHandler::DestroySocket(SocketWrapper* sw) {
	if (!sw) return;

	std::unique_ptr<SocketWrapper> owned;
	{
		std::lock_guard<std::mutex> lock(socketsMutex_);
		auto it = sockets_.find(sw);
		if (it != sockets_.end()) {
			owned = std::move(it->second);
			sockets_.erase(it);
		}
	}

	if (owned) {
		switch (owned->socketType) {
			case SM_SocketType_Tcp:
				std::static_pointer_cast<Socket<tcp>>(owned->socket)->Destroy();
				break;
			case SM_SocketType_Udp:
				std::static_pointer_cast<Socket<udp>>(owned->socket)->Destroy();
				break;
		}
	}
}

void SocketHandler::SetChildHandle(SocketWrapper* sw, int32_t handle) {
	if (!sw) return;

	switch (sw->socketType) {
		case SM_SocketType_Tcp:
			std::static_pointer_cast<Socket<tcp>>(sw->socket)->smHandle = handle;
			break;
		case SM_SocketType_Udp:
			std::static_pointer_cast<Socket<udp>>(sw->socket)->smHandle = handle;
			break;
	}
}

// Explicit template instantiations
template std::pair<std::shared_ptr<Socket<tcp>>, SocketWrapper*> SocketHandler::CreateSocket<tcp>(SM_SocketType);
template std::pair<std::shared_ptr<Socket<udp>>, SocketWrapper*> SocketHandler::CreateSocket<udp>(SM_SocketType);

SocketHandler socketHandler;
