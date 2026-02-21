#include "Socket.h"

#include <cstdio>
#include <functional>
#include <type_traits>

#include "Callback.h"
#include "CallbackHandler.h"
#include "SocketHandler.h"

using namespace boost::asio::ip;

// --- Private constructors ---

template <class SocketType>
Socket<SocketType>::Socket(boost::asio::io_context& ioc, SM_SocketType st)
	: smSocketType_(st), ioc_(ioc), strand_(ioc) {}

template <class SocketType>
Socket<SocketType>::Socket(boost::asio::io_context& ioc, SM_SocketType st,
                           typename SocketType::socket&& acceptedSocket)
	: smSocketType_(st), ioc_(ioc), strand_(ioc) {
	socket_ = std::make_unique<typename SocketType::socket>(std::move(acceptedSocket));
	open_ = true;
}

// --- Factory methods ---

template <class SocketType>
std::shared_ptr<Socket<SocketType>> Socket<SocketType>::Create(boost::asio::io_context& ioc, SM_SocketType st) {
	return std::shared_ptr<Socket<SocketType>>(new Socket<SocketType>(ioc, st));
}

template <class SocketType>
std::shared_ptr<Socket<SocketType>> Socket<SocketType>::CreateFromAccepted(
	boost::asio::io_context& ioc, SM_SocketType st, typename SocketType::socket&& acceptedSocket) {
	return std::shared_ptr<Socket<SocketType>>(new Socket<SocketType>(ioc, st, std::move(acceptedSocket)));
}

// --- Public interface ---

template <class SocketType>
bool Socket<SocketType>::IsOpen() {
	return open_.load();
}

template <class SocketType>
bool Socket<SocketType>::Bind(const char* hostname, uint16_t port) {
	try {
		if (localEndpoint_) return false;

		char sPort[6];
		snprintf(sPort, sizeof(sPort), "%hu", port);

		typename SocketType::resolver syncResolver(ioc_);
		auto endpointIterator = syncResolver.resolve(
			typename SocketType::resolver::query(SocketType::v4(), hostname, sPort));

		localEndpoint_ = std::make_unique<typename SocketType::endpoint>(endpointIterator->endpoint());
		return true;
	} catch (std::exception&) {
		return false;
	}
}

template <class SocketType>
bool Socket<SocketType>::Connect(const char* hostname, uint16_t port) {
	try {
		char sPort[6];
		snprintf(sPort, sizeof(sPort), "%hu", port);

		if (!socket_) InitializeSocket();

		auto self = this->shared_from_this();
		auto resolver = std::make_shared<typename SocketType::resolver>(ioc_);
		auto timer = std::make_shared<boost::asio::deadline_timer>(ioc_);
		auto timedOut = std::make_shared<std::atomic<bool>>(false);
		timer->expires_from_now(boost::posix_time::seconds(2));

		resolver->async_resolve(
			typename SocketType::resolver::query(SocketType::v4(), hostname, sPort),
			strand_.wrap([self, resolver, timer, timedOut](const boost::system::error_code& ec,
			                                     typename SocketType::resolver::iterator endpoints) {
				timer->cancel();
				if (self->destroyed_) return;

				if (!ec) {
					if (!self->socket_) return;

					boost::asio::async_connect(*self->socket_, endpoints,
						self->strand_.wrap([self, resolver](const boost::system::error_code& ec2,
						                                     typename SocketType::resolver::iterator) {
							if (self->destroyed_) return;

							if (!ec2) {
								if (self->connectCallback) {
									callbackHandler.AddCallback(
										Callback::MakeConnect(self->smHandle, self->connectCallback, self->smCallbackArg));
								}
								self->StartReceive();
							} else if (ec2 != boost::asio::error::operation_aborted) {
								if (self->errorCallback) {
									callbackHandler.AddCallback(
										Callback::MakeError(self->smHandle, self->errorCallback,
											self->smCallbackArg, SM_ErrorType_CONNECT_ERROR, ec2.value()));
								}
							}
						}));
				} else if (ec != boost::asio::error::operation_aborted) {
					if (self->errorCallback) {
						callbackHandler.AddCallback(
							Callback::MakeError(self->smHandle, self->errorCallback,
								self->smCallbackArg, SM_ErrorType_NO_HOST, ec.value()));
					}
				} else if (*timedOut && self->errorCallback) {
					// Timer cancelled the resolver — report as DNS timeout
					callbackHandler.AddCallback(
						Callback::MakeError(self->smHandle, self->errorCallback,
							self->smCallbackArg, SM_ErrorType_NO_HOST, ec.value()));
				}
			}));

		timer->async_wait([resolver, timedOut](const boost::system::error_code& ec) {
			if (!ec) {
				*timedOut = true;
				resolver->cancel();
			}
		});

		return true;
	} catch (std::exception&) {
		return false;
	}
}

template <class SocketType>
bool Socket<SocketType>::Disconnect() {
	// Allow disconnecting either a connected socket or a listening acceptor.
	if (!socket_ && !tcpAcceptor_) return false;

	auto self = this->shared_from_this();
	strand_.post([self]() {
		if (self->socket_) {
			boost::system::error_code ec;
			self->socket_->close(ec);
			self->open_ = false;
		}
		if (self->tcpAcceptor_) {
			boost::system::error_code ec;
			self->tcpAcceptor_->close(ec);
			self->open_ = false;
		}
	});
	return true;
}

// Generic Listen returns false; TCP specialization below
template <class SocketType>
bool Socket<SocketType>::Listen() {
	return false;
}

template <>
bool Socket<tcp>::Listen() {
	try {
		if (!localEndpoint_) return false;

		if (!tcpAcceptor_) {
			tcpAcceptor_ = std::make_unique<tcp::acceptor>(ioc_, *localEndpoint_);
			open_ = true;
			ApplyPendingOptions();
		}

		DoAcceptLoop();
		return true;
	} catch (std::exception&) {
		return false;
	}
}

template <class SocketType>
bool Socket<SocketType>::Send(const std::string& data) {
	try {
		if (!socket_) return false;

		auto buf = std::make_shared<std::vector<char>>(data.begin(), data.end());
		auto self = this->shared_from_this();

		sendQueueLength++;

		strand_.post([self, buf]() {
			self->sendQueue_.push_back(buf);
			if (!self->writing_) {
				self->DoSend();
			}
		});

		return true;
	} catch (std::exception&) {
		return false;
	}
}

template <class SocketType>
void Socket<SocketType>::DoSend() {
	if (destroyed_ || !socket_ || sendQueue_.empty()) return;

	writing_ = true;
	auto buf = sendQueue_.front();
	sendQueue_.pop_front();
	auto self = this->shared_from_this();

	auto handler = strand_.wrap([self, buf](const boost::system::error_code& ec, size_t) {
		if (self->destroyed_) {
			--self->sendQueueLength;
			self->writing_ = false;
			return;
		}

		if (ec == boost::asio::error::operation_aborted) {
			// Socket was closed by Disconnect() or Destroy() while a send was in-flight.
			// Drain the queue silently without reporting an error or firing SendQueueEmpty.
			unsigned int drained = static_cast<unsigned int>(self->sendQueue_.size());
			self->sendQueue_.clear();
			self->sendQueueLength -= (1 + drained);
			self->writing_ = false;
			return;
		}

		if (ec) {
			// Drain remaining queue items so sendQueueLength stays consistent
			unsigned int drained = static_cast<unsigned int>(self->sendQueue_.size());
			self->sendQueue_.clear();
			self->sendQueueLength -= (1 + drained);
			self->writing_ = false;

			if (self->errorCallback) {
				callbackHandler.AddCallback(
					Callback::MakeError(self->smHandle, self->errorCallback,
						self->smCallbackArg, SM_ErrorType_SEND_ERROR, ec.value()));
			}
			return;
		}

		unsigned int remaining = --self->sendQueueLength;

		if (!self->sendQueue_.empty()) {
			self->DoSend();
		} else {
			self->writing_ = false;
			if (remaining == 0 && self->sendqueueEmptyCallback) {
				callbackHandler.AddCallback(
					Callback::MakeSendQueueEmpty(self->smHandle, self->sendqueueEmptyCallback, self->smCallbackArg));
			}
		}
	});

	// async_write is a composed operation that guarantees complete sends (TCP).
	// async_send sends a single datagram atomically (UDP).
	// async_write requires AsyncWriteStream which datagram sockets don't satisfy.
	if constexpr (std::is_same_v<SocketType, tcp>) {
		boost::asio::async_write(*socket_, boost::asio::buffer(*buf), handler);
	} else {
		socket_->async_send(boost::asio::buffer(*buf), handler);
	}
}

// Generic SendTo returns false; UDP specialization below
template <class SocketType>
bool Socket<SocketType>::SendTo(const std::string& data, const char* hostname, uint16_t port) {
	return false;
}

template <>
bool Socket<udp>::SendTo(const std::string& data, const char* hostname, uint16_t port) {
	try {
		char sPort[6];
		snprintf(sPort, sizeof(sPort), "%hu", port);

		if (!socket_) InitializeSocket();

		auto buf = std::make_shared<std::vector<char>>(data.begin(), data.end());
		auto self = this->shared_from_this();
		auto resolver = std::make_shared<udp::resolver>(ioc_);
		auto timer = std::make_shared<boost::asio::deadline_timer>(ioc_);
		auto timedOut = std::make_shared<std::atomic<bool>>(false);
		timer->expires_from_now(boost::posix_time::seconds(2));

		sendQueueLength++;

		resolver->async_resolve(
			udp::resolver::query(udp::v4(), hostname, sPort),
			strand_.wrap([self, resolver, buf, timer, timedOut](const boost::system::error_code& ec,
			                                          udp::resolver::iterator endpoints) {
				timer->cancel();
				if (self->destroyed_) {
					--self->sendQueueLength;
					return;
				}

				if (!ec) {
					auto endpoint = *endpoints;
					if (!self->socket_) {
						--self->sendQueueLength;
						return;
					}

					self->socket_->async_send_to(boost::asio::buffer(*buf), endpoint,
						self->strand_.wrap([self, resolver, buf](const boost::system::error_code& ec2, size_t) {
							unsigned int remaining = --self->sendQueueLength;
							if (self->destroyed_) return;

							if (!ec2) {
								if (remaining == 0 && self->sendqueueEmptyCallback) {
									callbackHandler.AddCallback(
										Callback::MakeSendQueueEmpty(self->smHandle, self->sendqueueEmptyCallback, self->smCallbackArg));
								}
							} else if (ec2 != boost::asio::error::operation_aborted && self->errorCallback) {
								callbackHandler.AddCallback(
									Callback::MakeError(self->smHandle, self->errorCallback,
										self->smCallbackArg, SM_ErrorType_SEND_ERROR, ec2.value()));
							}
						}));
				} else if (ec != boost::asio::error::operation_aborted) {
					--self->sendQueueLength;
					if (self->errorCallback) {
						callbackHandler.AddCallback(
							Callback::MakeError(self->smHandle, self->errorCallback,
								self->smCallbackArg, SM_ErrorType_NO_HOST, ec.value()));
					}
				} else {
					--self->sendQueueLength;
					if (*timedOut && self->errorCallback) {
						callbackHandler.AddCallback(
							Callback::MakeError(self->smHandle, self->errorCallback,
								self->smCallbackArg, SM_ErrorType_NO_HOST, ec.value()));
					}
				}
			}));

		timer->async_wait([resolver, timedOut](const boost::system::error_code& ec) {
			if (!ec) {
				*timedOut = true;
				resolver->cancel();
			}
		});

		return true;
	} catch (std::exception&) {
		return false;
	}
}

template <class SocketType>
void Socket<SocketType>::SetSendqueueEmptyCallback(IPluginFunction* func) {
	// Post to the strand so this is serialized with DoSend. This prevents the race
	// where the game thread reads sendQueueLength==0 while DoSend is simultaneously
	// decrementing it, which could cause the callback to fire twice.
	auto self = this->shared_from_this();
	strand_.post([self, func]() {
		self->sendqueueEmptyCallback = func;
		if (!self->writing_ && self->sendQueueLength == 0 && func) {
			callbackHandler.AddCallback(
				Callback::MakeSendQueueEmpty(self->smHandle, func, self->smCallbackArg));
		}
	});
}

template <class SocketType>
bool Socket<SocketType>::SetOption(SM_SocketOption so, int value) {
	if (socket_ || tcpAcceptor_) {
		auto self = this->shared_from_this();
		strand_.post([self, so, value]() {
			if (self->socket_) {
				self->ApplyOption(so, value, *self->socket_);
			} else if (self->tcpAcceptor_) {
				self->ApplyOption(so, value, *self->tcpAcceptor_);
			}
		});
		return true;
	} else {
		pendingOptions_.push_back({so, value});
		return true;
	}
}

template <class SocketType>
void Socket<SocketType>::Destroy() {
	destroyed_ = true;

	auto self = this->shared_from_this();
	strand_.post([self]() {
		self->sendQueue_.clear();
		self->writing_ = false;
		if (self->socket_) {
			boost::system::error_code ec;
			self->socket_->close(ec);
			self->open_ = false;
		}
		if (self->tcpAcceptor_) {
			boost::system::error_code ec;
			self->tcpAcceptor_->close(ec);
			self->open_ = false;
		}
	});
}

// --- Private helpers ---

template <class SocketType>
void Socket<SocketType>::StartReceive() {
	auto self = this->shared_from_this();
	strand_.dispatch([self]() {
		if (self->destroyed_ || !self->socket_) return;
		auto buf = std::make_shared<std::vector<char>>(16384);
		self->DoReceive(buf);
	});
}

template <class SocketType>
void Socket<SocketType>::DoReceive(std::shared_ptr<std::vector<char>> buf) {
	if (destroyed_ || !socket_) return;

	auto self = this->shared_from_this();
	socket_->async_receive(boost::asio::buffer(*buf),
		strand_.wrap([self, buf](const boost::system::error_code& ec, size_t bytesTransferred) {
			if (self->destroyed_) return;

			if (!ec) {
				if (bytesTransferred && self->receiveCallback) {
					callbackHandler.AddCallback(
						Callback::MakeReceive(self->smHandle, self->receiveCallback,
							self->smCallbackArg, buf->data(), bytesTransferred));
				}
				self->DoReceive(buf);
			} else if (ec == boost::asio::error::eof ||
			           ec == boost::asio::error::connection_reset ||
			           ec == boost::asio::error::connection_aborted) {
				self->open_ = false;
				if (self->disconnectCallback) {
					callbackHandler.AddCallback(
						Callback::MakeDisconnect(self->smHandle, self->disconnectCallback, self->smCallbackArg));
				}
			} else if (ec != boost::asio::error::operation_aborted) {
				if (self->errorCallback) {
					callbackHandler.AddCallback(
						Callback::MakeError(self->smHandle, self->errorCallback,
							self->smCallbackArg, SM_ErrorType_RECV_ERROR, ec.value()));
				}
			}
		}));
}

// Generic DoAcceptLoop/HandleAccept — only meaningful for TCP
template <class SocketType>
void Socket<SocketType>::DoAcceptLoop() {}

template <>
void Socket<tcp>::DoAcceptLoop() {
	if (destroyed_ || !tcpAcceptor_) return;

	auto newSocket = std::make_shared<tcp::socket>(ioc_);
	auto self = this->shared_from_this();

	tcpAcceptor_->async_accept(*newSocket,
		strand_.wrap([self, newSocket](const boost::system::error_code& ec) {
			self->HandleAccept(newSocket, ec);
		}));
}

template <class SocketType>
void Socket<SocketType>::HandleAccept(std::shared_ptr<tcp::socket>, const boost::system::error_code&) {}

template <>
void Socket<tcp>::HandleAccept(std::shared_ptr<tcp::socket> newAsioSocket,
                               const boost::system::error_code& ec) {
	if (destroyed_) return;

	if (!ec && tcpAcceptor_) {
		try {
			std::string remoteIP = newAsioSocket->remote_endpoint().address().to_string();
			uint16_t remotePort = newAsioSocket->remote_endpoint().port();

			auto childResult = socketHandler.CreateSocketFromAccepted(smSocketType_, std::move(*newAsioSocket));
			SocketWrapper* childWrapper = childResult.second;

			IPluginFunction* cb = incomingCallback.load();
			if (cb) {
				callbackHandler.AddCallback(
					Callback::MakeIncoming(smHandle, cb, smCallbackArg,
						childWrapper, remoteIP, remotePort));
			} else {
				// No incoming callback — destroy the child socket immediately to prevent a leak.
				// It has no SM handle and cannot be closed by the plugin.
				socketHandler.DestroySocket(childWrapper);
			}
		} catch (std::exception&) {
			// remote_endpoint() or socket creation failed — skip this connection
		}

		// Continue accepting regardless of whether this connection succeeded
		DoAcceptLoop();
	} else if (ec && ec != boost::asio::error::operation_aborted) {
		if (errorCallback) {
			callbackHandler.AddCallback(
				Callback::MakeError(smHandle, errorCallback,
					smCallbackArg, SM_ErrorType_LISTEN_ERROR, ec.value()));
		}
		// Continue accepting — transient errors shouldn't kill the listener
		DoAcceptLoop();
	}
}

template <class SocketType>
void Socket<SocketType>::InitializeSocket() {
	if (socket_) return;

	if (localEndpoint_) {
		socket_ = std::make_unique<typename SocketType::socket>(ioc_, *localEndpoint_);
	} else {
		socket_ = std::make_unique<typename SocketType::socket>(ioc_);
	}

	if (!socket_->is_open()) socket_->open(SocketType::v4());
	open_ = true;
	ApplyPendingOptions();
}

template <class SocketType>
void Socket<SocketType>::ApplyPendingOptions() {
	for (auto& opt : pendingOptions_) {
		if (socket_) {
			ApplyOption(opt.first, opt.second, *socket_);
		} else if (tcpAcceptor_) {
			ApplyOption(opt.first, opt.second, *tcpAcceptor_);
		}
	}
	pendingOptions_.clear();
}

template <class SocketType>
template <typename Settable>
bool Socket<SocketType>::ApplyOption(SM_SocketOption so, int value, Settable& target) {
	try {
		switch (so) {
			case SM_SO_SocketBroadcast:
				target.set_option(boost::asio::socket_base::broadcast(value != 0));
				break;
			case SM_SO_SocketReuseAddr:
				target.set_option(boost::asio::socket_base::reuse_address(value != 0));
				break;
			case SM_SO_SocketKeepAlive:
				target.set_option(boost::asio::socket_base::keep_alive(value != 0));
				break;
			case SM_SO_SocketLinger:
				target.set_option(boost::asio::socket_base::linger(value > 0, value));
				break;
			case SM_SO_SocketOOBInline:
				return false;
			case SM_SO_SocketSendBuffer:
				target.set_option(boost::asio::socket_base::send_buffer_size(value));
				break;
			case SM_SO_SocketReceiveBuffer:
				target.set_option(boost::asio::socket_base::receive_buffer_size(value));
				break;
			case SM_SO_SocketDontRoute:
				target.set_option(boost::asio::socket_base::do_not_route(value != 0));
				break;
			case SM_SO_SocketReceiveLowWatermark:
				target.set_option(boost::asio::socket_base::receive_low_watermark(value));
				break;
			case SM_SO_SocketReceiveTimeout:
				return false;
			case SM_SO_SocketSendLowWatermark:
				target.set_option(boost::asio::socket_base::send_low_watermark(value));
				break;
			case SM_SO_SocketSendTimeout:
				return false;
			default:
				return false;
		}
		return true;
	} catch (std::exception&) {
		return false;
	}
}

// --- Explicit template instantiations ---

template class Socket<tcp>;
template class Socket<udp>;
