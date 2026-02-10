#include "CallbackHandler.h"

#include "Callback.h"

void CallbackHandler::AddCallback(std::unique_ptr<Callback> callback) {
	std::lock_guard<std::mutex> lock(mutex_);
	callbackQueue_.push_back(std::move(callback));
}

void CallbackHandler::ExecuteQueuedCallbacks() {
	std::deque<std::unique_ptr<Callback>> local;
	{
		std::lock_guard<std::mutex> lock(mutex_);
		local.swap(callbackQueue_);
	}

	for (auto& cb : local) {
		cb->Execute();
	}
}

void CallbackHandler::Flush() {
	std::lock_guard<std::mutex> lock(mutex_);
	callbackQueue_.clear();
}

CallbackHandler callbackHandler;
