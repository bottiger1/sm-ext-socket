#ifndef INC_SEXT_CALLBACKHANDLER_H
#define INC_SEXT_CALLBACKHANDLER_H

#include <deque>
#include <memory>
#include <mutex>

class Callback;

class CallbackHandler {
public:
	void AddCallback(std::unique_ptr<Callback> callback);
	void ExecuteQueuedCallbacks();
	void Flush();

private:
	std::deque<std::unique_ptr<Callback>> callbackQueue_;
	std::mutex mutex_;
};

extern CallbackHandler callbackHandler;

#endif
