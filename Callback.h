#ifndef INC_SEXT_CALLBACK_H
#define INC_SEXT_CALLBACK_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "sdk/smsdk_ext.h"
#include "Define.h"

struct SocketWrapper;

class Callback {
public:
	static std::unique_ptr<Callback> MakeConnect(int32_t handle, IPluginFunction* func, int32_t arg);
	static std::unique_ptr<Callback> MakeDisconnect(int32_t handle, IPluginFunction* func, int32_t arg);
	static std::unique_ptr<Callback> MakeReceive(int32_t handle, IPluginFunction* func, int32_t arg,
	                                             const char* data, size_t dataLength);
	static std::unique_ptr<Callback> MakeIncoming(int32_t handle, IPluginFunction* func, int32_t arg,
	                                              SocketWrapper* childSocketWrapper,
	                                              const std::string& remoteIP, uint16_t remotePort);
	static std::unique_ptr<Callback> MakeSendQueueEmpty(int32_t handle, IPluginFunction* func, int32_t arg);
	static std::unique_ptr<Callback> MakeError(int32_t handle, IPluginFunction* func, int32_t arg,
	                                           SM_ErrorType errorType, int errorNumber);

	void Execute();

private:
	Callback() = default;

	CallbackEvent event_;
	int32_t smHandle_ = 0;
	IPluginFunction* function_ = nullptr;
	int32_t arg_ = 0;

	// Receive data
	std::vector<char> receiveData_;

	// Incoming connection data
	SocketWrapper* childSocketWrapper_ = nullptr;
	std::string remoteIP_;
	uint16_t remotePort_ = 0;

	// Error data
	SM_ErrorType errorType_ = SM_ErrorType_EMPTY_HOST;
	int errorNumber_ = 0;
};

#endif
