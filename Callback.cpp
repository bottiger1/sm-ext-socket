#include "Callback.h"

#include "Extension.h"
#include "SocketHandler.h"

std::unique_ptr<Callback> Callback::MakeConnect(int32_t handle, IPluginFunction* func, int32_t arg) {
	std::unique_ptr<Callback> cb(new Callback());
	cb->event_ = CallbackEvent_Connect;
	cb->smHandle_ = handle;
	cb->function_ = func;
	cb->arg_ = arg;
	return cb;
}

std::unique_ptr<Callback> Callback::MakeDisconnect(int32_t handle, IPluginFunction* func, int32_t arg) {
	std::unique_ptr<Callback> cb(new Callback());
	cb->event_ = CallbackEvent_Disconnect;
	cb->smHandle_ = handle;
	cb->function_ = func;
	cb->arg_ = arg;
	return cb;
}

std::unique_ptr<Callback> Callback::MakeReceive(int32_t handle, IPluginFunction* func, int32_t arg,
                                                const char* data, size_t dataLength) {
	std::unique_ptr<Callback> cb(new Callback());
	cb->event_ = CallbackEvent_Receive;
	cb->smHandle_ = handle;
	cb->function_ = func;
	cb->arg_ = arg;
	cb->receiveData_.assign(data, data + dataLength);
	return cb;
}

std::unique_ptr<Callback> Callback::MakeIncoming(int32_t handle, IPluginFunction* func, int32_t arg,
                                                 SocketWrapper* childSocketWrapper,
                                                 const std::string& remoteIP, uint16_t remotePort) {
	std::unique_ptr<Callback> cb(new Callback());
	cb->event_ = CallbackEvent_Incoming;
	cb->smHandle_ = handle;
	cb->function_ = func;
	cb->arg_ = arg;
	cb->childSocketWrapper_ = childSocketWrapper;
	cb->remoteIP_ = remoteIP;
	cb->remotePort_ = remotePort;
	return cb;
}

std::unique_ptr<Callback> Callback::MakeSendQueueEmpty(int32_t handle, IPluginFunction* func, int32_t arg) {
	std::unique_ptr<Callback> cb(new Callback());
	cb->event_ = CallbackEvent_SendQueueEmpty;
	cb->smHandle_ = handle;
	cb->function_ = func;
	cb->arg_ = arg;
	return cb;
}

std::unique_ptr<Callback> Callback::MakeError(int32_t handle, IPluginFunction* func, int32_t arg,
                                              SM_ErrorType errorType, int errorNumber) {
	std::unique_ptr<Callback> cb(new Callback());
	cb->event_ = CallbackEvent_Error;
	cb->smHandle_ = handle;
	cb->function_ = func;
	cb->arg_ = arg;
	cb->errorType_ = errorType;
	cb->errorNumber_ = errorNumber;
	return cb;
}

void Callback::Execute() {
	if (!function_) return;

	switch (event_) {
		case CallbackEvent_Connect:
			function_->PushCell(smHandle_);
			function_->PushCell(arg_);
			function_->Execute(NULL);
			break;

		case CallbackEvent_Disconnect:
			function_->PushCell(smHandle_);
			function_->PushCell(arg_);
			function_->Execute(NULL);
			break;

		case CallbackEvent_Receive: {
			size_t len = receiveData_.size();
			std::vector<char> tmp(len + 1);
			memcpy(tmp.data(), receiveData_.data(), len);
			tmp[len] = '\0';

			function_->PushCell(smHandle_);
			function_->PushStringEx(tmp.data(), len + 1, SM_PARAM_STRING_COPY | SM_PARAM_STRING_BINARY, 0);
			function_->PushCell(static_cast<cell_t>(len));
			function_->PushCell(arg_);
			function_->Execute(NULL);
			break;
		}

		case CallbackEvent_Incoming: {
			Handle_t childHandle = handlesys->CreateHandle(
				extension.socketHandleType,
				childSocketWrapper_,
				function_->GetParentContext()->GetIdentity(),
				myself->GetIdentity(),
				NULL);

			socketHandler.SetChildHandle(childSocketWrapper_, childHandle);

			function_->PushCell(smHandle_);
			function_->PushCell(childHandle);
			function_->PushString(remoteIP_.c_str());
			function_->PushCell(remotePort_);
			function_->PushCell(arg_);
			function_->Execute(NULL);
			break;
		}

		case CallbackEvent_SendQueueEmpty:
			function_->PushCell(smHandle_);
			function_->PushCell(arg_);
			function_->Execute(NULL);
			break;

		case CallbackEvent_Error:
			function_->PushCell(smHandle_);
			function_->PushCell(errorType_);
			function_->PushCell(errorNumber_);
			function_->PushCell(arg_);
			function_->Execute(NULL);
			break;
	}
}
