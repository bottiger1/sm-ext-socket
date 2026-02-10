# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

SourceMod Socket Extension — a C++ extension for [SourceMod](https://github.com/alliedmodders/sourcemod/) that provides TCP and UDP socket functionality to SourcePawn plugins. Forked from sfPlayer's original. Uses Boost.Asio for async I/O and Boost.Thread for threading.

## Build System

This project uses **AMBuild** (AlliedModders Build system). There is also a legacy `Makefile` for Linux but AMBuild is the primary build method.

### Dependencies
- SourceMod source tree (headers in `public/`, `sourcepawn/include/`, `public/extensions/`)
- Boost (specifically: `boost::asio`, `boost::thread`; built as static x86 libraries)

### Build Commands
```bash
# From the build/ directory:
python ../configure.py --sm-path <SOURCEMOD_PATH> --boost-path <BOOST_PATH> [--enable-optimize 1] [--enable-debug 1]
ambuild
```

### Configure Options
- `--sm-path`: Path to SourceMod source tree
- `--boost-path`: Path to Boost root (expects `stage/x86_64/lib/` for built libraries)
- `--enable-optimize`: Enable optimization (O3 on GCC/Clang, Ox on MSVC)
- `--enable-debug`: Enable debug symbols

### Compiler Configuration
- C++17 standard (GCC/Clang)
- RTTI disabled (`/GR-` on MSVC), exceptions enabled (`/EHsc` on MSVC)
- Static runtime linking (`/MT` on MSVC, `-static-libgcc` on GCC)
- Output: `socket.ext.so` (Linux) or `socket.ext.dll` (Windows)

## Architecture

### Threading Model
- **Main thread (game thread)**: Runs `GameFrame()` hook which calls `CallbackHandler::ExecuteQueuedCallbacks()` to dispatch queued callbacks into SourcePawn
- **IO thread**: A single `boost::asio::io_context` runs in a background thread (`SocketHandler::RunIoService`), processing all async socket operations

All socket operations (connect, send, receive, listen/accept) are async via Boost.Asio. Results are queued as `Callback` objects and executed on the game thread.

### Key Classes

- **`Extension`** (`Extension.h/cpp`): SourceMod extension entry point. Registers natives, creates handle type, hooks `GameFrame`. Contains the native function implementations (SocketCreate, SocketConnect, SocketSend, etc.) that bridge SourcePawn calls to C++ Socket objects.

- **`Socket<SocketType>`** (`Socket.h/cpp`): Template class parameterized on `tcp` or `udp` (Boost.Asio protocol types). Manages async connect, send, receive, listen/accept, and disconnect. Uses reference counting (`m_async_count`) to prevent destruction while async operations are in-flight. Destructor chain posts cleanup to the io_context to avoid races.

- **`SocketHandler`** (`SocketHandler.h/cpp`): Owns the `io_context`, its worker thread, and the list of all `SocketWrapper` objects. Creates/destroys sockets.

- **`SocketWrapper`** (`SocketHandler.h`): Type-erased wrapper (stores `void*` to `Socket<tcp>` or `Socket<udp>` plus the `SM_SocketType` enum) so sockets can be stored in a single list regardless of protocol.

- **`Callback`** (`Callback.h/cpp`): Represents a queued event (connect, disconnect, receive, incoming connection, error, send queue empty). Constructed on the IO thread, executed on the game thread.

- **`CallbackHandler`** (`CallbackHandler.h/cpp`): Thread-safe queue of `Callback*` objects. `AddCallback` is called from IO thread; `ExecuteQueuedCallbacks` is called from game thread.

- **`Define.h`**: Enums for error types, socket types, socket options, and callback events.

### SourcePawn API
Defined in `scripting/include/socket.inc`. Exposes both legacy function-style natives (`SocketCreate`, `SocketConnect`, etc.) and modern methodmap syntax (`Socket.Socket`, `Socket.Connect`, etc.). Both map to the same native implementations.

### Concurrency
- `boost::shared_mutex handlerMutex` on each Socket prevents destruction while handlers are running (shared lock held by handlers, exclusive lock acquired during destroy)
- `boost::mutex socketMutex` protects the underlying Boost.Asio socket
- `boost::mutex callbackQueueMutex` protects the callback queue
- DNS resolution has a 2-second timeout via `boost::asio::deadline_timer`

## File Layout
- Root `.cpp/.h` files: All extension source code (no subdirectories for source)
- `sdk/`: SourceMod SDK glue (`smsdk_ext.cpp/h`, `smsdk_config.h`)
- `scripting/include/socket.inc`: SourcePawn include file shipped to users
- `examples/`: Sample SourcePawn plugins
- `msvc10/`: Visual Studio project files (legacy)
- `AMBuilder`, `AMBuildScript`, `configure.py`, `PackageScript`: AMBuild build system files
