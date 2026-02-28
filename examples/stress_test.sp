#include <sourcemod>
#include <socket>

#pragma newdecls required
#pragma semicolon 1

public Plugin myinfo = {
	name        = "Socket Stress Test",
	author      = "test",
	description = "Combined TCP+UDP echo stress test, loops forever",
	version     = "1.0",
	url         = ""
};

// Run stress_server.py before starting.
#define SERVER_IP     "127.0.0.1"
#define TCP_PORT      9000
#define UDP_PORT      9001
#define UDP_BIND_PORT 9002

#define TCP_TOTAL    1000
#define TCP_MSG_SIZE 64
#define UDP_TOTAL    500
#define UDP_MSG_SIZE 64

// ---- shared loop control ----
bool g_running = false;

// ---- TCP state ----
int   g_tcpIter       = 0;
int   g_tcpTotalBytes = 0;
int   g_tcpSent       = 0;
int   g_tcpRecvd      = 0;
float g_tcpStart      = 0.0;

// ---- UDP state ----
Socket g_udpSock       = null;
int    g_udpIter       = 0;
int    g_udpTotalSent  = 0;
int    g_udpTotalRecvd = 0;
int    g_udpSent       = 0;
int    g_udpRecvd      = 0;
float  g_udpStart      = 0.0;

public void OnPluginStart() {
	RegAdminCmd("sm_stress_start", Cmd_Start, ADMFLAG_ROOT,
		"Start combined TCP+UDP echo stress loop (requires stress_server.py)");
	RegAdminCmd("sm_stress_stop", Cmd_Stop, ADMFLAG_ROOT,
		"Stop stress loop after current iteration");
}

public Action Cmd_Start(int client, int args) {
	if (g_running) {
		ReplyToCommand(client, "[stress] Already running. Use sm_stress_stop to stop.");
		return Plugin_Handled;
	}
	g_running       = true;
	g_tcpIter       = 0;
	g_tcpTotalBytes = 0;
	g_udpIter       = 0;
	g_udpTotalSent  = 0;
	g_udpTotalRecvd = 0;
	PrintToServer("[stress] Starting TCP+UDP loop. sm_stress_stop to stop.");
	StartTCP();
	StartUDP();
	return Plugin_Handled;
}

public Action Cmd_Stop(int client, int args) {
	if (!g_running) {
		ReplyToCommand(client, "[stress] Not running.");
		return Plugin_Handled;
	}
	g_running = false;
	PrintToServer("[stress] Stopping after current iterations finish.");
	PrintToServer("[stress] TCP: %d iters, %d bytes echoed total", g_tcpIter, g_tcpTotalBytes);
	PrintToServer("[stress] UDP: %d iters, %d sent, %d echoed total",
		g_udpIter, g_udpTotalSent, g_udpTotalRecvd);
	return Plugin_Handled;
}

// ================================================================
//  TCP loop  —  completes when all echoed bytes received, then restarts
// ================================================================

static void StartTCP() {
	g_tcpSent  = 0;
	g_tcpRecvd = 0;
	g_tcpStart = GetGameTime();

	Socket sock = new Socket(SOCKET_TCP, TCP_Error);
	if (sock == null || view_as<int>(sock) == 0) {
		PrintToServer("[stress:tcp] ERROR: SocketCreate failed");
		if (g_running) CreateTimer(1.0, TCP_Retry);
		return;
	}
	sock.Connect(TCP_Connected, TCP_Receive, TCP_Disconnected, SERVER_IP, TCP_PORT);
}

public Action TCP_Retry(Handle timer) {
	if (g_running) StartTCP();
	return Plugin_Stop;
}

public void TCP_Connected(Socket sock, any arg) {
	char msg[TCP_MSG_SIZE + 1];
	for (int i = 0; i < TCP_MSG_SIZE - 1; i++) msg[i] = 'A' + (i % 26);
	msg[TCP_MSG_SIZE - 1] = '\n';
	msg[TCP_MSG_SIZE]     = '\0';
	for (int i = 0; i < TCP_TOTAL; i++) sock.Send(msg, TCP_MSG_SIZE);
	g_tcpSent = TCP_TOTAL * TCP_MSG_SIZE;
}

public void TCP_Receive(Socket sock, const char[] data, int dataSize, any arg) {
	g_tcpRecvd += dataSize;
	if (g_tcpRecvd >= g_tcpSent) {
		g_tcpIter++;
		g_tcpTotalBytes += g_tcpRecvd;
		float elapsed = GetGameTime() - g_tcpStart;
		float kbps    = elapsed > 0.0 ? (float(g_tcpSent) / 1024.0) / elapsed : 0.0;
		PrintToServer("[stress:tcp] #%d  echoed=%d B  %.1f KB/s  (cumulative: %d B, %d iters)",
			g_tcpIter, g_tcpRecvd, kbps, g_tcpTotalBytes, g_tcpIter);
		delete sock;
		if (g_running) StartTCP();
	}
}

public void TCP_Disconnected(Socket sock, any arg) {
	PrintToServer("[stress:tcp] #%d disconnected early (echoed %d / %d B)",
		g_tcpIter + 1, g_tcpRecvd, g_tcpSent);
	delete sock;
	if (g_running) CreateTimer(0.5, TCP_Retry);
}

public void TCP_Error(Socket sock, int errType, int errNum, any arg) {
	PrintToServer("[stress:tcp] #%d ERROR type=%d errno=%d", g_tcpIter + 1, errType, errNum);
	delete sock;
	if (g_running) CreateTimer(1.0, TCP_Retry);
}

// ================================================================
//  UDP loop  —  sends all datagrams, waits 2 s for echoes, then restarts
// ================================================================

static void StartUDP() {
	if (g_udpSock != null) return;

	g_udpSent  = 0;
	g_udpRecvd = 0;
	g_udpStart = GetGameTime();

	Socket sock = new Socket(SOCKET_UDP, UDP_Error);
	if (sock == null || view_as<int>(sock) == 0) {
		PrintToServer("[stress:udp] ERROR: SocketCreate failed");
		if (g_running) CreateTimer(1.0, UDP_Retry);
		return;
	}
	sock.Bind("127.0.0.1", UDP_BIND_PORT);
	sock.Connect(UDP_Connected, UDP_Receive, UDP_Disconnected, SERVER_IP, UDP_PORT);
	g_udpSock = sock;
}

public Action UDP_Retry(Handle timer) {
	if (g_running) StartUDP();
	return Plugin_Stop;
}

public void UDP_Connected(Socket sock, any arg) {
	char msg[UDP_MSG_SIZE + 1];
	for (int i = 0; i < UDP_MSG_SIZE; i++) msg[i] = 'U';
	msg[UDP_MSG_SIZE] = '\0';
	for (int i = 0; i < UDP_TOTAL; i++) sock.Send(msg, UDP_MSG_SIZE);
	g_udpSent = UDP_TOTAL;
	CreateTimer(2.0, UDP_Report, _, TIMER_FLAG_NO_MAPCHANGE);
}

public void UDP_Receive(Socket sock, const char[] data, int dataSize, any arg) {
	g_udpRecvd++;
}

public Action UDP_Report(Handle timer) {
	g_udpIter++;
	g_udpTotalSent  += g_udpSent;
	g_udpTotalRecvd += g_udpRecvd;
	int   loss   = g_udpSent - g_udpRecvd;
	float losspc = g_udpSent > 0 ? float(loss) / float(g_udpSent) * 100.0 : 0.0;
	PrintToServer("[stress:udp] #%d  sent=%d  echoed=%d  loss=%d (%.1f%%)  (cumulative: %d sent, %d echoed)",
		g_udpIter, g_udpSent, g_udpRecvd, loss, losspc, g_udpTotalSent, g_udpTotalRecvd);

	if (g_udpSock != null) {
		delete g_udpSock;
		g_udpSock = null;
	}
	if (g_running) StartUDP();
	return Plugin_Stop;
}

public void UDP_Disconnected(Socket sock, any arg) {
	PrintToServer("[stress:udp] #%d disconnected.", g_udpIter + 1);
	g_udpSock = null;
	delete sock;
	if (g_running) CreateTimer(0.5, UDP_Retry);
}

public void UDP_Error(Socket sock, int errType, int errNum, any arg) {
	PrintToServer("[stress:udp] #%d ERROR type=%d errno=%d", g_udpIter + 1, errType, errNum);
	g_udpSock = null;
	delete sock;
	if (g_running) CreateTimer(1.0, UDP_Retry);
}
