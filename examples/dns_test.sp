#include <sourcemod>
#include <socket>

#pragma newdecls required
#pragma semicolon 1

public Plugin myinfo = {
	name        = "DNS Resolution Test",
	author      = "test",
	description = "Tests DNS resolution via socket extension",
	version     = "1.0",
	url         = ""
};

#define TEST_PORT 80

// Maps socket handle (as string) -> hostname, so concurrent tests don't clobber each other.
StringMap g_hosts;

public void OnPluginStart() {
	g_hosts = new StringMap();
	RegAdminCmd("sm_dns_test", Cmd_DnsTest, ADMFLAG_ROOT, "Usage: sm_dns_test <hostname>");
}

public Action Cmd_DnsTest(int client, int args) {
	if (args < 1) {
		ReplyToCommand(client, "[dns_test] Usage: sm_dns_test <hostname>");
		return Plugin_Handled;
	}

	char host[256];
	GetCmdArg(1, host, sizeof(host));

	PrintToServer("[dns_test] Resolving '%s:%d' ...", host, TEST_PORT);

	Socket sock = new Socket(SOCKET_TCP, OnError);
	if (sock == null || view_as<int>(sock) == 0) {
		PrintToServer("[dns_test] ERROR: SocketCreate returned invalid handle (extension not loaded?)");
		return Plugin_Handled;
	}

	// Store hostname keyed by handle so callbacks can retrieve it.
	char key[16];
	IntToString(view_as<int>(sock), key, sizeof(key));
	g_hosts.SetString(key, host);

	sock.Connect(OnConnected, OnReceive, OnDisconnected, host, TEST_PORT);
	return Plugin_Handled;
}

static void GetHost(Socket sock, char[] host, int maxlen) {
	char key[16];
	IntToString(view_as<int>(sock), key, sizeof(key));
	if (!g_hosts.GetString(key, host, maxlen))
		host[0] = '\0';
}

static void RemoveHost(Socket sock) {
	char key[16];
	IntToString(view_as<int>(sock), key, sizeof(key));
	g_hosts.Remove(key);
}

public void OnConnected(Socket sock, any arg) {
	char host[256];
	GetHost(sock, host, sizeof(host));
	PrintToServer("[dns_test] SUCCESS: connected to '%s:%d' — DNS resolved and TCP handshake complete", host, TEST_PORT);

	char req[512];
	Format(req, sizeof(req), "HEAD / HTTP/1.0\r\nHost: %s\r\nConnection: close\r\n\r\n", host);
	sock.Send(req);
}

public void OnReceive(Socket sock, const char[] data, int dataSize, any arg) {
	// Print only the first line of the response (status line).
	char line[128];
	int end = dataSize < sizeof(line) - 1 ? dataSize : sizeof(line) - 1;
	for (int i = 0; i < end; i++) {
		if (data[i] == '\r' || data[i] == '\n') { line[i] = '\0'; break; }
		line[i] = data[i];
	}
	PrintToServer("[dns_test] Server replied: %s", line);
}

public void OnDisconnected(Socket sock, any arg) {
	PrintToServer("[dns_test] Connection closed cleanly.");
	RemoveHost(sock);
	delete sock;
}

public void OnError(Socket sock, int errorType, int errorNum, any arg) {
	char host[256];
	GetHost(sock, host, sizeof(host));
	PrintToServer("[dns_test] FAILED resolving '%s': errorType=%d errno=%d", host, errorType, errorNum);
	switch (errorType) {
		case 2: PrintToServer("[dns_test]   -> DNS resolution failed or timed out (2s limit)");
		case 3: PrintToServer("[dns_test]   -> TCP connect failed (host unreachable or port closed)");
	}
	RemoveHost(sock);
	delete sock;
}
