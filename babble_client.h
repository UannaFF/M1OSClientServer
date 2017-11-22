#ifndef __BABBLE_CLIENT_H__
#define __BABBLE_CLIENT_H__

/* connect with server */
int connect_to_server(char* host, int port);
unsigned long client_login(int sock, char* id);

/* interact for tests */
int client_follow(int sock, char* id, int with_streaming);
int client_follow_count(int sock);
int client_publish(int sock, char* msg, int with_streaming);
int client_timeline(int sock, int size_out);
int client_rdv(int sock);


#endif
