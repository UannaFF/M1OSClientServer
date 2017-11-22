#include <sys/socket.h>
#include <sys/types.h>
#include <stdio.h>
#include <strings.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>

#include "babble_client.h"
#include "babble_types.h"
#include "babble_communication.h"
#include "babble_utils.h"


int connect_to_server(char* host, int port)
{
    /* creating the socket */
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0){
        perror("ERROR opening socket");
        return -1;
    }

    /* connecting to the server */
    /*printf("Babble client connects to %s:%d\n", host, port);*/
    
    struct sockaddr_in serv_addr;
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;

    struct hostent *server= gethostbyname(host);
    if (server == NULL) {
        perror("gethostbyname");
        close(sockfd);
        return -1;
    }

    bcopy((char *)server->h_addr, 
          (char *)&serv_addr.sin_addr.s_addr,
          server->h_length);
    serv_addr.sin_port = htons(port);
    
    if (connect(sockfd,(struct sockaddr *) &serv_addr,sizeof(serv_addr)) < 0){
        perror("ERROR connecting");
        close(sockfd);
        return -1;
    }

    return sockfd;
}


unsigned long client_login(int sock, char* id)
{
    char buffer[BABBLE_BUFFER_SIZE];
    bzero(buffer, BABBLE_BUFFER_SIZE);

    if(strlen(id) > BABBLE_ID_SIZE){
        fprintf(stderr,"Error -- invalid client id (too long): %s\n", id);
        fprintf(stderr,"Max id size is %d\n", BABBLE_ID_SIZE);
        return 0;
    }
    
    snprintf(buffer, BABBLE_BUFFER_SIZE, "%d %s\n", LOGIN, id);

    
    if (network_send(sock, strlen(buffer)+1, buffer) != strlen(buffer)+1){
        perror("ERROR writing to socket");
        close(sock);
        return 0;
    }
    
    char* login_ack=NULL;
    
    if(network_recv(sock, (void**) &login_ack) == -1){
        perror("ERROR reading from socket");
        close(sock);
        return 0;
    }

    /* parsing the answer to get the key */
    unsigned long key=parse_login_ack(login_ack);
    
    free(login_ack);
    
    return key;
}


int client_follow(int sock, char* id, int with_streaming)
{
    char buffer[BABBLE_BUFFER_SIZE];
    bzero(buffer, BABBLE_BUFFER_SIZE);

    if(strlen(id) > BABBLE_ID_SIZE){
        fprintf(stderr,"Error -- invalid client id (too long): %s\n", id);
        fprintf(stderr,"Max id size is %d\n", BABBLE_ID_SIZE);
        return -1;
    }

    if(with_streaming){
        snprintf(buffer, BABBLE_BUFFER_SIZE, "S %d %s\n", FOLLOW, id);
    }
    else{
        snprintf(buffer, BABBLE_BUFFER_SIZE, "%d %s\n", FOLLOW, id);
    }

    if (network_send(sock, strlen(buffer)+1, buffer) != strlen(buffer)+1){
        fprintf(stderr,"Error -- sending FOLLOW message\n");
        return -1;
    }

    if(!with_streaming){
        char* ack=NULL;
        
        if(network_recv(sock, (void**) &ack) == -1){
            perror("ERROR reading from socket");
            close(sock);
            return -1;
        }

        /* check if answer is ok */
        if(strstr(ack, "follow")!=NULL){
            free(ack);
            return 0;
        }

        free(ack);
        return -1;
    }
    else{
        usleep(100);
    }


    return 0;
}


int client_follow_count(int sock)
{
    char buffer[BABBLE_BUFFER_SIZE];
    bzero(buffer, BABBLE_BUFFER_SIZE);

    snprintf(buffer, BABBLE_BUFFER_SIZE, "%d\n", FOLLOW_COUNT);

    if (network_send(sock, strlen(buffer)+1, buffer) != strlen(buffer)+1){
        fprintf(stderr,"Error -- sending FOLLOW_COUNT message\n");
        return -1;
    }

    char* count_ack=NULL;
    
    if(network_recv(sock, (void**) &count_ack) == -1){
        perror("ERROR reading from socket");
        close(sock);
        return 0;
    }
    
    /* parsing the answer to get the key */
    int count=parse_fcount_ack(count_ack);
    
    free(count_ack);
    
    return count;
}


int client_publish(int sock, char* msg, int with_streaming)
{
    char buffer[BABBLE_BUFFER_SIZE];
    bzero(buffer, BABBLE_BUFFER_SIZE);

    if(strlen(msg) > BABBLE_SIZE){
        fprintf(stderr,"Error -- invalid msg (too long): %s\n", msg);
        fprintf(stderr,"Max msg size is %d\n", BABBLE_SIZE);
        return -1;
    }

    if(with_streaming){
        snprintf(buffer, BABBLE_BUFFER_SIZE, "S %d %s\n", PUBLISH, msg);
    }
    else{
        snprintf(buffer, BABBLE_BUFFER_SIZE, "%d %s\n", PUBLISH, msg);
    }

    if (network_send(sock, strlen(buffer)+1, buffer) != strlen(buffer)+1){
        fprintf(stderr,"Error -- sending PUBLISH message\n");
        return -1;
    }

    if(!with_streaming){
        char* ack=NULL;
        
        if(network_recv(sock, (void**) &ack) == -1){
            perror("ERROR reading from socket");
            close(sock);
            return -1;
        }

        /* check if answer is ok */
        if(strstr(ack, "{")!=NULL){
            free(ack);
            return 0;
        }

        free(ack);
        return -1;
    }
    else{
        usleep(100);
    }


    return 0;
}

/* return the size of the timeline */
/* if size_out is set, always return timeline size. Otherwise simply
 * return -1 in case of error */
int client_timeline(int sock, int size_out)
{
    char buffer[BABBLE_BUFFER_SIZE];
    bzero(buffer, BABBLE_BUFFER_SIZE);
    
    snprintf(buffer, BABBLE_BUFFER_SIZE, "%d\n", TIMELINE);

    if (network_send(sock, strlen(buffer)+1, buffer) != strlen(buffer)+1){
        fprintf(stderr,"Error -- sending TIMELINE message\n");
        return -1;
    }

    int *nb_items, i=0;
    char *recv_buf;
    int total_items;

    if(network_recv(sock, (void**) &nb_items) != sizeof(int)){
        perror("ERROR in timeline protocol");
        return -1;
    }

    /* recv only the last BABBLE_TIMELINE_MAX */
    int to_recv= (*nb_items < BABBLE_TIMELINE_MAX)? *nb_items :  BABBLE_TIMELINE_MAX;
    total_items = *nb_items;

    free(nb_items);

    while(i < to_recv){
        if(network_recv(sock, (void**) &recv_buf) < 0){
            perror("ERROR reading from socket");
            break;
        }
        free(recv_buf);
        i++;
    }
    
    if(i < to_recv){
        if(size_out){
            return i;
        }
        return -1;
    }
 
    return total_items;
}


int client_rdv(int sock)
{
    char buffer[BABBLE_BUFFER_SIZE];
    bzero(buffer, BABBLE_BUFFER_SIZE);

    snprintf(buffer, BABBLE_BUFFER_SIZE, "%d\n", RDV);
 
    if (network_send(sock, strlen(buffer)+1, buffer) != strlen(buffer)+1){
        fprintf(stderr,"Error -- sending RDV message\n");
        return -1;
    }
    
    char* ack=NULL;
        
    if(network_recv(sock, (void**) &ack) == -1){
        perror("ERROR reading from socket");
        close(sock);
        return -1;
    }

    /* check if answer is ok */
    if(strstr(ack, "rdv_ack")!=NULL){
        free(ack);
        return 0;
    }

    free(ack);
    return -1;
}
