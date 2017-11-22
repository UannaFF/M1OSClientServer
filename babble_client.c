#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>

#include "babble_types.h"
#include "babble_communication.h"
#include "babble_utils.h"
#include "babble_client.h"

static void display_help(char *exec)
{
    printf("Usage: %s -m hostname -p port_number -i client_name\n", exec);
    printf("\t hostname can be an ip address\n" );
    printf("\t client_name can only include alphanumeric characters (no white spaces)\n");
}


static void client_console(int sock)
{
    char client_buf[BABBLE_BUFFER_SIZE];
    char *server_buf;

    char *user_buf =NULL;
    size_t user_buf_size=0;

    int cid = -1;
    int answer_expected = 0;
    
    
    while(getline(&user_buf, &user_buf_size, stdin) != -1){
        /* truncate input if need be */
        strncpy(client_buf, user_buf, BABBLE_BUFFER_SIZE);

        str_clean(client_buf);
        
        /* check command on client side */
        cid = str_to_command(client_buf, &answer_expected);
        
        if(cid == -1){
            printf("error -- malformed command : %s\n",client_buf);
            continue;
        }

        if(cid == 0){
            printf("Client already registered. You can run other commands\n");
            continue;
        }
        
        if (network_send(sock, strlen(client_buf)+1,(void*) client_buf) != strlen(client_buf)+1){
            perror("ERROR writing to socket");
            break;
        }
        
        if(cid == TIMELINE){
            
            /* specific protocol for timeline request */
            int *t_length, i;
            if(network_recv(sock, (void**) &t_length) != sizeof(int)){
                perror("ERROR in timeline protocol");
                break;
            }

            printf("Timeline of size: %d\n", *t_length);

            /* recv only the last BABBLE_TIMELINE_MAX */
            int to_recv= (*t_length < BABBLE_TIMELINE_MAX)? *t_length :  BABBLE_TIMELINE_MAX;

            free(t_length);
            
            for(i=0; i< to_recv; i++){
                if(network_recv(sock, (void**) &server_buf) < 0){
                    perror("ERROR reading from socket");
                    break;
                }
                printf("%s", server_buf);
                free(server_buf);
            }

            if(i != to_recv){
                break;
            }
        }
        else{
            if(answer_expected){
                if(network_recv(sock, (void**) &server_buf) < 0){
                    perror("ERROR reading from socket");
                    break;
                }
                printf("%s", server_buf);
                free(server_buf);
            }
        }
        
    }

    printf("### Client exiting \n");
}



int main(int argc, char *argv[])
{
    char hostname[BABBLE_BUFFER_SIZE]="127.0.0.1";
    int portno = BABBLE_PORT;

    int opt;
    int nb_args=1;

    char id_str[BABBLE_BUFFER_SIZE];
    bzero(id_str, BABBLE_BUFFER_SIZE);

    
    /* parsing command options */
    while ((opt = getopt (argc, argv, "+hm:p:i:")) != -1){
        switch (opt){
        case 'm':
            strncpy(hostname,optarg,BABBLE_BUFFER_SIZE);
            nb_args+=2;
            break;
        case 'p':
            portno = atoi(optarg);
            nb_args+=2;
            break;
        case 'i':
            strncpy(id_str,optarg,BABBLE_BUFFER_SIZE);
            nb_args+=2;
            break;
        case 'h':
        case '?':
        default:
            display_help(argv[0]);
            return -1;
        }
    }

    if(nb_args != argc){
        display_help(argv[0]);
        return -1;
    }

    if(strlen(id_str)==0){
        printf("Error: client identifier has to be specified with option -i\n");
        return -1;
    }
    else{
        printf("starting new client with id %s\n",id_str);
    }

    /* connecting to the server */
    printf("Babble client connects to %s:%d\n", hostname, portno);
    
    int sockfd = connect_to_server(hostname, portno);

    if(sockfd == -1){
        fprintf(stderr,"ERROR: failed to connect to server\n");
        return -1;
    }
    
    
    unsigned long key = client_login(sockfd, id_str);
    
    if(key == 0){
        fprintf(stderr,"ERROR: login ack\n");
        close(sockfd);
        return -1;
    }

    printf("Client registered with key %lu\n", key);
    
    client_console(sockfd);
    
    close(sockfd);
    
    return 0;
}
