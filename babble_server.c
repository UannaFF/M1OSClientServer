#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <assert.h>
#include <pthread.h>
#include <semaphore.h>

#include "babble_server.h"
#include "babble_types.h"
#include "babble_utils.h"
#include "babble_communication.h"

#define CBUFFER_SIZE 1000
command_t command_buffer[CBUFFER_SIZE];
int bcount = 0;

//semaphores
sem_t sem_cs;
sem_t sem_put;
sem_t sem_process;

//sem_t sem_threads;
//sem_t sem;


static void display_help(char *exec)
{
    printf("Usage: %s -p port_number\n", exec);
}


static int parse_command(char* str, command_t *cmd)
{
    /* start by cleaning the input */
    str_clean(str);
    
    /* get command id */
    cmd->cid=str_to_command(str, &cmd->answer_expected);

    /* initialize other fields */
    cmd->answer.size=-1;
    cmd->answer.aset=NULL;

    switch(cmd->cid){
    case LOGIN:
        if(str_to_payload(str, cmd->msg, BABBLE_ID_SIZE)){
            fprintf(stderr,"Error -- invalid LOGIN -> %s\n", str);
            return -1;
        }
        break;
    case PUBLISH:
        if(str_to_payload(str, cmd->msg, BABBLE_SIZE)){
            fprintf(stderr,"Warning -- invalid PUBLISH -> %s\n", str);
            return -1;
        }
        break;
    case FOLLOW:
        if(str_to_payload(str, cmd->msg, BABBLE_ID_SIZE)){
            fprintf(stderr,"Warning -- invalid FOLLOW -> %s\n", str);
            return -1;
        }
        break;
    case TIMELINE:
        cmd->msg[0]='\0';
        break;
    case FOLLOW_COUNT:
        cmd->msg[0]='\0';
        break;
    case RDV:
        cmd->msg[0]='\0';
        break;    
    default:
        fprintf(stderr,"Error -- invalid client command -> %s\n", str);
        return -1;
    }

    return 0;
}


static int process_command(command_t *cmd)
{
    int res=0;

    switch(cmd->cid){
    case LOGIN:
        res = run_login_command(cmd);
        break;
    case PUBLISH:
        res = run_publish_command(cmd);
        break;
    case FOLLOW:
        res = run_follow_command(cmd);
        break;
    case TIMELINE:
        res = run_timeline_command(cmd);
        break;
    case FOLLOW_COUNT:
        res = run_fcount_command(cmd);
        break;
    case RDV:
        res = run_rdv_command(cmd);
        break;
    default:
        fprintf(stderr,"Error -- Unknown command id\n");
        return -1;
    }

    if(res){
        fprintf(stderr,"Error -- Failed to run command ");
        display_command(cmd, stderr);
    }

    return res;
}

/* sends an answer for the command to the client if needed */
/* answer to a command is stored in cmd->answer after the command has
 * been processed. They are different cases
 + The client does not expect any answer (then nothing is sent)
 + The client expect an answer -- 2 cases
  -- The answer is a single msg
  -- The answer is potentially composed of multiple msgs (case of a timeline)
*/
static int answer_command(command_t *cmd)
{    
    /* case of no answer requested by the client */
    if(!cmd->answer_expected){
        if(cmd->answer.aset != NULL){
            free(cmd->answer.aset);
        }
        return 0;
    }
    
    /* no msg to be sent */
    if(cmd->answer.size == -2){
        return 0;
    }

    /* a single msg to be sent */
    if(cmd->answer.size == -1){
        /* strlen()+1 because we want to send '\0' in the message */
        if(write_to_client(cmd->key, strlen(cmd->answer.aset->msg)+1, cmd->answer.aset->msg)){
            fprintf(stderr,"Error -- could not send ack for %d\n", cmd->cid);
            free(cmd->answer.aset);
            return -1;
        }
        free(cmd->answer.aset);
        return 0;
    }
    

    /* a set of msgs to be sent */
    /* number of msgs sent first */
    if(write_to_client(cmd->key, sizeof(int), &cmd->answer.size)){
        fprintf(stderr,"Error -- send set size: %d\n", cmd->cid);
        return -1;
    }

    answer_t *item = cmd->answer.aset, *prev;
    int count=0;

    /* send only the last BABBLE_TIMELINE_MAX */
    int to_skip= (cmd->answer.size > BABBLE_TIMELINE_MAX)? cmd->answer.size - BABBLE_TIMELINE_MAX : 0;

    for(count=0; count < to_skip; count++){
        prev=item;
        item = item->next;
        free(prev);
    }
    
    while(item != NULL ){
        if(write_to_client(cmd->key, strlen(item->msg)+1, item->msg)){
            fprintf(stderr,"Error -- could not send set: %d\n", cmd->cid);
            return -1;
        }
        prev=item;
        item = item->next;
        free(prev);
        count++;
    }

    assert(count == cmd->answer.size);
    return 0;
}

void* executor_thread(void* arg) {

    while(1) {
        //Protect
        sem_wait(&sem_process);
        sem_wait(&sem_cs);

        bcount--;
        command_t cmd = command_buffer[bcount];
        

        sem_post(&sem_cs);
        sem_post(&sem_put);

        if(process_command(&cmd) == -1){
            fprintf(stderr, "Warning: unable to process command from client %lu\n", cmd.key);
        }

        if(answer_command(&cmd) == -1){
            fprintf(stderr, "Warning: unable to answer command from client %lu\n", cmd.key);
        }
    }
    
    return NULL;
}

//Communication thread to receive messages
void* communication_thread(void* arg)
{

    char* recv_buff=NULL;
    int recv_size=0;

    command_t *cmd;
    unsigned long client_key=0;
    char client_name[BABBLE_ID_SIZE+1];

    int socket = *((int *)arg);
    
    while(1){

        //Manage concurrency?
        fprintf(stderr, "before receiving first message\n");
        bzero(client_name, BABBLE_ID_SIZE+1);
        if((recv_size = network_recv(socket, (void**)&recv_buff)) < 0){
            fprintf(stderr, "Error -- recv from client\n");
            //close(socket);

            break;
            //continue;
        }
        fprintf(stderr, "after receiving first message\n");
        cmd = new_command(0);
        
        fprintf(stderr, "before parsing\n");
        if(parse_command(recv_buff, cmd) == -1 || cmd->cid != LOGIN){
            fprintf(stderr, "Error -- in LOGIN message\n");
            //close(socket);
            free(cmd);
            break;
            //continue;
        }

        /* before processing the command, we should register the
         * socket associated with the new client; this is to be done only
         * for the LOGIN command */
        cmd->sock = socket;
    
        fprintf(stderr, "before processing\n");
        if(process_command(cmd) == -1){
            fprintf(stderr, "Error -- in LOGIN\n");
            //close(socket);
            free(cmd);
            break;    
        }

        /* notify client of registration */
        if(answer_command(cmd) == -1){
            fprintf(stderr, "Error -- in LOGIN ack\n");
            //close(socket);
            free(cmd);
            break;
            //continue;
        }

        /* let's store the key locally */
        client_key = cmd->key;

        strncpy(client_name, cmd->msg, BABBLE_ID_SIZE);
        free(recv_buff);
        free(cmd);

        /* looping on client commands */
        while((recv_size=network_recv(socket, (void**) &recv_buff)) > 0){
            cmd = new_command(client_key);
            if(parse_command(recv_buff, cmd) == -1){
                fprintf(stderr, "Warning: unable to parse message from client %s\n", client_name);
                notify_parse_error(cmd, recv_buff);
            }
            else{
                //Protect!!!
                sem_wait(&sem_put);
                sem_wait(&sem_cs);

                command_buffer[bcount] = *cmd;
                bcount++;

                sem_post(&sem_cs);
                sem_post(&sem_process);
            }
            free(cmd);
        }

        if(client_name[0] != 0){
            cmd = new_command(client_key);
            cmd->cid= UNREGISTER;
            
            if(unregisted_client(cmd)){
                fprintf(stderr,"Warning -- failed to unregister client %s\n",client_name);
            }

            free(cmd);
            break;
        }
    }

    if(recv_buff != NULL) {
        free(recv_buff);
    }
    close(socket);
    return NULL;
}




int main(int argc, char *argv[])
{
    int sockfd, newsockfd;
    int portno=BABBLE_PORT;
    
    int opt;
    int nb_args=1;

    pthread_t tid_ex;

    //command_buffer = (command_t*)malloc(sizeof(command_t));

    while ((opt = getopt (argc, argv, "+p:")) != -1){
        switch (opt){
        case 'p':
            portno = atoi(optarg);
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

    server_data_init();

    if((sockfd = server_connection_init(portno)) == -1){
        return -1;
    }

    printf("Babble server bound to port %d\n", portno);    
    
    //Semaphore initialization
    sem_init(&sem_cs, 1, 1); //grant access to one
    sem_init(&sem_process, 1, 0); //lock from the begining
    sem_init(&sem_put, 1, CBUFFER_SIZE);

    //sem_init(&sem, 1, 1);
    //sem_init(&sem_threads, 1, BABBLE_COMMUNICATION_THREADS);

    //Create the unique execution thread
    if(pthread_create (&tid_ex, NULL, executor_thread, (void *)NULL) != 0){
        fprintf(stderr,"Failed to create the communication thread\n");
        return EXIT_FAILURE;
    }

    /* main server loop */
    while(1){

        pthread_t tid;

        //Accept connection
        if((newsockfd= server_connection_accept(sockfd))==-1){
            fprintf(stderr,"Failed to create accept SOCKET\n");
            return -1;
        }

        //Create the communication thread
        if(pthread_create (&tid, NULL, communication_thread, (void *)&newsockfd) != 0){
            fprintf(stderr,"Failed to create the communication thread\n");
            return EXIT_FAILURE;
        }
    }
    close(sockfd);
    return 0;
}
