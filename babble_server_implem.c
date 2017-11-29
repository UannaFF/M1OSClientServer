#include <stdio.h>
#include <unistd.h>
#include <strings.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>


#include "babble_server.h"
#include "babble_utils.h"
#include "babble_types.h"
#include "babble_communication.h"
#include "babble_registration.h"

time_t server_start;

/* used to create a timeline of publications*/
typedef struct timeline_item{
    publication_t *pub;   /* the publication */
    client_bundle_t *client;   /* publication author */
    struct timeline_item *next;
} timeline_item_t;


/* freeing client_bundle_t struct */
static void free_client_data(client_bundle_t *client)
{
    if(client == NULL){
        return;
    }


    /* IMPORTANT: we choose not to free client_bundle_t structures when
     * a client disconnects. The reason is that pointers to this data
     * structure are stored if several places in the code, and so,
     * freeing properly would be a complex operation */
    
    /*publication_t *item= client->pub_list, *previous;
    while(item!=NULL){
        previous=item;
        item = item->next;
        free(previous);
    }
    free(client);*/
}

/* stores an error message in the answer_set of a command */
void generate_cmd_error(command_t *cmd)
{
    /* lookup client */
    client_bundle_t *client = registration_lookup(cmd->key);

    

    if(client == NULL){
        fprintf(stderr, "Error -- no client found\n");
        return;
    }

    cmd->answer.size = -1;
    cmd->answer.aset = malloc(sizeof(answer_t));

    if(cmd->cid == LOGIN || cmd->cid == PUBLISH || cmd->cid == FOLLOW){
        snprintf(cmd->answer.aset->msg, BABBLE_BUFFER_SIZE,"%s[%ld]: ERROR -> %d { %s } \n", client->client_name, time(NULL)-server_start, cmd->cid, cmd->msg);
    }
    else{
        snprintf(cmd->answer.aset->msg, BABBLE_BUFFER_SIZE,"%s[%ld]: ERROR -> %d \n", client->client_name, time(NULL)-server_start, cmd->cid);

    }
}

/* can be used to display the content of a command */
void display_command(command_t *cmd, FILE* stream)
{
    switch(cmd->cid){
    case LOGIN:
        fprintf(stream,"LOGIN: %s\n", cmd->msg);
        break;
    case PUBLISH:
        fprintf(stream,"PUBLISH: %s\n", cmd->msg);
        break;
    case FOLLOW:
        fprintf(stream,"FOLLOW: %s\n", cmd->msg);
        break;
    case TIMELINE:
        fprintf(stream,"TIMELINE\n");
        break;
    case FOLLOW_COUNT:
        fprintf(stream,"FOLLOW_COUNT\n");
        break;
    case RDV:
        fprintf(stream,"RDV\n");
        break;
    default:
        fprintf(stream,"Error -- Unknown command id\n");
        return;
    }
}

/* initialize the server */
void server_data_init(void)
{
    server_start = time(NULL);

    registration_init();
}

/* open a socket to receive client connections */
int server_connection_init(int port)
{
    int sockfd;
    struct sockaddr_in serv_addr;
    int reuse_opt=1;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0){
        perror("ERROR opening socket");
        return -1;
    }

    if(setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (void*)&reuse_opt, sizeof(reuse_opt)) < 0 && setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, (void*)&reuse_opt, sizeof(reuse_opt)) < 0 ){
        perror("setsockopt failed\n");
        close(sockfd);
        return -1;
    }

    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);

    if (bind(sockfd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0){
        perror("ERROR on binding");
        close(sockfd);
        return -1;
    }
    
    if(listen(sockfd, BABBLE_BACKLOG)){
        perror("ERROR on listen");
        close(sockfd);
        return -1;
    }

    return sockfd;
}

/* accept connections of the server socket and return corresponding
 * new file descriptor */
int server_connection_accept(int sock)
{
    int new_sock;
    struct sockaddr_in cli_addr;
    socklen_t clilen = sizeof(cli_addr);

    new_sock = accept(sock, (struct sockaddr *) &cli_addr, &clilen);
    
    if (new_sock < 0){
        perror("ERROR on accept");
        close(sock);
        return -1;
    }
    
    return new_sock;
}


/* create a new command for client corresponding to key */
command_t* new_command(unsigned long key)
{
    command_t *cmd = malloc(sizeof(command_t));
    cmd->key = key;
    cmd->answer.size=-2;
    cmd->answer.aset=NULL;
    cmd->answer_expected=0;

    return cmd;
}



int run_login_command(command_t *cmd)
{
    struct timespec tt;
    clock_gettime(CLOCK_REALTIME, &tt);
    
    /* compute hash of the new client id */
    cmd->key = hash(cmd->msg);
    
    client_bundle_t *client_data=malloc(sizeof(client_bundle_t));
    
    strncpy(client_data->client_name, cmd->msg, BABBLE_ID_SIZE);
    client_data->sock = cmd->sock;
    client_data->key=cmd->key;
    client_data->pub_set=publication_set_create();
    client_data->last_timeline=(uint64_t)1000000000 * tt.tv_sec + tt.tv_nsec;
    /* by default, we follow ourself*/
    client_data->followed[0]=client_data;
    client_data->nb_followed=1;
    client_data->nb_followers=1;

    //Protected write
    //pthread_rwlock_wrlock(&lock_rw);
    if(registration_insert(client_data)){
        free(client_data->pub_set);
        free(client_data);
        generate_cmd_error(cmd);
        ////pthread_rwlock_unlock(&lock_rw);
        return -1;
    }
    //pthread_rwlock_unlock(&lock_rw);
    
    printf("### New client %s (key = %lu)\n", client_data->client_name, client_data->key);

    /* answer to client */
    cmd->answer.size = -1;
    cmd->answer.aset = malloc(sizeof(answer_t));
    
    snprintf(cmd->answer.aset->msg, BABBLE_BUFFER_SIZE,"%s[%ld]: registered with key %lu\n", client_data->client_name, tt.tv_sec - server_start, client_data->key);
    
    return 0;
}


int run_publish_command(command_t *cmd)
{    
    //pthread_rwlock_rdlock(&lock_rw);
    client_bundle_t *client = registration_lookup(cmd->key);
    //pthread_rwlock_unlock(&lock_rw);
    
    if(client == NULL){
        fprintf(stderr, "Error -- no client found\n");
        generate_cmd_error(cmd);
        return -1;
    }
    
    publication_t *pub = publication_set_insert(client->pub_set, cmd->msg);
    
    printf("### Client %s published { %s } at date %ld\n", client->client_name, pub->msg, pub->date);

    /* answer to client */
    cmd->answer.size = -1;
    cmd->answer.aset = malloc(sizeof(answer_t));

    snprintf(cmd->answer.aset->msg, BABBLE_BUFFER_SIZE,"%s[%ld]: { %s }\n", client->client_name, pub->date, pub->msg);
    
    return 0;
}


int run_follow_command(command_t *cmd)
{
    //pthread_rwlock_rdlock(&lock_rw);
    client_bundle_t *client = registration_lookup(cmd->key);
    //pthread_rwlock_unlock(&lock_rw);
    
    if(client == NULL){
        fprintf(stderr, "Error -- no client found\n");
        generate_cmd_error(cmd);
        return -1;
    }

    /* compute hash of the client to follow */
    unsigned long f_key = hash(cmd->msg);

    /* lookup client to follow */
    //pthread_rwlock_rdlock(&lock_rw);
    client_bundle_t *f_client = registration_lookup(f_key);
    //pthread_rwlock_unlock(&lock_rw);
    
    if(f_client == NULL){
        generate_cmd_error(cmd);        
        return 0;
    }
    
    /* if client is not already followed, add it*/
    int i=0;

    for(i=0; i<client->nb_followed; i++){
        if(client->followed[i]->key == f_key){
            break;
        }
    }
    
    if(i == client->nb_followed){
        client->followed[i]=f_client;
        client->nb_followed++;
        f_client->nb_followers++;
    }

    
    /* answer to client */
    cmd->answer.size = -1;
    cmd->answer.aset = malloc(sizeof(answer_t));

    snprintf(cmd->answer.aset->msg, BABBLE_BUFFER_SIZE,"%s[%ld]: follow %s\n", client->client_name, time(NULL)-server_start, f_client->client_name);

    return 0;
}


int run_timeline_command(command_t *cmd)
{
    int i=0;
    timeline_item_t *pub_list=NULL;
    int item_count=0;
    
    /* get current time to know up to when we publish*/
    struct timespec tt;
    clock_gettime(CLOCK_REALTIME, &tt);

    uint64_t end_time= (uint64_t)1000000000 * tt.tv_sec + tt.tv_nsec;

    /* lookup client */
    //pthread_rwlock_rdlock(&lock_rw);
    client_bundle_t *client = registration_lookup(cmd->key);
    //pthread_rwlock_unlock(&lock_rw);

    if(client == NULL){
        fprintf(stderr, "Error -- no client found\n");
        generate_cmd_error(cmd);
        return -1;
    }

    /* start from where we finished last time*/
    uint64_t start_time=client->last_timeline;
    
    /* gather publications over all followed clients */
    for(i=0; i < client->nb_followed; i++){
        client_bundle_t *f_client=client->followed[i];
        publication_t *pub=NULL;

        timeline_item_t *time_iter=pub_list, *prev;
        /* add recent items to the totally ordered list */
        while((pub = publication_set_getnext(f_client->pub_set, pub, start_time)) != NULL){
            /* if publication is more recent than the timeline
               command, we do not consider it */
            if(pub->ndate > end_time){
                break;
            }
            
            timeline_item_t *item=malloc(sizeof(timeline_item_t));
            item->pub=pub;
            item->client= f_client;
            item->next = NULL;

            while(time_iter!=NULL && time_iter->pub->date < pub->date){
                prev = time_iter;
                time_iter = time_iter->next;
            }

            /* insert in first position */
            if(time_iter == pub_list){
                pub_list = item;
                item->next=time_iter;
            }
            else{
                item->next=time_iter;
                prev->next=item;
            }
            item_count++;
            time_iter = item;
        }
    }


    /* now that we have a full timeline, we generate the messages to
     * the client */
    
    /* save number of items to transmit */
    cmd->answer.size = item_count;
    
    /* generate each item*/
    answer_t *current_answer=NULL;
    timeline_item_t *time_iter=pub_list;
    while(time_iter != NULL){
        if(current_answer == NULL){
            current_answer = malloc(sizeof(answer_t));
            current_answer->next=NULL;
            cmd->answer.aset = current_answer;
        }
        else{
            current_answer->next = malloc(sizeof(answer_t));
            current_answer = current_answer->next;
            current_answer->next=NULL;
        }
                
        snprintf(current_answer->msg, BABBLE_BUFFER_SIZE,"    %s[%ld]: %s\n", time_iter->client->client_name, time_iter->pub->date, time_iter->pub->msg);
    
        time_iter = time_iter->next;
    }

    client->last_timeline = end_time;
    
    return 0;
}


int run_fcount_command(command_t *cmd)
{
    /* lookup client */
    //pthread_rwlock_rdlock(&lock_rw);
    client_bundle_t *client = registration_lookup(cmd->key);
    //pthread_rwlock_unlock(&lock_rw);

    if(client == NULL){
        fprintf(stderr, "Error -- no client found\n");
        generate_cmd_error(cmd);
        return -1;
    }
    
    /* answer to client */
    cmd->answer.size = -1;
    cmd->answer.aset = malloc(sizeof(answer_t));
    
    snprintf(cmd->answer.aset->msg, BABBLE_BUFFER_SIZE,"%s[%ld]: has %d followers\n", client->client_name, time(NULL) - server_start, client->nb_followers);
    
    return 0;
}

int run_rdv_command(command_t *cmd)
{
    /* lookup client */
    //pthread_rwlock_rdlock(&lock_rw);
    client_bundle_t *client = registration_lookup(cmd->key);
    //pthread_rwlock_unlock(&lock_rw);
    
    if(client == NULL){
        fprintf(stderr, "Error -- no client found\n");
        generate_cmd_error(cmd);
        return -1;
    }
    
    /* answer to client */
    cmd->answer.size = -1;
    cmd->answer.aset = malloc(sizeof(answer_t));
    
    snprintf(cmd->answer.aset->msg, BABBLE_BUFFER_SIZE,"%s[%ld]: rdv_ack\n", client->client_name, time(NULL) - server_start);
    
    return 0;
}


int unregisted_client(command_t *cmd)
{
    assert(cmd->cid == UNREGISTER);
    
    /* remove client */
    //pthread_rwlock_wrlock(&lock_rw);
    client_bundle_t *client = registration_remove(cmd->key);
    //pthread_rwlock_unlock(&lock_rw);

    if(client != NULL){
        printf("### Unregister client %s (key = %lu)\n", client->client_name, client->key);
        close(client->sock);
     
        
        free_client_data(client);
    }
    

    return 0;
}


/* send error msg to client in case the input msg could not be parsed */
int notify_parse_error(command_t *cmd, char *input)
{
    /* lookup client */
    //pthread_rwlock_rdlock(&lock_rw);
    client_bundle_t *client = registration_lookup(cmd->key);
    //pthread_rwlock_unlock(&lock_rw);

    if(client == NULL){
        fprintf(stderr, "Error -- no client found\n");
        return -1;
    }


    if(cmd->answer_expected){
        char buffer[BABBLE_BUFFER_SIZE];
        
        snprintf(buffer, BABBLE_BUFFER_SIZE,"%s[%ld]: ERROR -> %s\n", client->client_name, time(NULL)-server_start, input);
    
        if(write_to_client(cmd->key, strlen(buffer)+1, buffer)){
            fprintf(stderr,"Error -- could not send error msg: %s\n", buffer);
            return -1;
        }
    }
    
    return 0;
}


/* send buf to client identified by key */
int write_to_client(unsigned long key, int size, void* buf)
{

    //pthread_rwlock_rdlock(&lock_rw);
    client_bundle_t *client = registration_lookup(key);
    //pthread_rwlock_unlock(&lock_rw);

    if(client == NULL){
        fprintf(stderr, "Error -- writing to non existing client %lu\n", key);
        return -1;
    }
    
    int write_size = network_send(client->sock, size, buf);
            
    if (write_size < 0){
        perror("writing to socket");
        return -1;
    }

    return 0;
}