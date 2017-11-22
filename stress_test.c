#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <pthread.h>

#include "babble_types.h"
#include "babble_communication.h"
#include "babble_utils.h"
#include "babble_client.h"

typedef struct client_thread_data{
    int nb_msgs;
    int nb_clients;
    int client_id;
    pthread_barrier_t *gbarrier;
} client_thread_data_t;


char hostname[BABBLE_BUFFER_SIZE]="127.0.0.1";
int portno = BABBLE_PORT;

int with_streaming = 0;

static void display_help(char *exec)
{
    printf("Usage: %s -m hostname -p port_number -n nb_clients -k nb_msgs -s [activate_streaming]\n", exec);
    printf("\t hostname can be an ip address\n" );
}


static void *client_thread (void *arg)
{
    
    int i=0;
    client_thread_data_t *data= (client_thread_data_t*) arg;
    
    char client_name[BABBLE_ID_SIZE];
    bzero(client_name, BABBLE_ID_SIZE);
    snprintf(client_name, BABBLE_ID_SIZE, "client_%d", data->client_id);


    int sockfd = connect_to_server(hostname, portno);

    if(sockfd == -1){
        fprintf(stderr,"*** Test Failed ***\n");
        fprintf(stderr,"client %s failed to contact server\n", client_name);
        exit(-1);
    }

    unsigned long client_key= client_login(sockfd, client_name);
    
    if(client_key == 0){
        fprintf(stderr,"*** Test Failed ***\n");
        fprintf(stderr,"client %s failed to login\n", client_name);
        close(sockfd);
        exit(-1);
    }

    /* global barrier before starting to follow others */
    int ret = pthread_barrier_wait(data->gbarrier);
    if (ret != 0 && ret != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        fprintf(stderr, "Barrier synchronization failed!\n");
        return (void*)EXIT_FAILURE;
    }

    /* follow all other clients */
    char client_to_follow[BABBLE_ID_SIZE];
    for(i=0; i< data->nb_clients; i++){
        bzero(client_to_follow, BABBLE_ID_SIZE);
        snprintf(client_to_follow, BABBLE_ID_SIZE, "client_%d", i);
        if(client_follow(sockfd, client_to_follow, with_streaming)){
            fprintf(stderr,"*** Test Failed ***\n");
            fprintf(stderr,"%s failed to follow %s\n", client_name, client_to_follow);
            close(sockfd);
            exit(-1);
        }
    }

    /* synch with server using RDV to be sure all previous msgs have
     * been processed */
    if(client_rdv(sockfd)){
        fprintf(stderr,"*** Test Failed ***\n");
        fprintf(stderr,"%s failed to rdv with server\n", client_name);
        close(sockfd);
        exit(-1);
    }

    /* global barrier before counting followers */
    ret = pthread_barrier_wait(data->gbarrier);
    if (ret != 0 && ret != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        fprintf(stderr, "Barrier synchronization failed!\n");
        return (void*)EXIT_FAILURE;
    }

    int nb_followers = client_follow_count(sockfd);
    if(nb_followers != data->nb_clients){
        fprintf(stderr,"*** Test Failed ***\n");
        fprintf(stderr,"%s has  %d followers\n", client_name, nb_followers);
        close(sockfd);
        exit(-1);
    }

    /* global barrier before starting publishing */
    ret = pthread_barrier_wait(data->gbarrier);
    if (ret != 0 && ret != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        fprintf(stderr, "Barrier synchronization failed!\n");
        return (void*)EXIT_FAILURE;
    }

    /* publishing my k msgs */
    char my_msg[BABBLE_SIZE];
    for(i=0; i< data->nb_msgs; i++){
        bzero(my_msg, BABBLE_SIZE);
        snprintf(my_msg, BABBLE_SIZE, "ping_%d:%d", data->client_id, i);
        if(client_publish(sockfd, my_msg, with_streaming)){
            fprintf(stderr,"*** Test Failed ***\n");
            fprintf(stderr,"%s failed to publish %s\n", client_name, my_msg);
            close(sockfd);
            exit(-1);
        }
    }

    /* synch with server using RDV to be sure all previous msgs have
     * been processed */
    if(client_rdv(sockfd)){
        fprintf(stderr,"*** Test Failed ***\n");
        fprintf(stderr,"%s failed to rdv with server\n", client_name);
        close(sockfd);
        exit(-1);
    }
    
    /* global barrier before asking timelines */
    ret = pthread_barrier_wait(data->gbarrier);
    if (ret != 0 && ret != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        fprintf(stderr, "Barrier synchronization failed!\n");
        return (void*)EXIT_FAILURE;
    }

    int timeline_size = client_timeline(sockfd, 1);
    if(timeline_size != data->nb_clients * data->nb_msgs){
        fprintf(stderr,"*** Test Failed ***\n");
        fprintf(stderr,"%s has %d msgs in timeline (%d expected)\n", client_name, timeline_size, data->nb_clients * data->nb_msgs);
        close(sockfd);
        exit(-1);
    }
    
    close(sockfd);
    return (void*)EXIT_SUCCESS;
}


int main(int argc, char *argv[])
{
    int nb_threads=-1;
    int nb_msgs=-1;

    int opt;
    int nb_args=1;

    pthread_t *tids=NULL;
    client_thread_data_t *clients_data=NULL;

    int i=0;

    pthread_barrier_t global_barrier;
    
    /* parsing command options */
    while ((opt = getopt (argc, argv, "+hm:p:n:k:s")) != -1){
        switch (opt){
        case 'm':
            strncpy(hostname,optarg,BABBLE_BUFFER_SIZE);
            nb_args+=2;
            break;
        case 'p':
            portno = atoi(optarg);
            nb_args+=2;
            break;
        case 'n':
            nb_threads= atoi(optarg);
            nb_args+=2;
            break;
        case 'k':
            nb_msgs= atoi(optarg);
            nb_args+=2;
            break;
        case 's':
            with_streaming=1;
            nb_args+=1;
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

    if( nb_threads == -1 || nb_msgs == -1){
        printf("Error: both number of clients (-n) and number of msgs (-k) have to be specified\n");
        return -1;
    }
    else{
        printf("starting stress test with %d clients sending %d msgs\n", nb_threads, nb_msgs);
    }

    if(pthread_barrier_init(&global_barrier, NULL, nb_threads+1))
    {
        printf("Could not create a barrier\n");
        return -1;
    }
    
    
    tids = malloc(sizeof(pthread_t)*nb_threads);
    clients_data = malloc(sizeof(client_thread_data_t)*nb_threads);
    
    for(i=0; i < nb_threads; i++){
        clients_data[i].gbarrier= &global_barrier;
        clients_data[i].nb_msgs = nb_msgs;
        clients_data[i].nb_clients = nb_threads;
        clients_data[i].client_id = i;
        if(pthread_create (&tids[i], NULL, client_thread, (void*) &clients_data[i]) != 0){
            fprintf(stderr,"WARNING: Failed to create comm thread\n");
        }
    }

    /* barrier after all clients registered*/
    int ret = pthread_barrier_wait(&global_barrier);
    if (ret != 0 && ret != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        fprintf(stderr, "Barrier synchronization failed!\n");
        return -1;
    }

    printf("**** SUCCESS: All clients registered\n");

    /* barrier after all follow command sent*/
    ret = pthread_barrier_wait(&global_barrier);
    if (ret != 0 && ret != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        fprintf(stderr, "Barrier synchronization failed!\n");
        return -1;
    }

    printf("**** SUCCESS: All FOLLOW commands executed\n");
    
    /* barrier after all follow_count*/
    ret = pthread_barrier_wait(&global_barrier);
    if (ret != 0 && ret != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        fprintf(stderr, "Barrier synchronization failed!\n");
        return -1;
    }

    printf("**** SUCCESS: All FOLLOW_COUNT correct\n");

    /* global barrier before asking timelines */
    ret = pthread_barrier_wait(&global_barrier);
    if (ret != 0 && ret != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        fprintf(stderr, "Barrier synchronization failed!\n");
        return -1;
    }

    printf("**** SUCCESS: All PUBLISH executed\n");
        
    
    for(i=0; i < nb_threads; i++){
        pthread_join (tids[i], NULL) ;
    }

    printf("**** SUCCESS: All TIMELINES correct\n");


    /* cleaning */
    free(tids);
    free(clients_data);
    
    return 0;
}
