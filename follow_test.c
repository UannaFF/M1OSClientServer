#include <stdio.h>
#include <pthread.h>
#include <strings.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>


#include "babble_types.h"
#include "babble_communication.h"
#include "babble_utils.h"
#include "babble_client.h"


int nb_timeline = 10;

char hostname[BABBLE_BUFFER_SIZE]="127.0.0.1";
int portno = BABBLE_PORT;

int with_streaming = 0;
int publish_count = 0;

int keep_on_going = 1;
pthread_barrier_t global_barrier;


static void display_help(char *exec)
{
    printf("Usage: %s -m hostname -p port_number -t nb_timeline_requests -s [activate_streaming]\n", exec);
    printf("\t hostname can be an ip address\n" );
}

static void *publish_thread (void *arg)
{
    char client_name[BABBLE_ID_SIZE];
    bzero(client_name, BABBLE_ID_SIZE);
    snprintf(client_name, BABBLE_ID_SIZE, "PUB");
    
    int sockfd = connect_to_server(hostname, portno);

    if(sockfd == -1){
        fprintf(stderr,"*** Test Failed ***\n");
        fprintf(stderr,"PUB failed to contact server\n");
        exit(-1);
    }

    unsigned long client_key= client_login(sockfd, client_name);
    
    if(client_key == 0){
        fprintf(stderr,"*** Test Failed ***\n");
        fprintf(stderr,"PUB failed to login\n");
        close(sockfd);
        exit(-1);
    }

    /* global barrier before starting to follow others */
    int ret = pthread_barrier_wait(&global_barrier);
    if (ret != 0 && ret != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        fprintf(stderr, "Barrier synchronization failed!\n");
        return (void*)EXIT_FAILURE;
    }


    /* continuously publishing msgs */
    char my_msg[BABBLE_SIZE];

    while(!__sync_bool_compare_and_swap(&keep_on_going, 0, 2)){
        bzero(my_msg, BABBLE_SIZE);
        snprintf(my_msg, BABBLE_SIZE, "msg_%d", publish_count);
        if(client_publish(sockfd, my_msg, with_streaming)){
            fprintf(stderr,"*** Test Failed ***\n");
            fprintf(stderr,"failed to publish %s\n", my_msg);
            close(sockfd);
            exit(-1);
        }
        publish_count++;
    }


    printf("PUB stops publishing\n");
    
    
    /* synch with server */
    if(client_rdv(sockfd)){
        fprintf(stderr,"*** Test Failed ***\n");
        fprintf(stderr,"PUB failed to rdv with server\n");
        close(sockfd);
        exit(-1);
    }

    
    /* barrier before the last timeline */
    ret = pthread_barrier_wait(&global_barrier);
    if (ret != 0 && ret != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        fprintf(stderr, "Barrier synchronization failed!\n");
        return (void*)EXIT_FAILURE;
    }
    
    close(sockfd);
    return (void*)EXIT_SUCCESS;
}

static void *follow_thread (void *arg)
{
    char client_name[BABBLE_ID_SIZE];
    bzero(client_name, BABBLE_ID_SIZE);
    snprintf(client_name, BABBLE_ID_SIZE, "TIM");
    
    int sockfd = connect_to_server(hostname, portno);

    int timeline_full_size = 0;

    if(sockfd == -1){
        fprintf(stderr,"*** Test Failed ***\n");
        fprintf(stderr,"TIM failed to contact server\n");
        exit(-1);
    }

    unsigned long client_key= client_login(sockfd, client_name);
    
    if(client_key == 0){
        fprintf(stderr,"*** Test Failed ***\n");
        fprintf(stderr,"TIM failed to login\n");
        close(sockfd);
        exit(-1);
    }

    /* global barrier before starting to follow others */
    int ret = pthread_barrier_wait(&global_barrier);
    if (ret != 0 && ret != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        fprintf(stderr, "Barrier synchronization failed!\n");
        return (void*)EXIT_FAILURE;
    }

    /* following PUB */
    char client_to_follow[BABBLE_ID_SIZE];
    bzero(client_to_follow, BABBLE_ID_SIZE);
    snprintf(client_to_follow, BABBLE_ID_SIZE, "PUB");
    if(client_follow(sockfd, client_to_follow, with_streaming)){
        fprintf(stderr,"*** Test Failed ***\n");
        fprintf(stderr,"failed to follow %s\n", client_to_follow);
        close(sockfd);
        exit(-1);
    }


    /* timeline requests */
    int i=0;
    int timeline_size=0;
    for(i=0; i< nb_timeline; i++){
        timeline_size = client_timeline(sockfd, 0);
        if(timeline_size == -1){
            fprintf(stderr,"*** Test Failed ***\n");
            fprintf(stderr,"pb in timeline\n");
            close(sockfd);
            exit(-1);
        }
        else{
            printf("%d: TIM got a timeline of size %d\n", i, timeline_size);
            timeline_full_size += timeline_size;
        }
    }

    /* stop the publisher thread */
    __sync_bool_compare_and_swap(&keep_on_going, 1, 0);

    /* barrier before the last timeline */
    ret = pthread_barrier_wait(&global_barrier);
    if (ret != 0 && ret != PTHREAD_BARRIER_SERIAL_THREAD)
    {
        fprintf(stderr, "Barrier synchronization failed!\n");
        return (void*)EXIT_FAILURE;
    }

    /* last timeline */
    timeline_size = client_timeline(sockfd, 0);
    if(timeline_size == -1){
        fprintf(stderr,"*** Test Failed ***\n");
        fprintf(stderr,"pb in timeline\n");
        close(sockfd);
        exit(-1);
    }
    else{
        printf("%d: TIM got a timeline of size %d\n", i, timeline_size);
        timeline_full_size += timeline_size;
    }
    
    if(timeline_full_size != publish_count){
        fprintf(stderr,"*** Test Failed ***\n");
        fprintf(stderr,"timeline includes only %d / %d msgs\n", timeline_full_size, publish_count);
        close(sockfd);
        exit(-1);
    }
    
    close(sockfd);
    return (void*)EXIT_SUCCESS;
}





int main(int argc, char *argv[])
{
    int opt;
    int nb_args=1;

    pthread_t tid;
    
    /* parsing command options */
    while ((opt = getopt (argc, argv, "+hm:p:t:s")) != -1){
        switch (opt){
        case 'm':
            strncpy(hostname,optarg,BABBLE_BUFFER_SIZE);
            nb_args+=2;
            break;
        case 'p':
            portno = atoi(optarg);
            nb_args+=2;
            break;
        case 't':
            nb_timeline= atoi(optarg);
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

    
    if(pthread_barrier_init(&global_barrier, NULL, 2))
    {
        printf("Could not create a barrier\n");
        return -1;
    }

    /* creating the publisher thread */
    if(pthread_create (&tid, NULL, publish_thread, NULL) != 0){
        fprintf(stderr,"WARNING: Failed to create comm thread\n");
    }

    /* creating the follower thread */
    if(pthread_create (&tid, NULL, follow_thread, NULL) != 0){
        fprintf(stderr,"WARNING: Failed to create comm thread\n");
    }

    /* wait for the follow thread to terminate */
    pthread_join (tid, NULL) ;

    printf("**** SUCCESS ****\n");
    
    return 0;
}
