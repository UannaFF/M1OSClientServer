#ifndef __BABBLE_TYPES_H__
#define __BABBLE_TYPES_H__

#include <time.h>

#include "babble_config.h"
#include "babble_publication_set.h"

typedef enum{
    LOGIN =0,
    PUBLISH,
    FOLLOW,
    TIMELINE,
    FOLLOW_COUNT,
    RDV,
    UNREGISTER
} command_id;


typedef struct answer{
    char msg[BABBLE_BUFFER_SIZE];
    struct answer *next;
} answer_t;


typedef struct answer_set{
    answer_t *aset;
    int size;    /* -2 means no msg to send
                    -1 means single msg to send imediately
                    >=0 means msgs to send (with send of the number of
                    messages first) */
} answer_set_t;

typedef struct command{
    command_id cid;
    int sock;    /* only needed by the LOGIN command, other commands
                  * will use the key */
    unsigned long key;
    char msg[BABBLE_SIZE];
    answer_set_t answer; /* once the cmd has been processed, answer
                           * to client is stored there */
    int answer_expected;   /* answer sent only if set */
} command_t;

typedef struct client_bundle{
    unsigned long key;     /* hash of the name */
    char client_name[BABBLE_ID_SIZE];    /* name as provided by the
                                          * client */
    int sock;              /* socket to communicate with this client
                            * */

    publication_set_t *pub_set; /* set of messages published by the
                                * client */
    
    struct client_bundle *followed[MAX_CLIENT];  /* key of the followed
                                                * clients */
    int nb_followed;
    uint64_t last_timeline;   /* stored to display only *new* messages
                               * */
    int nb_followers;
} client_bundle_t;


#endif
