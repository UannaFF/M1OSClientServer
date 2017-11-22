#ifndef __BABBLE_PUBLICATION_SET_H__
#define __BABBLE_PUBLICATION_SET_H__

#include <time.h>
#include <inttypes.h>

#include "babble_config.h"

/* a publication */
typedef struct publication{
    char msg[BABBLE_SIZE];
    time_t date;
    uint64_t ndate;
    struct publication *next; /* used to create a list */
} publication_t;

/* set implemented as a linked list */
typedef struct publication_set{
    publication_t *first; 
    publication_t *last; /* shortcut for faster insert */
} publication_set_t;

/* instanciate a new set */
publication_set_t* publication_set_create(void);

/* insert a new publication into the set */
publication_t* publication_set_insert(publication_set_t *set, char* msg);

/* get the next publication from the set */
/* The next is the first publication published after date min_date*/
/* last_pub can be used to speed up the search: it should be a
 * reference to the known publication older than min_date but the
 * closest to min_date */
publication_t* publication_set_getnext(publication_set_t *set, publication_t* last_pub, uint64_t min_date);


#endif
