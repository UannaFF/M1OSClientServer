#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "babble_publication_set.h"
#include "babble_server.h"

publication_set_t* publication_set_create(void)
{
    publication_set_t* new_set= malloc(sizeof(publication_set_t));
    new_set->first = NULL;
    new_set->last = NULL;

    return new_set;
}


publication_t* publication_set_insert(publication_set_t *set, char* msg)
{
    struct timespec tt;
    
    publication_t *pub= malloc(sizeof(publication_t));
    
    strncpy(pub->msg, msg, BABBLE_SIZE);
    pub->next=NULL;

    clock_gettime(CLOCK_REALTIME, &tt);
    
    pub->date= tt.tv_sec - server_start;
    pub->ndate = (uint64_t)1000000000 * tt.tv_sec + tt.tv_nsec;
    
    /* inserting the new publication in list */
    if(set->first == NULL){
        set->first = pub;
        set->last = pub;
    }
    else{
        set->last->next = pub;
        set->last = pub;
    }
    
    return pub;
}


publication_t* publication_set_getnext(publication_set_t *set, publication_t* last_pub, uint64_t min_date)
{   
    if(last_pub != NULL){
        if(last_pub->next == NULL || last_pub->next->ndate >= min_date){
            return last_pub->next;
        }
    }
    
    publication_t *item = set->first;
    
    while(item != NULL && item->ndate < min_date){
        item = item -> next;
    }
    

    return item;
}

