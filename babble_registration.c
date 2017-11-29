#include <stdio.h>
#include <strings.h>
#include <pthread.h>

#include "babble_registration.h"

client_bundle_t *registration_table[MAX_CLIENT];
int nb_registered_clients;
pthread_rwlock_t lock_rw = PTHREAD_RWLOCK_INITIALIZER;

void registration_init(void)
{
    nb_registered_clients=0;

    bzero(registration_table, MAX_CLIENT * sizeof(client_bundle_t*));
}

client_bundle_t* registration_lookup(unsigned long key)
{
    int i=0;

    pthread_rwlock_rdlock(&lock_rw);
    for(i=0; i< nb_registered_clients; i++){
        if(registration_table[i]->key == key){
            client_bundle_t* client = registration_table[i]; //is it good?
            pthread_rwlock_unlock(&lock_rw);
            return client;
        }
    }
    pthread_rwlock_unlock(&lock_rw);
    return NULL;
}

int registration_insert(client_bundle_t* cl)
{    
    pthread_rwlock_rdlock(&lock_rw);
    if(nb_registered_clients == MAX_CLIENT){
        pthread_rwlock_unlock(&lock_rw);
        return -1;
    }
    pthread_rwlock_unlock(&lock_rw);

    
    /* lookup to find if key already exists*/
    client_bundle_t* lp= registration_lookup(cl->key);
    if(lp != NULL){
        fprintf(stderr, "Error -- id % ld already in use\n", cl->key);
        return -1;
    }

    /* insert cl */
    pthread_rwlock_wrlock(&lock_rw);
    if(nb_registered_clients == MAX_CLIENT){
        pthread_rwlock_unlock(&lock_rw);
        return -1;
    }
    registration_table[nb_registered_clients]=cl;
    nb_registered_clients++;
    pthread_rwlock_unlock(&lock_rw);
    return 0;
}


client_bundle_t* registration_remove(unsigned long key)
{
    int i=0;
    pthread_rwlock_wrlock(&lock_rw);
    for(i=0; i<nb_registered_clients; i++){
        if(registration_table[i]->key == key){
            break;
        }
    }

    if(i == nb_registered_clients){
        fprintf(stderr, "Error -- no client found\n");
        return NULL;
    }
    
    
    client_bundle_t* cl= registration_table[i];

    nb_registered_clients--;
    registration_table[i] = registration_table[nb_registered_clients];
    pthread_rwlock_unlock(&lock_rw);
    return cl;
}
