#ifndef __BABBLE_COMMUNICATION_H__
#define __BABBLE_COMMUNICATION_H__


/**** Implementation of the communication protocol ****/

/* the implemented protocol is the following:
    + each packet starts with a header that includes the size of the
    payload to be sent in bytes
    + the recv function allocates a buffer to store the payload
    + It is the user duty to free the allocated buffers
    + Each function returns the number of data bytes sent/received
*/

/* send the buffer buf of size "size" using the file descriptor fd */
int network_send(int fd, unsigned long size, void* buf);

/* recv data from the file descriptor fd */
/* a buffer is allocated to store the data, its size is returned */
int network_recv(int fd, void **buf);


#endif
