#ifndef __BABBLE_UTILS_H__
#define __BABBLE_UTILS_H__

/* djb2 hash function */
unsigned long hash(char *str);

/* truncate input string at first line feed (\n), and remove \n */
void str_clean(char* str);

/* convert input string to babble command id*/
int str_to_command(char* str, int* ack_req);

/* copy payload of input into output (copy at most size characters) */
int str_to_payload(char* input, char* output, int size);

/* extract key from login ack */
unsigned long parse_login_ack(char* ack_msg);

/* extract nb of followers from follow_count msg */
int parse_fcount_ack(char* ack);


#endif
