CC =	gcc

CFLAGS =   -g  -Wall 
LDFLAGS =   -lpthread

TARGETS = babble_server.run babble_client.run stress_test.run follow_test.run

# source files the server depends on
SERVER_DEPS= 	babble_utils.c \
		babble_server_implem.c \
		babble_communication.c \
		babble_registration.c \
		babble_publication_set.c

# source files the client depends on
CLIENT_DEPS= 	babble_communication.c  \
		babble_utils.c	\
		babble_client_implem.c


SERVER_DEPS_OBJ= $(patsubst %.c, %.o, $(SERVER_DEPS))
CLIENT_DEPS_OBJ= $(patsubst %.c, %.o, $(CLIENT_DEPS))

DEPS = $(wildcard *.h)

all: $(TARGETS)

babble_server.run: babble_server.o $(SERVER_DEPS_OBJ)
	$(CC) -o $@ $^ $(LDFLAGS)

babble_client.run: babble_client.o $(CLIENT_DEPS_OBJ)
	$(CC) -o $@ $^

%.run: %.o $(CLIENT_DEPS_OBJ)
	$(CC) -o $@ $^ $(LDFLAGS)

%.o: %.c $(DEPS)
	$(CC) -c $< $(CFLAGS)

clean:
	rm -rf *.o *.run *~
