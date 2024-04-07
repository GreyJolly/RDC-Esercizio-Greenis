#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>

#define MAX_MESSAGE_SIZE 4096
#define MAX_STRING_SIZE 2048
#define PORT_NUMBER 7379
#define MAX_QUEUE 16
#define NOT_EXPIRING -1

// I will be storing data on a disordered linked list.
// This is very optimizable

// LIST METHODS

struct node
{
	char *key;
	char *value;
	int timestamp, expiry;
	struct node *next;
};

typedef struct node node;

struct thread_args
{
	int client_socket;
	struct sockaddr_in client_addr;
};

// Adds new head to list and returns it
node *addToList(node *head, char *key, char *value, int expiry)
{
	node *newNode = malloc(sizeof(node));
	newNode->key = malloc(sizeof(key));
	newNode->value = malloc(sizeof(value));
	strcpy(newNode->key, key);
	strcpy(newNode->value, value);
	newNode->timestamp = time(NULL);
	newNode->expiry = expiry;
	if (head != NULL)
	{
		newNode->next = head;
	}
	return newNode;
}

node *findKeyInList(node *n, char *key)
{
	if (n == NULL)
		return NULL; // Returns NULL if the key is not found
	if (strcmp(n->key, key) == 0)
		return n;
	return findKeyInList(n->next, key);
}

node *removeOldNodes(node *n)
{
	int currentTime = time(NULL);
	node *next, *head = n;
	if (head == NULL)
		return NULL;

	// We remove all nodes but the head
	while (n != NULL)
	{
		if (n->next != NULL)
		{
			next = n->next;

			if (next->expiry != -1 && currentTime > next->timestamp + next->expiry)
			{
				n->next = next->next;
				free(next->key);
				free(next->value);
				free(next);
			}
		}
		n = n->next;
	}

	// We remove the head if it is too old
	if (head->expiry != -1 && currentTime > head->timestamp + head->expiry)
	{
		next = head->next;
		free(head->key);
		free(head->value);
		free(head);
		head = next;
	}
	return head;
}

void printList(node *n)
{
	printf("[");
	while (n != NULL)
	{
		printf("%s, ", n->key);
		n = n->next;
	}
	printf("]\n");
}

// HELPER FUNCTIONS

void exit_with_error(const char *msg)
{
	perror(msg);
	exit(EXIT_FAILURE);
}

// GLOBAL VARIABLES
node *head = NULL;
sem_t semaphore;

// MAIN FUNCTIONS

void *connection_handler(void *args)
{
	int client_socket = ((struct thread_args *)args)->client_socket;
	struct sockaddr_in client_addr = ((struct thread_args *)args)->client_addr;
	free(args);

	char received_message[MAX_MESSAGE_SIZE], *parsed_token;
	char key_read[MAX_STRING_SIZE], value_read[MAX_STRING_SIZE];
	int expiry_read = NOT_EXPIRING;
	char set_detected = 0, timed_set_detected = 0, get_detected = 0, key_acquired = 0, skip = 0;
	int ret;
	node *found;

	while (1)
	{
		memset(received_message, 0, sizeof(received_message));
		ret = read(client_socket, received_message, MAX_MESSAGE_SIZE - 1);
		if (ret < 0)
			exit_with_error("Read error");
		if (ret == 0)
			pthread_exit(EXIT_SUCCESS);

		// Parse message and handle
		// The message parsing in this program is not very extendable, but I'm not sure how an actual implementation would behave
		parsed_token = strtok(received_message, "\r\n");
		for (; parsed_token != NULL; parsed_token = strtok(NULL, "\r\n"))
		{
			if (skip)
			{
				skip = 0;
				continue;
			}
			// Crutch to avoid dealing with setup
			if (parsed_token[0] == '*')
			{
				if (parsed_token[1] == '4')
				{
					ret = write(client_socket, "+OK\r\n", sizeof("+OK\r\n") - 1);
					if (ret < 0)
						exit_with_error("Write error");
					break;
				}
			}
			// Variuous parsing crutches
			if (strcmp(parsed_token, "SET") == 0)
			{
				set_detected = 1;
				skip = 1;
				continue;
			}
			if (strcmp(parsed_token, "GET") == 0)
			{
				get_detected = 1;
				skip = 1;
				continue;
			}
			if (strcmp(parsed_token, "EX") == 0)
			{
				timed_set_detected = 1;
				skip = 1;
				continue;
			}
			if (timed_set_detected)
			{
				expiry_read = atoi(parsed_token);
				continue;
			}
			if ((set_detected || get_detected) && !key_acquired)
			{
				strcpy(key_read, parsed_token);
				key_acquired = 1;
				skip = 1;
				continue;
			}
			if (set_detected && key_acquired)
			{
				strcpy(value_read, parsed_token);
				skip = 1;
				continue;
			}
		}
		// Handle SET
		if (set_detected)
		{

			printf("Handling set with key: %s, value: %s, expiry: %d\n", key_read, value_read, expiry_read);
			fflush(stdout);

			sem_wait(&semaphore);
			found = findKeyInList(head, key_read);

			if (found != NULL)
			{
				found->value = value_read;
				found->timestamp = time(NULL);
				found->expiry = expiry_read;
			}
			else
			{
				head = addToList(head, key_read, value_read, expiry_read);
			}
			sem_post(&semaphore);

			ret = write(client_socket, "+OK\r\n", sizeof("+OK\r\n"));
			if (ret < 0)
				exit_with_error("Write error");
		}
		// Handle GET
		if (get_detected)
		{
			printf("Handling get with key: %s\n", key_read);
			fflush(stdout);

			sem_wait(&semaphore);

			head = removeOldNodes(head);
			found = findKeyInList(head, key_read);

			sem_post(&semaphore);

			char answer[MAX_MESSAGE_SIZE];
			if (found == NULL)
				strcpy(answer, "$-1\r\n");
			else
				sprintf(answer, "$%ld\r\n%s\r\n", strlen(found->value), found->value);

			ret = write(client_socket, answer, strlen(answer));
			if (ret < 0)
				exit_with_error("Write error");
		}

		set_detected = 0;
		timed_set_detected = 0;
		expiry_read = NOT_EXPIRING;
		get_detected = 0;
		key_acquired = 0;
		skip = 0;
	}

	// We should never get here
	pthread_exit(EXIT_SUCCESS);
}

int main(int argc, const char *argv[])
{

	// Open Socket

	int ret, listening_socket, client_socket, client_length, reuseaddr = 1;
	struct sockaddr_in server_addr, client_addr;
	client_length = sizeof(client_addr);

	listening_socket = socket(AF_INET, SOCK_STREAM, 0);
	if (listening_socket < 0)
		exit_with_error("Cannot open socket");

	ret = setsockopt(listening_socket, SOL_SOCKET, SO_REUSEADDR, &reuseaddr, sizeof(reuseaddr));
	if (ret < 0)
		exit_with_error("Cannot set socket options");

	memset(&server_addr, 0, sizeof(server_addr));
	server_addr.sin_family = AF_INET;

	server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	server_addr.sin_port = htons(PORT_NUMBER);

	ret = bind(listening_socket, (struct sockaddr *)&server_addr, sizeof(server_addr));
	if (ret < 0)
		exit_with_error("Cannot bind");

	listen(listening_socket, MAX_QUEUE);

	printf("Server started\n");
	fflush(stdout);

	// Open semaphore
	ret = sem_init(&semaphore, 1, 1);

	while (1)
	{
		client_socket = accept(listening_socket, (struct sockaddr *)&client_addr, &client_length);
		if (client_socket < 0)
			exit_with_error("Cannot accept");

		struct thread_args *args = malloc(sizeof(struct thread_args));
		args->client_socket = client_socket;
		args->client_addr = client_addr;
		pthread_t thread;

		ret = pthread_create(&thread, NULL, connection_handler, (void *)args);
	}
}
