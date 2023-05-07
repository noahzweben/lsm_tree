#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include "lsm.h"

#define PORT 8080
#define MAX_PENDING_CONNECTIONS 10
#define NODE_NUM 512
#define BUFFER_SIZE 1024

typedef struct
{
    int client_socket;
    lsmtree *lsm;
} thread_args_t;

void *handle_connection(void *thread_args)
{
    thread_args_t *args = (thread_args_t *)thread_args;

    int client_socket = args->client_socket;
    lsmtree *lsm = args->lsm;
    ssize_t bytes_received;

    // Receive the message from the client
    while (1)
    {
        char buffer[BUFFER_SIZE];
        bytes_received = recv(client_socket, buffer, BUFFER_SIZE - 1, 0);
        if (bytes_received == 0)
        {
            break;
        }

        if (bytes_received < 0)
        {
            perror("recv");
            close(client_socket);
            return NULL;
        }

        // Print the client's message
        buffer[bytes_received] = '\0';
        // printf("Client message: %s\n", buffer);

        char command;
        int x, y;

        if (sscanf(buffer, "%c %d %d", &command, &x, &y) >= 2)
        {
            switch (command)
            {
            case 'g':
            {
                // convert result to string
                char result_str[20];
                int result = get(lsm, x);
                sprintf(result_str, "%d", result);

                if (send(client_socket, result_str, strlen(result_str), 0) < 0)
                {
                    perror("send");
                }
                break;
            }
            case 'p':
            {
                insert(lsm, x, y);
                if (send(client_socket, "", 0, 0) < 0)
                {
                    perror("send");
                }
                break;
            }
            case 'd':
            {
                delete (lsm, x);
                if (send(client_socket, "", 0, 0) < 0)
                {
                    perror("send");
                }
                break;
            }
            case 'r':
            {
                int n_results = 0;
                node *results = NULL;
                range(lsm, &results, &n_results, x, y);
                // for each node send key:value to client if not deleted
                for (int i = 0; i < n_results; i++)
                {

                    node n = results[i];
                    if (!n.delete)
                    {
                        char result_str[60];
                        sprintf(result_str, "%d:%d ", n.key, n.value);
                        if (send(client_socket, result_str, strlen(result_str), 0) < 0)
                        {
                            perror("send");
                        }
                    }
                }

                break;
            }
            default:
            {
                printf("Invalid command: %c\n", command);
                break;
            }
            }

            if (send(client_socket, "\n", 1, 0) < 0)
            {
                perror("send");
            }
        }
        else
        {
            printf("Invalid input format\n");
        }
    }

    // Close the connection
    close(client_socket);
    print_lol();
    printf("Connection closed\n");
    return NULL;
}

int main()
{
    // create an lsm tree
    lsmtree *lsm = create(NODE_NUM);

    int server_socket, client_socket;
    struct sockaddr_in server_address, client_address;
    socklen_t client_address_size;

    // Create a server socket
    server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0)
    {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Configure the server address
    memset(&server_address, 0, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_addr.s_addr = INADDR_ANY;
    server_address.sin_port = htons(PORT);

    // Bind the server socket to the server address
    if (bind(server_socket, (struct sockaddr *)&server_address, sizeof(server_address)) < 0)
    {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    // Listen for incoming connections
    if (listen(server_socket, MAX_PENDING_CONNECTIONS) < 0)
    {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    printf("Server listening on port %d...\n", PORT);

    while (1)
    {
        // Accept an incoming connection
        client_address_size = sizeof(client_address);
        client_socket = accept(server_socket, (struct sockaddr *)&client_address, &client_address_size);
        if (client_socket < 0)
        {
            perror("accept");
            exit(EXIT_FAILURE);
        }

        printf("Connection accepted from %s:%d\n",
               inet_ntoa(client_address.sin_addr),
               ntohs(client_address.sin_port));

        // Create a new thread for handling the connection
        pthread_t client_thread;

        // pass client_socket_ptr and the lsm to handle_connection
        // so that it can use them
        thread_args_t args;
        args.client_socket = client_socket;
        args.lsm = lsm;

        if (pthread_create(&client_thread, NULL, handle_connection, (void *)&args) != 0)
        {
            perror("pthread_create");
            exit(EXIT_FAILURE);
        }

        // Detach the thread so that its resources are automatically released upon completion
        pthread_detach(client_thread);
    }

    // Close the server socket
    close(server_socket);
    // free the lsm tree
    destroy(lsm);

    return 0;
}