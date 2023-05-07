#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define SERVER_IP "127.0.0.1"
#define PORT 8080
#define BUFFER_SIZE 1024

void communicate_with_server(int client_socket, char *filename)
{
    char buffer[BUFFER_SIZE];
    ssize_t bytes_received;

    // open file
    FILE *fp = fopen(filename, "r");
    if (fp == NULL)
    {
        perror("fopen");
        exit(EXIT_FAILURE);
    }

    // loop throuhg file and send each line to server
    while (fgets(buffer, BUFFER_SIZE, fp) != NULL)
    {
        // Send a text message to the server
        if (send(client_socket, buffer, strlen(buffer), 0) < 0)
        {
            perror("send");
            exit(EXIT_FAILURE);
        }

        // keep receiving response from server until the server sends a newline char
        while (1)
        {
            char recv_buffer[BUFFER_SIZE];
            // Receive the response from the server
            bytes_received = recv(client_socket, recv_buffer, BUFFER_SIZE - 1, 0);
            if (bytes_received < 0)
            {
                perror("recv");
                exit(EXIT_FAILURE);
            }

            // Print the server's response
            recv_buffer[bytes_received] = '\0';
            // printf("%s", recv_buffer);

            // if the server sends a newline char, break out of the loop
            if (recv_buffer[bytes_received - 1] == '\n')
            {
                printf("\n");
                break;
            }
        }
    }
    // close the connection and file
    close(client_socket);
    fclose(fp);
}

int main(int argc, char *argv[])
{
    // get filename from argv
    if (argc != 2)
    {
        printf("Usage: %s <filename>\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    char *filename = argv[1];

    int client_socket;
    struct sockaddr_in server_address;

    // Create a client socket
    client_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (client_socket < 0)
    {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    // Configure the server address
    memset(&server_address, 0, sizeof(server_address));
    server_address.sin_family = AF_INET;
    server_address.sin_port = htons(PORT);
    if (inet_pton(AF_INET, SERVER_IP, &server_address.sin_addr) <= 0)
    {
        perror("invalid server IP address");
        exit(EXIT_FAILURE);
    }

    // Connect to the server
    if (connect(client_socket, (struct sockaddr *)&server_address, sizeof(server_address)) < 0)
    {
        perror("connect");
        exit(EXIT_FAILURE);
    }

    printf("Connected to server %s:%d\n", SERVER_IP, PORT);

    // Communicate with the server
    communicate_with_server(client_socket, filename);

    // Close the client socket
    close(client_socket);

    return 0;
}