#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define PORT 9092

typedef struct Client {
    int fd;
    pthread_t thread;
    struct Client *next;
} Client;

static volatile sig_atomic_t stopping = 0;
static pthread_mutex_t stats_mutex = PTHREAD_MUTEX_INITIALIZER;
static Client *clients = NULL;
static unsigned int active_clients = 0;
static unsigned int max_active = 0;
static unsigned long total_clients = 0;
static unsigned long finished_clients = 0;

static void on_signal(int sig) {
    (void)sig;
    stopping = 1;
}

static void *client_worker(void *arg) {
    Client *client = arg;
    char buffer[4096];

    for (;;) {
        ssize_t count = recv(client->fd, buffer, sizeof(buffer), 0);
        if (count == 0) break;
        if (count < 0) {
            if (errno == EINTR) continue;
            break;
        }

        ssize_t sent = 0;
        while (sent < count) {
            ssize_t n = send(client->fd, buffer + sent, (size_t)(count - sent), 0);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) goto done;
            sent += n;
        }
    }

done:
    pthread_mutex_lock(&stats_mutex);
    if (client->fd >= 0) {
        close(client->fd);
        client->fd = -1;
    }
    active_clients--;
    finished_clients++;
    unsigned int now = active_clients;
    pthread_mutex_unlock(&stats_mutex);

    printf("Client disconnected; active=%u\n", now);
    fflush(stdout);
    return NULL;
}

int main(void) {
    struct sigaction action;
    memset(&action, 0, sizeof(action));
    action.sa_handler = on_signal;
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);
    signal(SIGPIPE, SIG_IGN);

    int server = socket(AF_INET, SOCK_STREAM, 0);
    if (server < 0) {
        perror("socket");
        return 1;
    }

    int reuse = 1;
    setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    struct sockaddr_in address;
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(PORT);

    if (bind(server, (struct sockaddr *)&address, sizeof(address)) < 0 ||
        listen(server, 16) < 0) {
        perror("bind/listen");
        close(server);
        return 1;
    }

    int flags = fcntl(server, F_GETFL, 0);
    if (flags < 0 || fcntl(server, F_SETFL, flags | O_NONBLOCK) < 0) {
        perror("fcntl");
        close(server);
        return 1;
    }

    printf("Server listening on 127.0.0.1:%d\n", PORT);
    fflush(stdout);

    while (!stopping) {
        struct pollfd item = {.fd = server, .events = POLLIN};
        int ready = poll(&item, 1, 250);
        if (ready < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }
        if (ready == 0 || !(item.revents & POLLIN)) continue;

        for (;;) {
            if (stopping) break;
            int fd = accept(server, NULL, NULL);
            if (fd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                if (errno == EINTR) continue;
                perror("accept");
                stopping = 1;
                break;
            }

            Client *client = calloc(1, sizeof(*client));
            if (!client) {
                close(fd);
                continue;
            }
            client->fd = fd;

            pthread_mutex_lock(&stats_mutex);
            active_clients++;
            total_clients++;
            if (active_clients > max_active) max_active = active_clients;
            unsigned int now = active_clients;
            pthread_mutex_unlock(&stats_mutex);

            int status = pthread_create(&client->thread, NULL, client_worker, client);
            if (status != 0) {
                pthread_mutex_lock(&stats_mutex);
                active_clients--;
                total_clients--;
                close(client->fd);
                client->fd = -1;
                pthread_mutex_unlock(&stats_mutex);
                free(client);
                continue;
            }

            client->next = clients;
            clients = client;
            printf("Client connected; active=%u\n", now);
            fflush(stdout);
        }
    }

    close(server);
    pthread_mutex_lock(&stats_mutex);
    for (Client *client = clients; client; client = client->next) {
        if (client->fd >= 0) shutdown(client->fd, SHUT_RDWR);
    }
    pthread_mutex_unlock(&stats_mutex);

    while (clients) {
        Client *next = clients->next;
        pthread_join(clients->thread, NULL);
        free(clients);
        clients = next;
    }

    pthread_mutex_lock(&stats_mutex);
    printf("Shutdown: active=%u, total=%lu, finished=%lu, peak=%u\n",
           active_clients, total_clients, finished_clients, max_active);
    pthread_mutex_unlock(&stats_mutex);
    pthread_mutex_destroy(&stats_mutex);
    return 0;
}
