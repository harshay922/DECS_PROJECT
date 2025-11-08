// kv-server.c
// Usage: ./kv-server <bind-ip> <port>
// Example: ./kv-server 0.0.0.0 5000
// Single-client at a time. KV persists in memory across clients.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

typedef struct KVNode {
    int key;
    size_t len;
    char *val;            // may contain spaces / arbitrary bytes (no NUL guarantee)
    struct KVNode *next;
} KVNode;

static KVNode *kv_head = NULL;

/* ---------- KV store helpers ---------- */
static KVNode* kv_find(int key) {
    for (KVNode *p = kv_head; p; p = p->next) // JAB TAK NULL VALUE NHI MILTA means last kv node nhi milta tab tak chlne do
    {
        if (p->key == key) return p;
    }
    return NULL;
}

static int kv_create(int key, const char *buf, size_t len) {
    if (kv_find(key)) return -1; // exists , mtlb wo key already exist krta h
    KVNode *n = (KVNode*)malloc(sizeof(KVNode));
    if (!n) return -2;
    n->val = (char*)malloc(len);
    if (!n->val) { free(n); return -2; }
    memcpy(n->val, buf, len);
    n->len = len;
    n->key = key;
    n->next = kv_head;// like a add node in front in linked list??
    kv_head = n; // naya head ban gaya
    return 0;
}

static int kv_update(int key, const char *buf, size_t len) {
    KVNode *n = kv_find(key);
    if (!n) return -1; // not found in the store return -1
    char *nv = (char*)malloc(len);
    if (!nv) return -2;
    memcpy(nv, buf, len);
    free(n->val);// purani value free krdi dynamic memory ki
    n->val = nv;
    n->len = len;
    return 0;
}

static int kv_delete(int key) {
    KVNode *prev = NULL, *cur = kv_head;
    while (cur) {
        if (cur->key == key) {
            if (prev) prev->next = cur->next;
            else kv_head = cur->next;
            free(cur->val);// purane value ko free krdo
            free(cur); // node ko free krdo dynamic memory ki
            return 0;
        }
        prev = cur;
        cur = cur->next;
    }
    return -1;
}

/* ---------- I/O helpers ---------- */

// Read exactly n bytes (or 0 if peer closed)
static ssize_t read_n(int fd, void *buf, size_t n) {
    size_t left = n;
    char *p = (char*)buf;
    while (left > 0) {
        ssize_t r = read(fd, p, left);
        if (r == 0) return (n - left);     // peer closed early
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        left -= (size_t)r;
        p += r;
    }
    return (ssize_t)n;
}

// Write exactly n bytes
static int write_n(int fd, const void *buf, size_t n) {
    size_t left = n;
    const char *p = (const char*)buf;
    while (left > 0) {
        ssize_t w = write(fd, p, left);
        if (w < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        left -= (size_t)w;
        p += w;
    }
    return 0;
}

// Read a line terminated by '\n' (up to maxlen-1), returns length (excludes '\n') or -1 on error, 0 on EOF.
static ssize_t read_line(int fd, char *buf, size_t maxlen) {
    size_t pos = 0;
    while (pos + 1 < maxlen) {
        char c;
        ssize_t r = read(fd, &c, 1);
        if (r == 0) return (pos == 0) ? 0 : (ssize_t)pos; // EOF
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (c == '\n') {
            buf[pos] = '\0';
            return (ssize_t)pos;
        }
        buf[pos++] = c;
    }
    // line too long -> truncate
    buf[pos] = '\0';
    return (ssize_t)pos;
}

// Trim trailing CR if present (for CRLF clients)
static void rtrim_cr(char *s) {
    size_t n = strlen(s);
    if (n > 0 && s[n-1] == '\r') s[n-1] = '\0';
}

/* ---------- Command handling ---------- */

static int handle_client(int cfd) {
    char line[4096];

    for (;;) {
        ssize_t ln = read_line(cfd, line, sizeof(line));
        if (ln == 0) return 0;         // client closed
        if (ln < 0) return -1;
        rtrim_cr(line);

        // Parse command
        // Commands: CREATE key size | READ key | UPDATE key size | DELETE key
        char cmd[16];
        int key;
        size_t size = 0;

        // Try forms with size first
        if (sscanf(line, "%15s %d %zu", cmd, &key, &size) >= 2) {
            // Normalize to uppercase-ish compare
            for (char *p = cmd; *p; ++p) if (*p >= 'a' && *p <= 'z') *p = (char)(*p - 'a' + 'A');

            if (strcmp(cmd, "CREATE") == 0 || strcmp(cmd, "UPDATE") == 0) {
                if (size == 0 && strcmp(cmd, "UPDATE") == 0) {
                    const char *em = "ERR size must be > 0\n";
                    write_n(cfd, em, strlen(em));
                    continue;
                }
                // Read value bytes
                char *val = NULL;
                if (size > 0) {
                    val = (char*)malloc(size);
                    if (!val) {
                        const char *em = "ERR out of memory\n";
                        write_n(cfd, em, strlen(em));
                        continue;
                    }
                    ssize_t rr = read_n(cfd, val, size);
                    if (rr != (ssize_t)size) {
                        free(val);
                        const char *em = "ERR premature EOF on value\n";
                        write_n(cfd, em, strlen(em));
                        return -1;
                    }
                }

                int rc = (strcmp(cmd, "CREATE") == 0)
                         ? kv_create(key, val, size)
                         : kv_update(key, val, size);

                free(val);

                if (rc == 0) {
                    const char *ok = "OK\n";
                    write_n(cfd, ok, strlen(ok));
                } else if (rc == -1) {
                    const char *em = (strcmp(cmd, "CREATE") == 0)
                                     ? "ERR key exists\n" : "ERR no such key\n";
                    write_n(cfd, em, strlen(em));
                } else {
                    const char *em = "ERR internal error\n";
                    write_n(cfd, em, strlen(em));
                }
                continue;
            }

            if (strcmp(cmd, "READ") == 0) {
                KVNode *n = kv_find(key);
                if (!n) {
                    const char *em = "ERR no such key\n";
                    write_n(cfd, em, strlen(em));
                } else {
                    char hdr[64];
                    int hl = snprintf(hdr, sizeof(hdr), "OK %zu\n", n->len);
                    if (write_n(cfd, hdr, (size_t)hl) < 0 ||
                        write_n(cfd, n->val, n->len) < 0) {
                        return -1;
                    }
                }
                continue;
            }

            if (strcmp(cmd, "DELETE") == 0) {
                int rc = kv_delete(key);
                if (rc == 0) {
                    const char *ok = "OK\n";
                    write_n(cfd, ok, strlen(ok));
                } else {
                    const char *em = "ERR no such key\n";
                    write_n(cfd, em, strlen(em));
                }
                continue;
            }

            // Unknown command with 2-3 tokens
            const char *em = "ERR unknown command\n";
            write_n(cfd, em, strlen(em));
        } else {
            // Could be malformed/empty
            if (ln == 0) return 0;
            const char *em = "ERR malformed command\n";
            write_n(cfd, em, strlen(em));
        }
    }
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <bind-ip> <port>\n", argv[0]);
        return 1;
    }

    const char *bind_ip = argv[1];
    int port = atoi(argv[2]);

    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sfd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, bind_ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "Invalid bind IP: %s\n", bind_ip);// Invlalid IP address
        close(sfd);
        return 1;
    }
    addr.sin_port = htons((uint16_t)port);

    if (bind(sfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");// bind this to socket is failed
        close(sfd);
        return 1;
    }

    if (listen(sfd, 5) < 0) {
        perror("listen");
        close(sfd);
        return 1;
    }

    fprintf(stdout, "KV server listening on %s:%d\n", bind_ip, port);

    for (;;) {
        struct sockaddr_in cli;
        socklen_t clen = sizeof(cli);
        int cfd = accept(sfd, (struct sockaddr*)&cli, &clen);
        // accept connection from client   
        if (cfd < 0) {
            perror("accept");
            continue;
        }
        char ipstr[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &cli.sin_addr, ipstr, sizeof(ipstr));
        fprintf(stdout, "Client connected from %s:%d\n", ipstr, ntohs(cli.sin_port));

        // Handle one client to completion (single-client at a time)
        handle_client(cfd);
        close(cfd);
        fprintf(stdout, "Client disconnected.\n");
    }
}
