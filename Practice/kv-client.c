// kv-client.c
// Usage:
//   Interactive: ./kv-client interactive
//   Batch:       ./kv-client batch <commands.txt>
//
// Commands (typed by user or in the batch file):
//   connect <server-ip> <server-port>
//   disconnect
//   create <key> <value-size> <value-with-spaces-allowed>
//   read <key>
//   update <key> <value-size> <value-with-spaces-allowed>
//   delete <key>
//   help
//   quit | exit
//
// NOTE: <value-size> must exactly match the number of bytes in <value> (ASCII).

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define MAXLINE 8192

static int conn_fd = -1;

/* ------------- TCP helpers ------------- */
static int connect_to(const char *host, int port) {
    if (conn_fd != -1) return -2; // already connected

    struct addrinfo hints, *res = NULL, *rp = NULL;
    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;      // IPv4 for simplicity
    hints.ai_socktype = SOCK_STREAM;

    int e = getaddrinfo(host, portstr, &hints, &res);
    if (e != 0) return -3;

    int fd = -1;
    for (rp = res; rp != NULL; rp = rp->ai_next) {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd == -1) continue;
        if (connect(fd, rp->ai_addr, rp->ai_addrlen) == 0) break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    if (fd == -1) return -4;

    conn_fd = fd;
    return 0;
}

static void disconnect_now(void) {
    if (conn_fd != -1) {
        close(conn_fd);
        conn_fd = -1;
    }
}

// Write all
static int write_n(int fd, const void *buf, size_t n) {
    const char *p = (const char*)buf;
    size_t left = n;
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

// Read exactly n
static ssize_t read_n(int fd, void *buf, size_t n) {
    char *p = (char*)buf;
    size_t left = n;
    while (left > 0) {
        ssize_t r = read(fd, p, left);
        if (r == 0) return (ssize_t)(n - left);
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        left -= (size_t)r;
        p += r;
    }
    return (ssize_t)n;
}

// Read line until '\n', return length (excluding '\n'), 0 on EOF, -1 on error
static ssize_t read_line(int fd, char *buf, size_t maxlen) {
    size_t pos = 0;
    while (pos + 1 < maxlen) {
        char c;
        ssize_t r = read(fd, &c, 1);
        if (r == 0) return (pos == 0) ? 0 : (ssize_t)pos;
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
    buf[pos] = '\0';
    return (ssize_t)pos;
}

static void rtrim_cr(char *s) {
    size_t n = strlen(s);
    if (n > 0 && s[n-1] == '\r') s[n-1] = '\0';
}

/* ------------- Client command helpers ------------- */

static void to_lower(char *s) { for (; *s; ++s) *s = (char)tolower((unsigned char)*s); }

// Send header + optional value to server
static int send_cmd_with_optional_value(const char *header, const char *value, size_t vlen) {
    if (conn_fd == -1) {
        fprintf(stderr, "ERROR: not connected\n");
        return -1;
    }
    if (write_n(conn_fd, header, strlen(header)) < 0) {
        perror("write");
        return -1;
    }
    if (vlen > 0 && value != NULL) {
        if (write_n(conn_fd, value, vlen) < 0) {
            perror("write");
            return -1;
        }
    }
    return 0;
}

static int recv_status_and_optional_value(void) {
    if (conn_fd == -1) {
        fprintf(stderr, "ERROR: not connected\n");
        return -1;
    }
    char line[MAXLINE];
    ssize_t ln = read_line(conn_fd, line, sizeof(line));
    if (ln <= 0) { fprintf(stderr, "ERROR: server closed or read error\n"); return -1; }
    rtrim_cr(line);

    if (strncmp(line, "OK", 2) == 0) {
        // Maybe "OK size"
        size_t sz = 0;
        if (sscanf(line, "OK %zu", &sz) == 1) {
            char *buf = (char*)malloc(sz + 1);
            if (!buf) { fprintf(stderr, "ERROR: OOM\n"); return -1; }
            ssize_t rr = read_n(conn_fd, buf, sz);
            if (rr != (ssize_t)sz) {
                fprintf(stderr, "ERROR: truncated value from server\n");
                free(buf);
                return -1;
            }
            // Print as a string (not necessarily NUL terminated data)
            buf[sz] = '\0';
            // For terminal safety, we print raw bytes; but since assignment says "strings", printing as text is fine:
            printf("%s\n", buf);
            free(buf);
        } else {
            printf("OK\n");
        }
        return 0;
    } else if (strncmp(line, "ERR", 3) == 0) {
        printf("%s\n", line);
        return -1;
    } else {
        printf("ERR unexpected response: %s\n", line);
        return -1;
    }
}

// Parse "create/update key size <value...>" into key, size, value_ptr
static int parse_create_or_update(char *line, int *out_key, size_t *out_size, char **out_value) {
    // line includes command; find the first three tokens then the remainder is value (may contain spaces)
    // We will copy the remainder directly; the reported size must match strlen(remainder)
    char cmd[16]; long key; size_t sz;
    // First consume cmd, key, size
    int n = 0;
    {
        char *save = NULL;
        char *p = strtok_r(line, " \t\r\n", &save); if (!p) return -1;
        strncpy(cmd, p, sizeof(cmd)-1); cmd[sizeof(cmd)-1] = '\0';
        p = strtok_r(NULL, " \t\r\n", &save); if (!p) return -1;
        key = strtol(p, NULL, 10);
        p = strtok_r(NULL, " \t\r\n", &save); if (!p) return -1;
        sz = (size_t)strtoull(p, NULL, 10);

        // Now, get pointer into original line beyond the first three tokens.
        // Easiest: find substring of the third token and step past it in the original buffer captured before tokenization.
        // But since we destructively tokenized, rebuild value from the rest tokens joined by single spaces.
        // Simpler approach: ask user to include value on same line; we reconstruct with spaces between tokens.
        char *rest = save ? save : NULL; // not directly helpful—tokens consumed
        // We'll rebuild by reading remaining tokens and inserting spaces.
        static char value_buf[MAXLINE];
        value_buf[0] = '\0';
        char *q;
        int first = 1;
        while ((q = strtok_r(NULL, "\n", &save)) != NULL) {
            // This will rarely trigger because previous strtok_r used space delimiters,
            // so instead we must read the remainder of stdin line BEFORE tokenization.

            // Fallback—do nothing
            break;
        }
        // Since strtok_r destroyed spaces, better: re-parse manually.
        // Alternative: do a second pass: find the third space occurrence in original string passed to this function.
        return -2;
    }
    return 0;
}

// Find the third space position; everything after is value (can be empty).
static int split_key_size_value(const char *orig, int *out_key, size_t *out_size, const char **out_val_start) {
    // Expect: "<cmd> <key> <size> <value...>\n"
    // We'll scan tokens but keep pointer into original.
    const char *p = orig;
    // skip cmd
    while (*p && !isspace((unsigned char)*p)) p++;
    while (*p && isspace((unsigned char)*p)) p++;
    // key start
    const char *key_start = p;
    while (*p && !isspace((unsigned char)*p)) p++;
    if (p == key_start) return -1;
    char keybuf[64]; size_t klen = (size_t)(p - key_start);
    if (klen >= sizeof(keybuf)) return -1;
    memcpy(keybuf, key_start, klen); keybuf[klen] = '\0';
    while (*p && isspace((unsigned char)*p)) p++;
    // size start
    const char *size_start = p;
    while (*p && !isspace((unsigned char)*p)) p++;
    if (p == size_start) return -1;
    char sizebuf[64]; size_t slen = (size_t)(p - size_start);
    if (slen >= sizeof(sizebuf)) return -1;
    memcpy(sizebuf, size_start, slen); sizebuf[slen] = '\0';

    // remainder is value start (may be empty)
    while (*p && isspace((unsigned char)*p)) { // only skip ONE space—value may intentionally start with spaces
        // We will skip exactly one separating space if present.
        p++;
        break;
    }
    *out_val_start = p;
    *out_key = (int)strtol(keybuf, NULL, 10);
    *out_size = (size_t)strtoull(sizebuf, NULL, 10);
    return 0;
}

static void handle_local_command(char *line_raw) {
    char line[MAXLINE];
    strncpy(line, line_raw, sizeof(line)-1);
    line[sizeof(line)-1] = '\0';

    // Normalize a copy for command detection
    char first[16] = {0};
    sscanf(line, "%15s", first);
    for (char *p = first; *p; ++p) *p = (char)tolower((unsigned char)*p);
    if (first[0] == '\0') return;

    if (strcmp(first, "connect") == 0) {
        char host[256]; int port;
        if (sscanf(line, "%*s %255s %d", host, &port) != 2) {
            printf("ERR usage: connect <server-ip> <server-port>\n");
            return;
        }
        int rc = connect_to(host, port);
        if (rc == 0) printf("OK\n");
        else if (rc == -2) printf("ERR already connected\n");
        else printf("ERR connect failed\n");
        return;
    }

    if (strcmp(first, "disconnect") == 0) {
        disconnect_now();
        printf("OK\n");
        return;
    }

    if (strcmp(first, "quit") == 0 || strcmp(first, "exit") == 0) {
        if (conn_fd != -1) disconnect_now();
        exit(0);
    }

    if (strcmp(first, "help") == 0) {
        printf("Commands:\n");
        printf("  connect <ip> <port>\n");
        printf("  disconnect\n");
        printf("  create <key> <value-size> <value>\n");
        printf("  read <key>\n");
        printf("  update <key> <value-size> <value>\n");
        printf("  delete <key>\n");
        printf("  quit | exit | help\n");
        return;
    }

    // Server-bound commands
    if (conn_fd == -1) {
        printf("ERR not connected\n");
        return;
    }

    if (strcmp(first, "create") == 0 || strcmp(first, "update") == 0) {
        int key; size_t sz; const char *val_start = NULL;
        if (split_key_size_value(line, &key, &sz, &val_start) != 0) {
            printf("ERR usage: %s <key> <value-size> <value>\n", first);
            return;
        }
        size_t actual_len = strlen(val_start);
        if (actual_len != sz) {
            printf("ERR value-size (%zu) does not match actual length (%zu)\n", sz, actual_len);
            return;
        }
        char header[256];
        int hl = snprintf(header, sizeof(header),
                          "%s %d %zu\n",
                          (strcmp(first, "create") == 0) ? "CREATE" : "UPDATE",
                          key, sz);
        if (send_cmd_with_optional_value(header, val_start, sz) == 0) {
            (void)recv_status_and_optional_value();
        }
        return;
    }

    if (strcmp(first, "read") == 0) {
        int key;
        if (sscanf(line, "%*s %d", &key) != 1) {
            printf("ERR usage: read <key>\n");
            return;
        }
        char header[128];
        int hl = snprintf(header, sizeof(header), "READ %d\n", key);
        (void)send_cmd_with_optional_value(header, NULL, 0);
        (void)recv_status_and_optional_value();
        return;
    }

    if (strcmp(first, "delete") == 0) {
        int key;
        if (sscanf(line, "%*s %d", &key) != 1) {
            printf("ERR usage: delete <key>\n");
            return;
        }
        char header[128];
        int hl = snprintf(header, sizeof(header), "DELETE %d\n", key);
        (void)send_cmd_with_optional_value(header, NULL, 0);
        (void)recv_status_and_optional_value();
        return;
    }

    printf("ERR unknown command (type 'help')\n");
}

/* ------------- Modes ------------- */

static void run_interactive(void) {
    char line[MAXLINE];
    for (;;) {
        printf("kv> ");
        if (!fgets(line, sizeof(line), stdin)) {
            puts("");
            break;
        }
        // strip trailing newline
        size_t n = strlen(line);
        if (n && line[n-1] == '\n') line[n-1] = '\0';
        handle_local_command(line);
    }
    if (conn_fd != -1) disconnect_now();
}

static void run_batch(const char *filename) {
    FILE *f = fopen(filename, "r");
    if (!f) { perror("fopen"); return; }
    char line[MAXLINE];
    int lineno = 0;
    while (fgets(line, sizeof(line), f)) {
        lineno++;
        // strip trailing newline
        size_t n = strlen(line);
        if (n && line[n-1] == '\n') line[n-1] = '\0';
        if (line[0] == '\0' || line[0] == '#') continue; // allow comments
        handle_local_command(line);
    }
    fclose(f);
    if (conn_fd != -1) disconnect_now();
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage:\n  %s interactive\n  %s batch <file>\n", argv[0], argv[0]);
        return 1;
    }
    if (strcmp(argv[1], "interactive") == 0) {
        run_interactive();
    } else if (strcmp(argv[1], "batch") == 0) {
        if (argc != 3) {
            fprintf(stderr, "Usage: %s batch <file>\n", argv[0]);
            return 1;
        }
        run_batch(argv[2]);
    } else {
        fprintf(stderr, "Unknown mode: %s\n", argv[1]);
        return 1;
    }
    return 0;
}
