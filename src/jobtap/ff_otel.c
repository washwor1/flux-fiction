#include "ff_otel.h"

#include <errno.h>
#include <jansson.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

struct ff_otel_span_impl {
    uint64_t id;
    char *name;
    json_t *attrs;
};

struct ff_otel_state {
    int fd;
    bool enabled;
    uint64_t next_id;
    struct sockaddr_un addr;
    socklen_t addrlen;
    char service_name[128];
    pid_t pid;
};

static struct ff_otel_state g_otel = {
    .fd = -1,
    .enabled = false,
    .next_id = 1,
};

static unsigned int g_send_error_count = 0;

static char *ff_otel_strdup(const char *value)
{
    if (!value)
        return NULL;

    size_t len = strlen(value) + 1;
    char *copy = malloc(len);
    if (!copy)
        return NULL;

    memcpy(copy, value, len);
    return copy;
}

static void ff_otel_disable(void)
{
    if (g_otel.fd >= 0) {
        close(g_otel.fd);
        g_otel.fd = -1;
    }
    g_otel.enabled = false;
}

static void ff_otel_send(json_t *root)
{
    if (!g_otel.enabled || g_otel.fd < 0 || !root)
        return;

    char *payload = json_dumps(root, JSON_COMPACT);
    if (!payload)
        return;

    ssize_t rc = sendto(g_otel.fd,
                        payload,
                        strlen(payload),
                        MSG_DONTWAIT,
                        (const struct sockaddr *)&g_otel.addr,
                        g_otel.addrlen);
    if (rc < 0 && g_send_error_count < 10) {
        ++g_send_error_count;
        fprintf(stderr,
                "ff_otel sendto failed for %s: %s\n",
                g_otel.addr.sun_path,
                strerror(errno));
    }
    (void)rc;
    free(payload);
}

int ff_otel_init(const char *service_name, const char *endpoint)
{
    ff_otel_disable();

    if (!endpoint || endpoint[0] == '\0')
        return -1;

    int fd = socket(AF_UNIX, SOCK_DGRAM, 0);
    if (fd < 0)
        return -1;

    memset(&g_otel.addr, 0, sizeof(g_otel.addr));
    g_otel.addr.sun_family = AF_UNIX;
    if (strlen(endpoint) >= sizeof(g_otel.addr.sun_path)) {
        close(fd);
        errno = ENAMETOOLONG;
        return -1;
    }
    strncpy(g_otel.addr.sun_path, endpoint, sizeof(g_otel.addr.sun_path) - 1);
    g_otel.addrlen = sizeof(sa_family_t) + strlen(g_otel.addr.sun_path) + 1;

    g_otel.fd = fd;
    g_otel.enabled = true;
    g_otel.pid = getpid();
    g_otel.next_id = 1;
    g_send_error_count = 0;
    memset(g_otel.service_name, 0, sizeof(g_otel.service_name));
    if (service_name && service_name[0] != '\0')
        strncpy(g_otel.service_name, service_name, sizeof(g_otel.service_name) - 1);
    else
        strncpy(g_otel.service_name, "flux-fiction-jobtap", sizeof(g_otel.service_name) - 1);
    return 0;
}

ff_otel_span_t ff_otel_span_start(const char *name)
{
    if (!g_otel.enabled || !name)
        return NULL;

    struct ff_otel_span_impl *span = calloc(1, sizeof(*span));
    if (!span)
        return NULL;

    span->id = g_otel.next_id++;
    span->name = ff_otel_strdup(name);
    span->attrs = json_object();
    if (!span->name || !span->attrs) {
        if (span->attrs)
            json_decref(span->attrs);
        free(span->name);
        free(span);
        return NULL;
    }

    json_t *root = json_object();
    json_object_set_new(root, "kind", json_string("span_start"));
    json_object_set_new(root, "span_id", json_integer((json_int_t)span->id));
    json_object_set_new(root, "name", json_string(span->name));
    json_object_set_new(root, "service", json_string(g_otel.service_name));
    json_object_set_new(root, "source", json_string("jobtap"));
    json_object_set_new(root, "pid", json_integer((json_int_t)g_otel.pid));
    ff_otel_send(root);
    json_decref(root);

    return span;
}

void ff_otel_span_set_attr_str(ff_otel_span_t span_ptr,
                               const char *key,
                               const char *value)
{
    struct ff_otel_span_impl *span = (struct ff_otel_span_impl *)span_ptr;
    if (!span || !key || !span->attrs)
        return;
    json_object_set_new(span->attrs, key, json_string(value ? value : ""));
}

void ff_otel_span_set_attr_u64(ff_otel_span_t span_ptr,
                               const char *key,
                               uint64_t value)
{
    struct ff_otel_span_impl *span = (struct ff_otel_span_impl *)span_ptr;
    if (!span || !key || !span->attrs)
        return;
    json_object_set_new(span->attrs, key, json_integer((json_int_t)value));
}

void ff_otel_span_end(ff_otel_span_t span_ptr)
{
    struct ff_otel_span_impl *span = (struct ff_otel_span_impl *)span_ptr;
    if (!span)
        return;

    if (g_otel.enabled) {
        json_t *root = json_object();
        json_object_set_new(root, "kind", json_string("span_end"));
        json_object_set_new(root, "span_id", json_integer((json_int_t)span->id));
        json_object_set_new(root, "service", json_string(g_otel.service_name));
        json_object_set_new(root, "source", json_string("jobtap"));
        json_object_set_new(root, "pid", json_integer((json_int_t)g_otel.pid));
        if (span->attrs) {
            json_object_set(root, "attrs", span->attrs);
        }
        ff_otel_send(root);
        json_decref(root);
    }

    if (span->attrs)
        json_decref(span->attrs);
    free(span->name);
    free(span);
}
