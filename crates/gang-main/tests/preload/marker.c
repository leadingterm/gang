#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>

__attribute__((constructor)) static void create_marker(void) {
    const char *path = getenv("PRELOAD_MARKER");
    if (path == NULL) {
        return;
    }
    int fd = open(path, O_CREAT | O_WRONLY, 0600);
    if (fd >= 0) {
        close(fd);
    }
}
