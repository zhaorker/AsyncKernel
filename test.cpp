#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <cstdio>
#include <cstdlib>

int main() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8888);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sock);
        return 1;
    }

    printf("Connected to echo server (port 8888). Type lines to send, Ctrl+D to quit.\n");

    char buf[1024];
    while (fgets(buf, sizeof(buf), stdin)) {
        // 发送数据
        size_t len = strlen(buf);
        ssize_t sent = send(sock, buf, len, 0);
        if (sent != (ssize_t)len) {
            perror("send");
            break;
        }

        // 接收回显（循环读满相同长度）
        size_t received = 0;
        while (received < len) {
            ssize_t n = recv(sock, buf + received, len - received, 0);
            if (n <= 0) {
                perror("recv");
                goto done;
            }
            received += n;
        }

        buf[len] = '\0';  // 确保字符串结束（原数据已含换行）
        printf("Echo: %s", buf);
    }

done:
    close(sock);
    return 0;
}