#include <pulsar/Client.h>
using namespace pulsar;

int main(int argc, char *argv[]) {
    Client client{"pulsar://localhost:6650"};
    client.close();
    return 0;
}
