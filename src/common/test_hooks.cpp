#include "test_hooks.h"

#include <fcntl.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

struct Config {
    bool enabled = false;        // CHAT_HOOK_LOG 已设置
    bool pause_enabled = true;   // CHAT_HOOK_PAUSE != "0"
    std::string log_path;
    std::string resume_path;     // <log>.resume：被测进程轮询、驱动追加 seq
    std::vector<std::uint64_t> pause_on_ids;
    long long deadline_ms = 30000;
};

const Config& config() {
    static const Config cfg = [] {
        Config c;
        const char* log = std::getenv("CHAT_HOOK_LOG");
        if (log != nullptr && *log != '\0') {
            c.enabled = true;
            c.log_path = log;
            c.resume_path = std::string(log) + ".resume";
        }
        const char* pause = std::getenv("CHAT_HOOK_PAUSE");
        if (pause != nullptr && std::strcmp(pause, "0") == 0) c.pause_enabled = false;
        const char* ids = std::getenv("CHAT_HOOK_PAUSE_ON_IDS");
        if (ids != nullptr) {
            std::istringstream iss(ids);
            std::string item;
            while (std::getline(iss, item, ',')) {
                if (!item.empty()) c.pause_on_ids.push_back(std::strtoull(item.c_str(), nullptr, 10));
            }
        }
        const char* deadline = std::getenv("CHAT_HOOK_DEADLINE_MS");
        if (deadline != nullptr) c.deadline_ms = std::atoll(deadline);
        return c;
    }();
    return cfg;
}

std::mutex& io_mutex() {
    static std::mutex m;
    return m;
}

// <log>.pause_ids：运行期才知道"要暂停在哪个身份上"时用（如进程内 harness 先连上、
// 拿到身份号再告诉钩子）。每批读一次，只在该批判断暂停时调用；env 与文件取并集。
std::vector<std::uint64_t> load_pause_ids_file() {
    std::vector<std::uint64_t> out;
    const Config& cfg = config();
    if (!cfg.enabled) return out;
    const int fd = ::open((cfg.log_path + ".pause_ids").c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return out;
    std::string buf;
    char chunk[256];
    ssize_t n = 0;
    while ((n = ::read(fd, chunk, sizeof(chunk))) > 0) buf.append(chunk, static_cast<std::size_t>(n));
    ::close(fd);
    std::istringstream iss(buf);
    std::string item;
    while (iss >> item) {
        out.push_back(std::strtoull(item.c_str(), nullptr, 10));
    }
    return out;
}

unsigned long long next_seq() {
    static std::atomic<unsigned long long> counter{0};
    return ++counter;
}

long long now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 8);
    for (char ch : s) {
        switch (ch) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default: out += ch;
        }
    }
    return out;
}

// 追加一条 JSONL：单次 write + O_APPEND，多线程下天然整行原子
void append_record(unsigned long long seq, const char* tag, const char* phase,
                   const std::string& detail) {
    const Config& cfg = config();
    if (!cfg.enabled) return;
    std::ostringstream oss;
    oss << "{\"seq\":" << seq
        << ",\"ts\":" << now_ms()
        << ",\"pid\":" << static_cast<long long>(::getpid())
        << ",\"tid\":" << static_cast<long long>(::gettid())
        << ",\"tag\":\"" << json_escape(tag)
        << "\",\"phase\":\"" << phase
        << "\",\"detail\":\"" << json_escape(detail) << "\"}";

    std::lock_guard<std::mutex> lock(io_mutex());
    const int fd = ::open(cfg.log_path.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0) return;
    const std::string line = oss.str() + "\n";
    const ssize_t n = ::write(fd, line.data(), line.size());
    (void)n;  // 记录失败不改变业务路径；由驱动按"记录缺失"判本轮无效
    ::close(fd);
}

bool resume_seen(unsigned long long seq) {
    const Config& cfg = config();
    const int fd = ::open(cfg.resume_path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) return false;
    std::string buf;
    char chunk[256];
    ssize_t n = 0;
    while ((n = ::read(fd, chunk, sizeof(chunk))) > 0) buf.append(chunk, static_cast<std::size_t>(n));
    ::close(fd);

    std::istringstream iss(buf);
    std::string item;
    while (iss >> item) {
        if (std::strtoull(item.c_str(), nullptr, 10) == seq) return true;
    }
    return false;
}

void wait_for_resume(unsigned long long seq) {
    const Config& cfg = config();
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(cfg.deadline_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (resume_seen(seq)) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
}

void do_pause(const char* tag, const std::string& detail, unsigned long long seq) {
    // 同进程内暂停串行化：测试编排保证同一时刻只有一个暂停点被等待
    static std::mutex pause_mutex;
    std::lock_guard<std::mutex> lock(pause_mutex);
    append_record(seq, tag, "ready", detail);
    wait_for_resume(seq);
    append_record(seq, tag, resume_seen(seq) ? "resumed" : "timeout", detail);
}

}  // namespace

namespace test_hooks {

void mark(const char* tag, const std::string& detail) {
    const Config& cfg = config();
    if (!cfg.enabled) return;
    append_record(next_seq(), tag, "mark", detail);
}

void pause(const char* tag, const std::string& detail) {
    const Config& cfg = config();
    if (!cfg.enabled) return;
    if (!cfg.pause_enabled) {
        append_record(next_seq(), tag, "mark", detail);  // 只记录不暂停
        return;
    }
    do_pause(tag, detail, next_seq());
}

void observe_event_loop_batch(const struct epoll_event* events, int count) {
    const Config& cfg = config();
    if (!cfg.enabled) return;

    std::vector<std::uint64_t> wants = cfg.pause_on_ids;
    for (std::uint64_t extra : load_pause_ids_file()) wants.push_back(extra);

    std::string detail = "n=" + std::to_string(count) + " ids=";
    bool hit = false;
    for (int i = 0; i < count; ++i) {
        const std::uint64_t id = events[i].data.u64;
        detail += (i == 0 ? "" : ",") + std::to_string(id);
        for (std::uint64_t want : wants) {
            if (id == want) hit = true;
        }
    }

    if (hit && cfg.pause_enabled) {
        do_pause("event_loop.batch", detail, next_seq());
    } else {
        append_record(next_seq(), "event_loop.batch", "mark", detail);
    }
}

void observe_create(std::uint64_t id, std::size_t registry_size) {
    mark("registry.created", "id=" + std::to_string(id) + " size=" + std::to_string(registry_size));
}

void observe_disconnect(std::uint64_t id, std::size_t registry_size, std::size_t orphan_routes) {
    mark("registry.disconnected", "id=" + std::to_string(id) + " size=" +
                                      std::to_string(registry_size) + " orphan=" +
                                      std::to_string(orphan_routes));
}

std::string format_ids(const std::vector<ConnectionId>& ids) {
    std::string out;
    for (std::size_t i = 0; i < ids.size(); ++i) {
        if (i != 0) out += ",";
        out += std::to_string(static_cast<std::uint64_t>(ids[i]));
    }
    return out;
}

}  // namespace test_hooks
