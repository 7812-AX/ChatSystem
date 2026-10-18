/**
 * logger.cpp — 统一日志出口的实现
 *
 * 所在层：common。
 * 内容：底层写出函数 + LogLine 析构；模板成员 operator<< 的定义在 logger.h。
 */

#include "logger.h"

#include <unistd.h>
#include <cerrno>

#include <mutex>
#include <string>

// ==================== 底层出口：锁内一次性写出整行 ====================

void log_emit(const std::string& line, bool is_error) {
    static std::mutex line_mutex;  // 静态局部变量：C++11 保证初始化线程安全，即全项目唯一那把日志锁
    ssize_t n;
    std::string line_f = line + '\n';  // 锁外拼接：分配与拷贝不进临界区

    {
        std::lock_guard<std::mutex> lock(line_mutex);
        do{
            // 单次 write：分多次写会把插队窗口重新打开
            n = write(is_error ? STDERR_FILENO : STDOUT_FILENO,line_f.data(),line_f.size());
        }while (n < 0 && errno == EINTR);  // EINTR = 零字节写入，重发整行安全；短写不补齐
    }
}

// ==================== 装配单元的析构：整条消息一次性输出 ====================

LogLine::~LogLine() {
    log_emit(oss_.str(),is_error_);  // 分号处析构 ⇒ 所有 << 已攒齐，此处一次写出
}
