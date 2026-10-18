#ifndef LOGGER_H
#define LOGGER_H

/**
 * logger.h — 统一日志出口
 *
 * 所在层：common。全项目唯一的日志写出点，被各层调用，自身不依赖其他模块。
 * 职责范围：把「一行日志」装配成整串、在锁内一次写出；不做分级、不落盘、不轮转。
 *
 * 用法：
 *   LOG_ERR  << "队列已满 [" << n << "/" << m << "]";   // 自动补换行，不要再写 << std::endl
 *   LOG_INFO << "客户端已连接  client_conn_id=" << static_cast<std::uint64_t>(id) << " client_fd=" << fd;
 *
 * 机制：LOG_ERR 展开成临时对象 LogLine(true)，它活到完整表达式结束（分号处）才析构 ——
 *   所有 << 先攒进内部缓冲，析构时锁内一次 write，锁区间即整条消息的原子边界。
 *   裸 std::cerr << a << b 做不到：标准库的锁只在单次 operator<< 调用层面生效，
 *   6 次调用 = 6 个可插队窗口（实测灌水日志 135865 行里 32078 行被撕裂）。
 *
 * 两条改不得的约束：
 * ① 格式逐字节不变：log_emit 只输出 line + '\n'，不加时间戳/级别前缀 —— 撕裂率基线由锚定
 *    整行正则从旧格式日志算出，加前缀即失配，单变量前后对照作废。
 * ② fd 映射不变：LOG_INFO 走 stdout(fd 1)、LOG_ERR 走 stderr(fd 2)，与原 cout/cerr 一致，
 *    两者共用同一把锁 —— 判据脚本按 fd 分流抓日志。
 *
 * 生产对照：本实现即 spdlog 同步模式 / C++20 std::osyncstream 的机制内核；生产环境进一步
 *   走「异步单写者」（线程本地缓冲 + 无锁队列 + 单后端线程），用消除并发代替加锁互斥。
 */

#include <sstream>
#include <string>

/**
 * 锁内一次性写出整行：整行是一个原子单位。
 * @param line 已拼好的整行正文，不含换行（换行由本函数补）
 * @param is_error true 写 stderr(fd 2)；false 写 stdout(fd 1)
 * @note 只发一次 write —— 分多次或循环重发会把插队窗口重新打开。EINTR 重发是唯一例外：
 *       POSIX 保证 write 返回 EINTR 时一个字节都没写出去，故重发整行不会重复写入。
 * @note 短写（0 < 返回值 < 长度）不补齐，接受该行被截断 —— 日志不能反过来拖慢服务器。
 * @note 不许改用 std::cerr / std::cout 输出：它们那把锁与本锁不是同一把，等于没加锁。
 */
void log_emit(const std::string& line, bool is_error);

/**
 * 单条日志的装配单元：构造 = 开始收集，析构 = 一次性原子输出。
 * @note 不可拷贝：内部持有装配缓冲，拷贝语义无意义。正常只以临时对象形态存在。
 */
class LogLine {
public:
    explicit LogLine(bool is_error) : is_error_(is_error) {}

    /**
     * 把攒好的整行交给 log_emit 输出，是「整条消息只输出一次」的落点。
     * @note 依赖 oss_.str() 取得正文，该调用会拷贝一份 string（C++17 无 rvalue 版 str()）。
     * @note 析构默认 noexcept，此处必须不抛 —— log_emit 内只做 write，不抛异常。
     */
    ~LogLine();

    // 禁止拷贝：内部持有装配缓冲，拷贝语义无意义（同 ThreadPool 的处置）
    LogLine(const LogLine&) = delete;
    LogLine& operator=(const LogLine&) = delete;

    /**
     * 追加一片内容，返回自身以支持链式 <<。
     * @param v 任意可喂给 std::ostringstream 的值
     * @note 定义必须留在头文件：模板需看到定义才能实例化，挪到 .cpp 会链接期报 undefined reference。
     * @note 只拼接，不加锁、不输出 —— 锁只在析构那一次拿。
     */
    template <typename T>
    LogLine& operator<<(const T& v) {
        oss_ << v;     // 只拼接，不输出
        return *this;  // 返回自身：a << b << c 的第二次 << 作用在第一次的返回值上
    }

private:
    std::ostringstream oss_;  // 本行的装配缓冲
    bool is_error_;           // true → stderr；false → stdout
};

// 调用点形态：LOG_ERR << "..."（与 std::cerr << "..." 逐字对应，替换是文本级的）
#define LOG_INFO LogLine(false)
#define LOG_ERR  LogLine(true)

#endif // LOGGER_H
