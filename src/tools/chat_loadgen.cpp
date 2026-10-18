// chat_loadgen — 协议感知的非交互式 TCP 负载驱动器。
//
// 每条并发连接独占一个 worker 线程、一个账号和一条长连接，按固定次数重复
// 登录请求；worker 只在本地累计结果，主线程汇总后输出吞吐、错误分类与
// 成功请求的 P50/P95/P99。并发数不能超过账号数。

#include <cerrno>
#include <cstring>
#include <algorithm>
#include <cmath>
#include <iomanip>
#include <iostream>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include <charconv>     //引入字符转换工具：std::from_chars
#include <stdexcept>    //引入标准异常类型：std::invalid_argument
#include <string_view>  //引入字符串只读视图

#include <arpa/inet.h>  //Linux socket
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <sys/time.h>   //socket 超时

#include <thread>       //main 线程注册

enum class WorkloadKind {
    Login,
};

struct Credential {
    std::string username;
    std::string password;
};

struct DriverConfig {
    std::string host;                         // 固定服务端地址
    std::uint16_t port{};                     // 固定服务端端口
    std::size_t concurrency{};                // 每个 worker 一条 TCP 连接
    std::vector<Credential> account_pool;     // 预先准备、可重复使用的输入
    WorkloadKind workload{WorkloadKind::Login};
    std::size_t payload_size{};               // 固定消息大小
    std::size_t requests_per_connection{};   // 每条连接的固定请求数
    std::chrono::milliseconds timeout{};      // 单请求截止时间
};

struct WorkerResult {
    std::uint64_t attempts{};          // 纳入统计的请求尝试总数
    std::uint64_t successes{};         // 完成协议交互并收到完整预期响应的请求数
    std::uint64_t failures{};          // 未成功请求总数；满足 attempts = successes + failures
    std::uint64_t timeouts{};          // 截止时间内未收到完整预期响应的失败请求数
    std::uint64_t connect_failures{};  // 因 worker 建连失败而失败的计划请求数（不是连接条数）
    std::uint64_t protocol_failures{}; // 错误响应或连接关闭时响应仍不完整的失败请求数
    std::vector<double> successful_latency_ms; // 成功请求的端到端响应延迟
};

//样本排序 --> 取分位 percentile --> 返回分位位置的样本
double percentile_ms(std::vector<double> samples, double percentile) {
    if (samples.empty()) return 0.0;

    std::sort(samples.begin(), samples.end());  //排升序

    //计算分位 percentile * vector.size() --> 向上取整ceil
    const auto rank = static_cast<std::size_t>(std::ceil(percentile * static_cast<double>(samples.size())));

    //真实索引 rank-1 ，min()：防止下标溢出，rank==0：防止无符号整数下溢至负数
    const std::size_t index = rank == 0 ? 0 : std::min(rank - 1, samples.size() - 1);

    return samples[index];  //返回 percentile 位置样本
}

//数字参数输入合法检测
std::size_t parse_positive(std::string_view text,const char* field_name){
    std::size_t value{};
    const char* begin = text.data();
    const char* end = begin+text.size();

    auto [ptr,error] = std::from_chars(begin,end,value);

    if(error != std::errc{} || ptr != end || value == 0){
        throw std::invalid_argument(std::string(field_name) + "必须是正整数");
    }

    return value;
}

// 解析并校验命令行参数；账号池必须覆盖全部并发连接。
DriverConfig parse_args(int argc, char* argv[]){
    if(argc < 7) {throw std::invalid_argument(
        "用法: chat_loadgen HOST PORT CONCURRENCY "
        "REQUESTS TIMEOUT_MS USER:PASS [USER:PASS ...]");
    }

    DriverConfig config{};

    //绑定config ( argv[0]--argv[6]/argv[n] )
    config.workload = WorkloadKind::Login;
    config.payload_size = 0;

    config.host = argv[1];
    if(config.host.empty()){
        throw std::invalid_argument("HOST 不能为空");
    }
    //检测 IPV4 是否可用
    in_addr addr_test{};
    if(inet_pton(AF_INET,config.host.c_str(),&addr_test) != 1){
        throw std::invalid_argument("HOST 必须是合法 IPV4 地址");
    }

    std::size_t port_value = parse_positive(argv[2],"PORT");
    if(port_value > 65535){
        throw std::invalid_argument("PORT 应在 1~65535 之间");
    }
    config.port = static_cast<std::uint16_t>(port_value);

    config.concurrency = parse_positive(argv[3],"CONCURRENCY");

    config.requests_per_connection = parse_positive(argv[4],"REQUESTS");

    config.timeout = std::chrono::milliseconds{parse_positive(argv[5],"TIMEOUT_ms")};

    for(int i = 6;i<argc;++i){
        std::string login_text = argv[i];

        std::size_t pos = login_text.find(":");

        if(pos == std::string::npos || pos == 0 || pos+1 == login_text.size()){
            throw std::invalid_argument("账号格式应为: USER:PASS");
        }

        Credential cred{login_text.substr(0,pos),login_text.substr(pos+1)};

        config.account_pool.push_back(cred);
    }

    if(config.account_pool.size() < config.concurrency){
        throw std::invalid_argument("CONCURRENCY 数量不能超过账号数量");
    }

    return config;
}

// 独占一条连接执行固定负载，返回本地计数；worker 不直接更新共享计数。
WorkerResult run_worker(const DriverConfig& config, std::size_t worker_id){
    WorkerResult res{};
    res.attempts = config.requests_per_connection;

    if(worker_id >= config.concurrency || worker_id >= config.account_pool.size()){
        throw std::out_of_range("worker_id 超出并发或账号池数量");
    }

    const Credential& cred = config.account_pool[worker_id]; //账号-worker_id映射

    //创建socket
    int sockfd = socket(AF_INET,SOCK_STREAM,0);
    if(sockfd < 0){
        res.failures = res.attempts;
        res.connect_failures = res.attempts;    //一次失败连接导致所有请求作废

        int error_code = errno;
        std::cerr<<"worker "<<worker_id<<" socket 失败: "<<std::strerror(error_code)<<'\n';
        return res;
    }

    //选定IP,端口
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(config.port);
    int addr_res = inet_pton(AF_INET,config.host.c_str(),&server_addr.sin_addr);
    //防御性编程--检测 IPV4 合法性
    if(addr_res != 1){
        if(addr_res == 0){
            close(sockfd);
            throw std::invalid_argument("HOST 为非法 IPV4 地址");
        }
        close(sockfd);
        throw std::runtime_error("inet_pton 调用失败");
    }

    //connect 连接IP，端口
    if(connect(sockfd,reinterpret_cast<sockaddr*>(&server_addr),sizeof(server_addr)) < 0){
        res.failures = res.attempts;
        res.connect_failures = res.attempts;

        int error_code = errno;
        std::cerr<< "worker " << worker_id<< " connect 失败: "<< std::strerror(error_code)<< '\n';
        close(sockfd);
        return res;
    }

    const auto timeout_ms = config.timeout.count(); //接收无单位时间

    //时间换算
    timeval socket_timeout{};
    socket_timeout.tv_sec = timeout_ms / 1000;
    socket_timeout.tv_usec = (timeout_ms % 1000) * 1000;

    //设置 recv / send 超时
    if (setsockopt(sockfd,SOL_SOCKET,SO_RCVTIMEO,&socket_timeout,sizeof(socket_timeout)) < 0) {
        int error_code = errno;
        std::cerr << "worker " << worker_id<< " 设置接收超时失败: "<< std::strerror(error_code) << '\n';

        res.failures = res.attempts;
        close(sockfd);
        return res;
    }

    if (setsockopt(sockfd,SOL_SOCKET,SO_SNDTIMEO,&socket_timeout,sizeof(socket_timeout)) < 0) {
        int error_code = errno;
        std::cerr << "worker " << worker_id<< " 设置发送超时失败: "<< std::strerror(error_code) << '\n';

        res.failures = res.attempts;
        close(sockfd);
        return res;
    }

    //请求消息构造（原子化，方便后续验证其他消息类型）
    std::string test_msg = "login:"+cred.username+" "+cred.password+"\n";

    //回复验证字符串--期望字符串
    std::string recv_buffer;
    std::string expected = "[ChatSystem] 欢迎 "+cred.username+"\n";

    //执行所有请求 requests_per_connection
    for(std::uint64_t i=0;i<res.attempts;++i){
        const auto request_start = std::chrono::steady_clock::now();//请求起始时间
        std::uint64_t done_req = res.successes+res.failures;        //已经处理过的请求
        std::uint64_t rest_req = res.attempts - done_req;           //余下的请求

        //循环 send 至空
        std::size_t sent = 0;

        while(sent < test_msg.size()){
            ssize_t n = send(sockfd,test_msg.data()+sent,test_msg.size()-sent,0);

            //正常发送
            if(n > 0){
                sent += static_cast<size_t>(n);
                continue;
            }

            //错误码--信号打断
            if(n < 0 && errno == EINTR) continue;

            //错误码--发送缓冲区满-超时无法写入
            if(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)){
                int error_code = errno;
                std::cerr<<"worker "<<worker_id<<" 发送登录消息超时："<<std::strerror(error_code)<<'\n';

                res.failures += rest_req;
                res.timeouts += rest_req;

                close(sockfd);
                return res;
            }

            //发送错误
            int error_code = errno;
            std::cerr<<"worker "<<worker_id<<" 发送登录消息失败："<<std::strerror(error_code)<<'\n';

            res.failures += rest_req;

            close(sockfd);
            return res;
        }

        //循环 recv 至全
        while(true){
            char buffer[1024];

            ssize_t n = recv(sockfd,buffer,sizeof(buffer),0);

            //正常接收
            if(n > 0){
                recv_buffer.append(buffer,static_cast<std::size_t>(n));

                // '\n' 判断消息边界
                std::size_t line_pos = recv_buffer.find('\n');
                if(line_pos == std::string::npos) continue;

                //拆分单独一条消息--判断是否为期待回复
                std::string response_line = recv_buffer.substr(0,line_pos+1);

                recv_buffer.erase(0,line_pos+1);    //处理粘包

                //回复验证成功
                if(response_line == expected){
                    ++res.successes;    //计数器++

                    const auto elapsed = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - request_start);
                    res.successful_latency_ms.push_back(elapsed.count());   //耗时入数组

                    break;
                }

                //验证失败
                res.failures += rest_req;
                res.protocol_failures += rest_req;

                close(sockfd);
                return res;
            }

            //对端关闭
            if(n == 0){
                res.failures += rest_req;
                res.protocol_failures += rest_req;

                close(sockfd);
                return res;
            }

            //错误码--信号打断
            if(n < 0 && errno == EINTR) continue;

            //错误码--接收缓冲区空-超时无新数据
            if(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)){
                int error_code = errno;
                std::cerr<<"worker "<<worker_id<<" 接收登录回复超时："<<std::strerror(error_code)<<'\n';

                res.failures += rest_req;
                res.timeouts += rest_req;

                close(sockfd);
                return res;
            }

            //接收错误
            int error_code = errno;
            std::cerr<<"worker "<<worker_id<<" 接收登录回复失败："<<std::strerror(error_code)<<'\n';

            res.failures += rest_req;

            close(sockfd);
            return res;
        }
    }

    close(sockfd);
    return res;
}

// 主线程汇总各 worker 的本地结果，并校验计数一致性。
WorkerResult merge_results(const std::vector<WorkerResult>& results){
    WorkerResult worker_total{};

    for(const WorkerResult& r : results){
        if(r.attempts != (r.successes+r.failures)){
            throw std::logic_error("单个 worker-WorkerResult 计数不一致");
        }

        worker_total.attempts += r.attempts;
        worker_total.successes += r.successes;
        worker_total.failures += r.failures;
        worker_total.connect_failures += r.connect_failures;
        worker_total.protocol_failures += r.protocol_failures;
        worker_total.timeouts += r.timeouts;
        worker_total.successful_latency_ms.insert(worker_total.successful_latency_ms.end(),
                                                   r.successful_latency_ms.begin(),
                                                   r.successful_latency_ms.end());
    }

    if(worker_total.attempts != worker_total.successes+worker_total.failures){
        throw std::logic_error("worker_total-WorkerResult 计数不一致");
    }

    return worker_total;
}

int main(int argc, char* argv[]){
    //外层捕获异常参数
    try{
        DriverConfig config = parse_args(argc,argv);            //协议拆分
        std::vector<WorkerResult> res(config.concurrency);      //结果数组
        const auto run_start = std::chrono::steady_clock::now();//所有线程起始时间

        //线程
        std::vector<std::thread> workers;
        workers.reserve(config.concurrency);

        //线程函数处理注册--调用
        for(std::size_t worker_id = 0;worker_id<config.concurrency;++worker_id){
            workers.emplace_back([&config,&res,worker_id]{res[worker_id] = run_worker(config,worker_id);});
        }

        //阻塞并结束所有线程
        for(auto& w : workers){
            w.join();
        }

        //汇总结果--输出
        WorkerResult total_res = merge_results(res);

        double error_rate = 100.0 * static_cast<double>(total_res.failures) / static_cast<double>(total_res.attempts);  //错误率

        const auto elapsed_s = std::chrono::duration<double>(std::chrono::steady_clock::now() - run_start).count();     //请求响应总时长
        const double throughput = elapsed_s > 0.0 ? static_cast<double>(total_res.successes) / elapsed_s : 0.0;         //吞吐量

        std::cout << std::fixed << std::setprecision(3);
        std::cout<<"attempts="<<total_res.attempts
                 <<"\nsuccesses="<<total_res.successes
                 <<"\nfailures="<<total_res.failures
                 <<"\ntimeouts="<<total_res.timeouts
                 <<"\nconnect_failures="<<total_res.connect_failures
                 <<"\nprotocol_failures="<<total_res.protocol_failures
                 <<"\nerror_rate="<<error_rate<<"%"
                 <<"\nelapsed_s="<<elapsed_s
                 <<"\nthroughput_success_per_s="<<throughput
                 <<"\nlatency_samples="<<total_res.successful_latency_ms.size();
        if (total_res.successful_latency_ms.empty()) {
            std::cout << "\nlatency_p50_ms=NA"
                      << "\nlatency_p95_ms=NA"
                      << "\nlatency_p99_ms=NA\n";
        } else {
            std::cout << "\nlatency_p50_ms=" << percentile_ms(total_res.successful_latency_ms, 0.50)
                      << "\nlatency_p95_ms=" << percentile_ms(total_res.successful_latency_ms, 0.95)
                      << "\nlatency_p99_ms=" << percentile_ms(total_res.successful_latency_ms, 0.99)
                      << "\n";
        }
    }catch(const std::exception& error){
        std::cerr<<"驱动器失效："<<error.what()<<'\n';
        return 1;
    }

    return 0;
}
