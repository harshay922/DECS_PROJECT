#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <string>
#include <mutex>
#include <curl/curl.h>

using namespace std;
using clk = chrono::high_resolution_clock;

// Ignore response body
size_t write_cb(void* contents, size_t size, size_t nmemb, void* userp) {
    return size * nmemb;
}

struct Stats {
    atomic<long> ops{0};
    atomic<long> errors{0};
    vector<double> latencies;
    mutex lock;

    void add(double ms) {
        lock_guard<mutex> g(lock);
        latencies.push_back(ms);
    }
};

// HTTP GET
bool http_get(CURL* curl, const string& url, long& code) {
    curl_easy_reset(curl);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) return false;

    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    return true;
}

// HTTP POST
bool http_post(CURL* curl, const string& url, const string& body, long& code) {
    curl_easy_reset(curl);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_cb);

    CURLcode res = curl_easy_perform(curl);
    if (res != CURLE_OK) return false;

    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
    return true;
}

void worker(const string& server, int duration_sec, int id,
            Stats& stats, int keyspace, const string& workload)
{
    mt19937 rng(id + 999);
    uniform_int_distribution<int> dist(1, keyspace);

    CURL* curl = curl_easy_init();

    auto start = clk::now();
    auto endt = start + chrono::seconds(duration_sec);

    while (clk::now() < endt) {
        string key = "key" + to_string(dist(rng));
        long code = 0;
        bool ok = false;

        auto t0 = clk::now();

        if (workload == "put_all") {
            string v = to_string(rng() % 10000);
            ok = http_post(curl, server + "/set", "key=" + key + "&value=" + v, code);
        }
        else if (workload == "get_all") {
            ok = http_get(curl, server + "/get?key=" + key, code);
        }
        else if (workload == "get_popular") {
            ok = http_get(curl, server + "/get?key=key1", code); // always popular key
        }
        else if (workload == "cpu_bound") {
            ok = http_get(curl, server + "/compute", code);
        }

        auto t1 = clk::now();

        double ms = chrono::duration<double, milli>(t1 - t0).count();
        stats.add(ms);
        stats.ops++;

        if (!ok || code >= 500) stats.errors++;
    }

    curl_easy_cleanup(curl);
}

int main(int argc, char* argv[]) {

    if (argc < 6) {
        cerr << "Usage: ./loadgen <server_url> <threads> <duration_sec> <workload> <keyspace>\n";
        return 1;
    }

    string server = argv[1];
    int threads = stoi(argv[2]);
    int duration_sec = stoi(argv[3]);
    string workload = argv[4];
    int keyspace = stoi(argv[5]);

    Stats stats;
    vector<thread> pool;

    auto start = clk::now();

    for (int i = 0; i < threads; i++) {
        pool.emplace_back(worker, server, duration_sec, i,
                          ref(stats), keyspace, workload);
    }

    for (auto& t : pool) t.join();

    auto end = clk::now();
    double run_time = chrono::duration<double>(end - start).count();

    long total_ops = stats.ops;
    double throughput = total_ops / run_time;

    double avg_latency = 0.0;
    {
        lock_guard<mutex> g(stats.lock);
        for (double x : stats.latencies) avg_latency += x;
        if (!stats.latencies.empty())
            avg_latency /= stats.latencies.size();
    }

    
    cout << "Workload       : " << workload << "\n";
    cout << "Threads        : " << threads << "\n";
    cout << "Duration       : " << duration_sec << "\n";
    cout << "Total Ops      : " << total_ops << "\n";
    cout << "Throughput     : " << throughput << " ops/sec\n";
    cout << "Avg Latency    : " << avg_latency << " ms\n";


    return 0;
}
