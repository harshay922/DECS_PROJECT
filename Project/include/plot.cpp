#include <iostream>
#include <fstream>
#include <vector>
#include <sstream>
#include "matplotlibcpp.h"

using namespace plt = matplotlibcpp;
using namespace std;

int main() {
    vector<int> threads;
    vector<double> throughput;
    vector<double> latency;

    ifstream file("results.csv");
    if (!file.is_open()) {
        cerr << "Error: results.csv not found.\n";
        return 1;
    }

    string line;
    while (getline(file, line)) {
        stringstream ss(line);
        int th;
        double tp, lt;
        char comma;

        ss >> th >> comma >> tp >> comma >> lt;
        threads.push_back(th);
        throughput.push_back(tp);
        latency.push_back(lt);
    }

    file.close();

    // Throughput Plot
    figure();
    plot(threads, throughput);
    title("Throughput vs Threads");
    xlabel("Threads");
    ylabel("Throughput (ops/sec)");
    grid(true);
    save("throughput.png");

    // Latency Plot
    figure();
    plot(threads, latency);
    title("Latency vs Threads");
    xlabel("Threads");
    ylabel("Avg Latency (ms)");
    grid(true);
    save("latency.png");

    cout << "Genrated: throughput.png & latency.png\n";
    return 0;
}
