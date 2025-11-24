#include "../include/httplib.h"
#include "../include/cache.h"
#include <iostream>
#include <mysql/mysql.h>
#include <mutex>


using namespace std;
using namespace httplib;

mutex db_lock;

// make counters thread-safe

atomic<long> total_get_requests{0};
atomic<long> total_set_requests{0};
atomic<long> total_delete_requests{0};
atomic<long> total_compute_requests{0};


// CPU heavy function (Fibonacci)
long long fib(int n) {
    if (n <= 1) return n;
    return fib(n - 1) + fib(n - 2);
}




int main(int argc, char *argv[])
{

    // taking arguments for intialization and database connection

    if (argc < 5)
    {
        cerr << "Usage: " << argv[0] << " <host> <user> <password> <database>\n";
        return 1;
    }

    string Host = argv[1];
    string User = argv[2];
    string Password = argv[3];
    string DBname = argv[4];

    // MySQL Connection for Key-Value Store

    MYSQL *conn = mysql_init(NULL);

    if (conn == NULL)
    {
        cerr << "mysql_init() failed\n";
        return 1;
    }

    if (!mysql_real_connect(conn, Host.c_str(), User.c_str(), Password.c_str(),
                            DBname.c_str(), 0, NULL, 0))

    {
        cerr << "mysql_real_connect() failed: " << mysql_error(conn) << "\n";
        return 1;
    }

    cout << "Connected to MySQL successfully!\n";

    //CREATE Cache
    LRUCache cache(5000);

    // HTTP
    Server svr;

    // initialize server, get response Hello World

    svr.Get("/hi", [](const Request &, Response &res)
            { res.set_content("Hello World!, this is Harshay\n", "text/plain"); });

    // set query

    svr.Post("/set", [&](const Request &req, Response &res)
             {
                cout << "POST /set reached!!\n" ;

                total_set_requests++;



    if (!req.has_param("key") || !req.has_param("value")) {
        res.set_content("Missing key or value\n", "text/plain");
        return;
    }


    string key = req.get_param_value("key");
    string value = req.get_param_value("value");

    cache.put(key, value);

    string sql = "INSERT INTO kvstore (k, v) VALUES ('" + key + "', '" + value +
                 "') ON DUPLICATE KEY UPDATE v='" + value + "';";

                 {
    lock_guard<mutex> guard(db_lock);

    if (mysql_query(conn, sql.c_str())) {
        res.set_content("DB Error\n", "text/plain");
    } else {
        res.set_content("OK\n", "text/plain");
    }
 } });

    // get query

    svr.Get("/get", [&](const Request &req, Response &res)
            {

                total_get_requests++;
                cout << "GET /get reached!!\n" ;

    if (!req.has_param("key")) {
        res.set_content("Missing key\n", "text/plain");
        return;
    }

    string key = req.get_param_value("key");

    string value;

      if (cache.get(key, value)) 
        {
            cout << "CACHE HIT for key = " << key << "\n";
            res.set_content(value + "\n", "text/plain");
            return;
        }

        cout << "CACHE MISS for key = " << key << "\n";

        //now fetch from database

    string sql = "SELECT v FROM kvstore WHERE k='" + key + "';";

    
    {
        lock_guard<mutex> guard(db_lock);
        
        if (mysql_query(conn, sql.c_str())) {
            res.set_content("DB Error\n", "text/plain");
            return;
        }
        
        MYSQL_RES* result = mysql_store_result(conn);

   

     if (!result) {
            res.set_content("DB Error\n", "text/plain");
            return;
        }

    MYSQL_ROW row = mysql_fetch_row(result);

    if (row) {

        value =row[0];

        mysql_free_result(result);

        //store in cache

        cache.put(key, value);
        
        res.set_content(value + string("\n"), "text/plain");
        return;
    } else {
        mysql_free_result(result);
        res.set_content("NOT_FOUND\n", "text/plain");
        return;
    }
} 

});

    // DELETE query

    svr.Delete("/delete", [&](const Request &req, Response &res)
               {

                cout << "DELETE /delete reached!!\n" ;

                total_delete_requests++;


    if (!req.has_param("key")) {
        res.set_content("Missing key\n", "text/plain");
        return;
    }

    string key = req.get_param_value("key");

    cout << "DELETE called for key = " << key << "\n";

    //firstly remove from cache

     cache.remove(key);

     //remove from database

    string sql = "DELETE FROM kvstore WHERE k='" + key + "';";

    {
        lock_guard<mutex> guard(db_lock);

    if (mysql_query(conn, sql.c_str())) {
        res.set_content("DB Error in deleting\n", "text/plain");
        return;
    } 

    if (mysql_affected_rows(conn) == 0) {
        res.set_content("NOT_FOUND in the Database\n", "text/plain");
    } 
    else {
        res.set_content("DELETED\n", "text/plain");
    }

} });

// /stats endpoint

svr.Get("/stats", [&](const Request&, Response& res) {

    // Build JSON 

    res.set_header("Content-Type", "application/json");


    string json = "{\n";
    json += "  \"cache_capacity\": " + to_string(cache.getCapacity()) + ",\n";
    json += "  \"cache_size\": " + to_string(cache.currentSize()) + ",\n";
    json += "  \"cache_hits\": " + to_string(cache.getHits()) + ",\n";
    json += "  \"cache_misses\": " + to_string(cache.getMisses()) + ",\n";
    json += "  \"cache_evictions\": " + to_string(cache.getEvictions()) + ",\n";

    long total_cache = cache.getHits() + cache.getMisses();
    double hit_ratio = (total_cache == 0) ? 0.0 :
                        ((double)cache.getHits() / total_cache);

    json += "  \"cache_hit_ratio\": " + to_string(hit_ratio) + ",\n";

        json += "  \"total_get_requests\": " + to_string(total_get_requests) + ",\n";
    json += "  \"total_set_requests\": " + to_string(total_set_requests) + ",\n";
    json += "  \"total_delete_requests\": " + to_string(total_delete_requests) + ",\n";
    json += "  \"total_compute_requests\": " + to_string(total_compute_requests) + "\n";



    json += "}\n";
    

    res.set_content(json, "application/json");
});

//CPU BOUND WORKLOAD

svr.Get("/compute", [&](const Request &req, Response &res) {
   total_compute_requests++;
        cout << "CPU COMPUTE /compute\n";

        long long result = fib(35); // heavy calculation

        res.set_content("FIB=" + to_string(result), "text/plain");
});


    // SERVER START HERE

    cout << "Starting server...\n";
    bool ok = svr.listen("0.0.0.0", 8080);
    cout << "Server exited with: " << ok << "\n";

    mysql_close(conn);
    return ok ? 0 : 1;
}