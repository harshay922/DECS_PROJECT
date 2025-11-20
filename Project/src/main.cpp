#include "../include/httplib.h"
#include "../include/cache.h"
#include <iostream>
#include <mysql/mysql.h>
#include <mutex>


using namespace std;
using namespace httplib;

mutex db_lock;

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
    LRUCache cache(5);

    // HTTP
    Server svr;

    // initialize server, get response Hello World

    svr.Get("/hi", [](const Request &, Response &res)
            { res.set_content("Hello World!, this is Harshay\n", "text/plain"); });

    // set query

    svr.Post("/set", [&](const Request &req, Response &res)
             {
                cout << "POST /set reached!!\n" ;

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

    MYSQL_RES* result = nullptr;

    {
    lock_guard<mutex> guard(db_lock);

    if (mysql_query(conn, sql.c_str())) {
        res.set_content("DB Error\n", "text/plain");
        return;
    }

    result = mysql_store_result(conn);

     if (!result) {
            res.set_content("DB Error\n", "text/plain");
            return;
        }

    MYSQL_ROW row = mysql_fetch_row(result);

    if (row) {

        value =row[0];

        mysql_free_result(result);

        cache.put(key, value);
        
        res.set_content(value + string("\n"), "text/plain");
    } else {
        res.set_content("NOT_FOUND\n", "text/plain");
        mysql_free_result(result);
        return;
    }
}

     

    //store in cache

     cache.put(key, value);

        res.set_content(value + "\n", "text/plain"); 

});

    // DELETE query

    svr.Delete("/delete", [&](const Request &req, Response &res)
               {

                cout << "DELETE /delete reached!!\n" ;

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

    // SERVER START HERE

    cout << "Starting server...\n";
    bool ok = svr.listen("0.0.0.0", 8080);
    cout << "Server exited with: " << ok << "\n";
}