#include "httplib.h"
#include <mysql/mysql.h>
#include <string>

using namespace httplib;
using namespace std;

int main() {

    // CREATE MYSQL CONNECTION
    MYSQL* conn = mysql_init(NULL);

    mysql_real_connect(conn, "127.0.0.1", "root", "Hsrahay@123", "kvdb", 3306, NULL, 0);

    Server svr;

    svr.Get("/hi", [](const Request&, Response& res) {
        res.set_content("Hello World!", "text/plain");
    });

    svr.Get("/set", [&](const Request& req, Response& res) {
        string key = req.get_param_value("key");
        string value = req.get_param_value("value");

        string query = "REPLACE INTO kv_store (k, v) VALUES('" + key + "','" + value + "')";
        mysql_query(conn, query.c_str());
        res.set_content("Stored", "text/plain");
    });

    svr.Get("/get", [&](const Request& req, Response& res) {
        string key = req.get_param_value("key");
        string query = "SELECT v FROM kv_store WHERE k='" + key + "'";
        mysql_query(conn, query.c_str());

        MYSQL_RES* result = mysql_store_result(conn);
        MYSQL_ROW row = mysql_fetch_row(result);

        if(row) res.set_content(row[0], "text/plain");
        else res.set_content("NOT_FOUND", "text/plain");

        mysql_free_result(result);
    });

    svr.Get("/delete", [&](const Request& req, Response& res) {
        string key = req.get_param_value("key");
        string query = "DELETE FROM kv_store WHERE k='" + key + "'";
        mysql_query(conn, query.c_str());
        res.set_content("Deleted", "text/plain");
    });

    svr.listen("0.0.0.0",8080);

    mysql_close(conn);
}
