 
# HTTP Key-Value Store with LRU Cache & MySQL Backend  


This project implements a **multi-tier Key-Value (KV) Server** using **C++**, featuring:

- Multi-threaded HTTP server (cpp-httplib)
- Custom LRU In-Memory Cache (Doubly Linked List + Hash Map)
- Persistent MySQL storage
- Thread-safe operations using mutex locks
- RESTful API for SET / GET / DELETE


---

#  Project Structure

```
DECS_PROJECT/
│
├── include/
│   ├── httplib.h        # HTTP server library
│   └── cache.h          # Cache implementation
│
├── src/
│   └── main.cpp         # Main Key-Value server logic
│
├── schema.sql           # MySQL KV Database schema
└── README.md
```

---

#  Installation Requirements

### Install MySQL
```
sudo apt install mysql-server
sudo mysql_secure_installation
```

### Install MySQL development library
```
sudo apt install libmysqlclient-dev
```

### Install g++
```
sudo apt install g++
```

---

#  Database Setup

Create the database and table using:

```
mysql -u root -p < schema.sql
```

Contents of `schema.sql`:

```
CREATE DATABASE IF NOT EXISTS keyvalueDB;
USE keyvalueDB;

CREATE TABLE IF NOT EXISTS kvstore (
    k VARCHAR(255) PRIMARY KEY,
    v VARCHAR(255)
);
```

---

#  Compiling the Server

Inside the `src/` directory, run:

```
g++ main.cpp -o server -std=c++17 -I../include -lmysqlclient -lpthread
```

If compilation succeeds, it will generate:

```
./server
```

---

#  Running the Server

Syntax:

```
./server <host> <user> <password> <database>
```

Example:

```
./server localhost root xxxxxxx keyvalueDB
```

Expected output:

```
Connected to MySQL successfully!
Starting server...
```

Server runs at:

```
http://localhost:8080/
```

---

#  REST API Endpoints

##  1. Test Endpoint
```
curl "http://localhost:8080/hi"
```

---

#  2. SET Key–Value Pair  
### **POST /set**
```
curl -X POST "http://localhost:8080/set"      -H "Content-Type: application/x-www-form-urlencoded"      -d "key=a&value=10"
```

Response:
```
OK
```

---

#  3. GET Key  
### **GET /get**
```
curl "http://localhost:8080/get?key=a"
```

Output:
```
<value>
```

OR:

```
NOT_FOUND
DB Error
```

When cache is hit/miss, server prints:

```
CACHE HIT for key = a
CACHE MISS for key = a
```

---

#  4. DELETE Key  
### **DELETE /delete**
```
curl -X DELETE "http://localhost:8080/delete?key=a"
```

Output:

```
DELETED
```

OR (if key does not exist):

```
NOT_FOUND in the Database
```

---

#  LRU Cache Details

The cache is implemented manually using:

- A **Doubly Linked List**
- An **unordered_map (hash map)**
- O(1) GET, SET, DELETE, Eviction
- Thread-safe using `mutex`

### Cache Workflow:
####  GET:
1. Check cache  
2. If HIT → return value  
3. If MISS → query DB → insert to cache  

####  SET:
- Insert/update cache  
- Write-through to DB  
- Evict LRU when full  

####  DELETE:
- Remove from cache  
- Remove from DB  

  **Cache resets when server restarts** (RAM-only).

---

#  Thread Safety & Concurrency

- Every database operation is protected by:

```
mutex db_lock;
lock_guard<mutex> guard(db_lock);
```

- Cache operations protected by internal cache mutex
- cpp-httplib automatically uses multiple worker threads  
- Server supports concurrent requests correctly

---

#  Testing Summary

Test SET:
```
curl -X POST http://localhost:8080/set      -H "Content-Type: application/x-www-form-urlencoded"      -d "key=x&value=50"
```

Test GET:
```
curl http://localhost:8080/get?key=x
```

Test DELETE:
```
curl -X DELETE http://localhost:8080/delete?key=x
```

Test Cache Behavior:
```
curl http://localhost:8080/get?key=a
curl http://localhost:8080/get?key=a
```

Test LRU Eviction:
Insert 6 keys to trigger eviction (cache size = 5):

```
set a, b, c, d, e, f
```

Output:
```
LRU EVICT: a
```

---


