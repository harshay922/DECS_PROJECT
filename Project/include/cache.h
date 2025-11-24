#ifndef CACHE_H
#define CACHE_H

#include <iostream>
#include <unordered_map>
#include <string>
#include <list>
#include <mutex>
#include <atomic>

using namespace std;

class LRUCache {

private:
    struct Node {
        string key;
        string value;
        Node *prev;
        Node *next;

        Node(const string &k, const string &v)
            : key(k), value(v), prev(nullptr), next(nullptr) {}
    };

    size_t capacity;
    unordered_map<string, Node*> cache_map;
    Node *head; 
    Node *tail;  // (Least Recently Used)
    mutex lock;

    atomic<long> hits{0};
atomic<long> misses{0};
atomic<long> evictions{0};

    // Move node to the front 

    void moveToFront(Node *node) {
        if (node == head) return; 


        // Unlink node
        if (node->prev) node->prev->next = node->next;
        if (node->next) node->next->prev = node->prev;

        // Update tail if needed

        if (node == tail)
            tail = tail->prev;

        // Insert node at head

        node->prev = nullptr;
        node->next = head;

        if (head)
            head->prev = node;

        head = node;

        // If list was empty

        if (!tail)
            tail = head;
    }

public:
    LRUCache(size_t cap) : capacity(cap), head(nullptr), tail(nullptr) {}

    long getHits() const { return hits.load(); }
long getMisses() const { return misses; }
long getEvictions() const { return evictions; }
size_t currentSize() const { return cache_map.size(); }
size_t getCapacity() const { return capacity; }

    // GET operation

    bool get(const string &key, string &value) {
        lock_guard<mutex> guard(lock);

        auto it = cache_map.find(key);
        if (it == cache_map.end()){
        misses++;
            return false; // MISS
            }

            hits++;

        Node *node = it->second;
        value = node->value;

        moveToFront(node);
        return true; // HIT
    }

    // PUT operation

    void put(const string &key, const string &value) {
        lock_guard<mutex> guard(lock);

        auto it = cache_map.find(key);

        // If key exists — update and move to MRU

        if (it != cache_map.end()) {
            Node *node = it->second;
            node->value = value;
            moveToFront(node);
            return;
        }

        // If cache full — evict LRU

        if (cache_map.size() >= capacity) {
            Node *lru = tail;
            evictions++;
        
            // cout << "LRU EVICT: " << lru->key << "\n";

            cache_map.erase(lru->key);

            // Unlink tail

            if (lru->prev)
                lru->prev->next = nullptr;

            tail = lru->prev;

            if (!tail)    // Cache becomes empty

                head = nullptr;

            delete lru;
        }

        // Add new node at MRU

        Node *node = new Node(key, value);
        node->next = head;

        if (head)
            head->prev = node;

        head = node;

        if (!tail)
            tail = head;

        cache_map[key] = node;
    }

    // DELETE operation

    void remove(const string &key) {
        lock_guard<mutex> guard(lock);

        auto it = cache_map.find(key);
        if (it == cache_map.end())
            return; // nothing to delete

        Node *node = it->second;

        // Unlink from list
        
        if (node->prev)
            node->prev->next = node->next;

        if (node->next)
            node->next->prev = node->prev;

        if (node == head)
            head = head->next;

        if (node == tail)
            tail = tail->prev;

        cache_map.erase(key);
        delete node;
    }
};

#endif // CACHE_H
