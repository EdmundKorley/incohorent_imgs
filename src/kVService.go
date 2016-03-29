package main

import (
	"net/http"
	"sync"
	"net/url"
	"fmt"
)

var kVStore map[string]string
var kVStoreMutex sync.RWMutex

func main() {
	kVStore = make(map[string]string)
	kVStoreMutex = sync.RWMutex{}
	http.HandleFunc("/get", get)
	http.HandleFunc("/set", set)
	http.HandleFunc("/remove", remove)
	http.HandleFunc("/list", list)
	fmt.Println("kVService is up! 🗃")
	http.ListenAndServe(":3000", nil)
}

func get(w http.ResponseWriter, r *http.Request) {
	if (r.Method == http.MethodGet) {

		// If this is a GET request
		values, err := url.ParseQuery(r.URL.RawQuery) // Reference any values and erros
		if err != nil { // If err is non-falsely
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error 🚫:", err)
			return
		}
		// At this point err object is non-falsely
		// Now check if no key specified
		if len(values.Get("key")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error 🚫:", "Wrong input key.")
			return
		}

		// Here we get a meaningful request but since we have distributed workers
		// We want to use a mutex lock to handle this concurrent reading and writing
		kVStoreMutex.RLock()
		value := kVStore[string(values.Get("key"))]
		kVStoreMutex.RUnlock()

		fmt.Fprint(w, value)

	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error 🚫: Only GET accepted.")
	}
}

func set(w http.ResponseWriter, r *http.Request) {
	if (r.Method == http.MethodPost) {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error 🚫:", err)
			return
		}
		if len(values.Get("key")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error 🚫:", "Wrong input key.")
			return
		}
		if len(values.Get("value")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error 🚫:", "Wrong input value.")
			return
		}

		kVStoreMutex.Lock()
		kVStore[string(values.Get("key"))] = string(values.Get("value"))
		kVStoreMutex.Unlock()

		fmt.Fprint(w, "success")
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error 🚫: Only POST accepted.")
	}
}

func remove(w http.ResponseWriter, r *http.Request) {
	if (r.Method == http.MethodDelete) {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error 🚫:", err)
			return
		}
		if len(values.Get("key")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error 🚫:", "Wrong input key.")
			return
		}
		kVStoreMutex.Lock()
		delete(kVStore, values.Get("key"))
		kVStoreMutex.Unlock()

		fmt.Fprint(w, "success")
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error 🚫: Only DELETE accepted.")
	}
}

func list(w http.ResponseWriter, r *http.Request) {
	if (r.Method == http.MethodGet) {
		kVStoreMutex.RLock()
		for key, value := range kVStore {
			fmt.Fprintln(w, key, ":", value)
		}
		kVStoreMutex.RUnlock()
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error 🚫: Only GET accepted.")
	}
}
