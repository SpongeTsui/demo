// 1. add log file
// 2. add log rotation
// 2. log level
// 3. add line number
// 4. cmd time out
// 5. send message
// 6. error handling
// 7. panic system
// 8. test code

package main

import (
	"encoding/gob"
	"encoding/json"
	"github.com/fsnotify/fsnotify"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/exec"
	"time"
)

type Config struct {
	Cool   int      `json:"cool"`
	Tcp    string   `json:"tcp,omitempty"`
	Udp    string   `json:"udp,omitempty"`
	Conts  []string `json:"container"`
	Dam    string   `json:"dam"`
	Tenant string   `json:"tenant"`
	User   string   `json:"user"`
	Pass   string   `json:"pass"`
	Samba  string   `json:"samba"`
}

const (
	No    int = 0
	New   int = 1
	Ready int = 2
)

const ConfPath string = "/etc/demo/demo.conf"

var config Config

func init() {
	// load configration
	var err error
	config, err = loadConf()
	if err != nil {
		log.Println("error:", err)
		return
	}

	// TODO: verify configuration
	if len(config.Conts) == 0 {
		log.Println("error: no container available")
		return
	}

	log.Println("configration:", config)
}

func monitor(done <-chan bool, chReq chan<- Request) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	log.Println("watch file: ", config.Samba)
	err = watcher.Add(config.Samba)
	if err != nil {
		log.Fatal(err)
	}

	pending := make(map[string]int)

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				if event.Op&fsnotify.Create == fsnotify.Create {
					log.Println("event:", event)
					pending[event.Name] = New
					//log.Println("pending:", pending)
				}
				if event.Op&fsnotify.Remove == fsnotify.Remove {
					log.Println("event:", event)
					pending[event.Name] = No
					//log.Println("pending:", pending)
				}
				if event.Op&fsnotify.Chmod == fsnotify.Chmod {
					log.Println("event:", event)
					pending[event.Name] = Ready
					//log.Println("pending:", pending)
				}
			case err := <-watcher.Errors:
				if err != nil {
					log.Println("error:", err)
				}
			case <-time.After(time.Duration(config.Cool) * time.Second):
				//log.Println("cold down, pending:", pending)

				req := Request{Level: 0}
				for k, v := range pending {
					if v != No {
						req.Files = append(req.Files, k)
					}
					delete(pending, k)
				}

				if len(req.Files) > 0 {
					log.Println("send request:", req)
					chReq <- req
				}

			case <-done:
				log.Println("done")
				return
			}
		}
	}()

	<-done
}

func genConf() error {
	conf := Config{
		Cool:   10,
		Tcp:    "localhost:1234",
		Udp:    "localhost:1234",
		Conts:  []string{"hello", "test"},
		Dam:    "10.2.162.110",
		Tenant: "da",
		User:   "system",
		Pass:   "123456",
		Samba:  "/tmp/foo",
	}

	b, err := json.MarshalIndent(conf, "", "    ")
	if err != nil {
		log.Println("error:", err)
		return err
	}

	os.Stdout.Write(b)
	return nil
}

func loadConf() (Config, error) {
	log.Printf("config file: %s", ConfPath)

	if _, err := os.Stat(ConfPath); os.IsNotExist(err) {
		log.Println("file not exist:", ConfPath)
		return Config{}, err
	}

	bytes, err := ioutil.ReadFile(ConfPath)
	if err != nil {
		log.Println("error:", err)
		return Config{}, err
	}

	var config Config
	err = json.Unmarshal(bytes, &config)
	if err != nil {
		log.Println("error:", err)
		return Config{}, err
	}

	return config, nil
}

func udpSender(msg string) error {
	log.Println("send msg via udp:", msg)

	conn, err := net.Dial("udp", config.Udp)
	if err != nil {
		log.Println("connect error:", err)
		return err
	}
	defer conn.Close()

	err = gob.NewEncoder(conn).Encode(msg)
	if err != nil {
		log.Println("send error:", err)
		return err
	}

	return nil
}

func tcpSender(msg string) error {
	log.Println("send msg via tcp:", msg)

	conn, err := net.Dial("tcp", config.Tcp)
	if err != nil {
		log.Println("connect error:", err)
		return err
	}
	defer conn.Close()

	err = gob.NewEncoder(conn).Encode(msg)
	if err != nil {
		log.Println("send error:", err)
		return err
	}

	return nil
}

type Request struct {
	Level int
	Files []string // file name
}

func selectCont(lastCont string) string {
	// verify configuration
	if lastCont == "" {
		return config.Conts[0]
	}

	var newCont string
	for _, value := range config.Conts {
		// keep two different containers in config file
		if lastCont != value {
			newCont = value
			break
		}
	}
	return newCont
}

func cmdExecutor(name string, arg ...string) error {
	log.Println("cmd:", name, ", options:", arg)

	out, err := exec.Command(name, arg...).Output()
	if err != nil {
		log.Println("error:", err)
		return err
	}

	log.Println("result:", string(out))
	return nil
}

func upload(file, cont string) {
	log.Printf("upload file %s to container %s", file, cont)

	name := "dacli"
	args := []string{
		"putObject",
		"-p", config.Dam,
		"-t", config.Tenant,
		"-u", config.User,
		"-P", config.Pass,
		"-c", cont,
		"-f", file,
		"-o", file,
		"--xdata", "use=demo",
	}

	err := cmdExecutor(name, args...)
	if err != nil {
		return
	}
}

func sync(cont string) {
	log.Printf("sync container: %s", cont)

	name := "dacli"
	args := []string{
		"sync",
		"-p", config.Dam,
		"-t", config.Tenant,
		"-u", config.User,
		"-P", config.Pass,
		"-c", cont,
		"--sync",
	}

	err := cmdExecutor(name, args...)
	if err != nil {
		return
	}
}

func handler(done <-chan bool, chReq <-chan Request) {
	// TODO: send msg
	log.Println("start handler to handle request")

	var cont string

	for {
		select {
		case req := <-chReq:
			log.Println("receive req:", req)
			// select contaienr
			cont = selectCont(cont)
			log.Println("select container:", cont)

			// upload files as a batch
			for _, file := range req.Files {
				upload(file, cont)
			}

			// sync
			sync(cont)

		case <-done:
			log.Println("done")
			return
		}
	}
}

func main() {
	/*
		done := make(chan bool)
		chReq := make(chan Request)

		// start monitor: generate request, send to handler;
		go monitor(done, chReq)

		// start handler: handle request
		go handler(done, chReq)

		<-done
	*/
	udpSender("hello")
}